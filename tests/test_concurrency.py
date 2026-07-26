"""Two managers over one workspace: contention, races, and lease takeover.

Every test here drives at least two independent :class:`TaskManager`
incarnations against one workspace directory, because contested multi-writer
operation over a shared filesystem is the whole reason the marker protocol
exists. Each manager attaches its own :class:`WorkflowWorkspace` instance, so
the two share nothing in memory — no marker index, no journal writer, no cached
state frame — exactly as two managers on two nodes would not.

Whether a job was launched once or twice is never inferred from manager
bookkeeping: the runner itself records its attempt against the job id with an
exclusive create, so a second launch of the same job leaves durable evidence in
the workspace-independent claim log even if every marker ends up looking right.
"""

import json
import logging
import os
import signal
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import TaskManager, WorkflowWorkspace
from httk.workflow._logging import reset_logging
from httk.workflow.errors import FormatError, TransitionLostError
from httk.workflow.journal import JournalWriter, read_record
from httk.workflow.models import Marker
from httk.workflow.workspace import _IndexEntry

# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

#: Records the attempt that ran this job into a shared directory, with an
#: exclusive create keyed on the job id. A second attempt of the same job can
#: therefore never overwrite the first: it lands in ``duplicates/`` instead,
#: which is the only evidence a double launch would leave behind.
_CLAIM_RECORDING_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
log = Path("@LOG@")
claims = log / "claims"
duplicates = log / "duplicates"
claims.mkdir(parents=True, exist_ok=True)
duplicates.mkdir(parents=True, exist_ok=True)
try:
    handle = os.open(str(claims / context["job_id"]), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
except FileExistsError:
    (duplicates / (context["job_id"] + "." + context["attempt_id"])).write_text(context["attempt_id"])
else:
    with os.fdopen(handle, "w") as stream:
        stream.write(context["attempt_id"])
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

#: Hangs on its first attempt and finishes on any later one, so a job can be
#: taken over from a manager that stopped and still reach a terminal state
#: inside one test.
_HANGS_THEN_SUCCEEDS_RUNNER = """#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
if context["attempt_ordinal"] == 1:
    time.sleep(600)
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

_SLOW_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
time.sleep(0.4)
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

#: A parent that spawns one child into a pool it cannot itself claim from, waits
#: for it, and then finishes. The child runs the very same runner file, so the
#: whole two-manager workflow needs one runner source.
_SPAWNING_RUNNER = """#!/usr/bin/env python3
import json
import os
import shutil
import uuid
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job_dir = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}
if context["step"] == "spawn":
    child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), "child"))
    child_key = "child--" + child_id
    child_dir = temporary / "children" / "jobs" / child_key
    (child_dir / "files").mkdir(parents=True)
    shutil.copyfile(job_dir / "files" / "runner", child_dir / "files" / "runner")
    (child_dir / "files" / "runner").chmod(0o755)
    (child_dir / "job.json").write_text(json.dumps({
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": child_id,
        "tag": "child",
        "name": "Contended child",
        "workflow": "tests.concurrency",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": 500,
        "claim": {"pool": "beta", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": {
            "workspace_id": context["workspace_id"],
            "job_id": context["job_id"],
            "activation_id": context["activation_id"],
        },
    }))
    reference = {
        "workspace_id": context["workspace_id"],
        "job_id": child_id,
        "job_key": child_key,
        "placement": "project/children",
        "label": "only",
    }
    (temporary / "children" / "spawn.json").write_text(json.dumps({"children": [reference]}))
    outcome = {
        **base,
        "action": "wait",
        "next_step": "gather",
        "join": {
            "children": [{
                "workspace_id": reference["workspace_id"],
                "job_id": reference["job_id"],
                "job_key": reference["job_key"],
                "placement_hint": reference["placement"],
            }],
            "condition": "all_succeeded",
        },
    }
else:
    outcome = {**base, "action": "succeed"}
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep records propagating to the capture handlers pytest installs."""

    reset_logging()
    yield
    reset_logging()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _payload(
    root: Path,
    runner_source: str,
    *,
    tag: str,
    pool: str = "default",
    workdir_mode: str = "persistent",
    initial_step: str = "only",
    retry_on: tuple[str, ...] = (),
) -> tuple[Path, str]:
    """Write one complete payload directory and return it with its job id."""

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
        "name": f"Concurrency job {tag}",
        "workflow": "tests.concurrency",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": workdir_mode, "path": "run"},
        "data": {"mode": "none"},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": pool, "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": 3,
            "maximum_total_attempts": 6,
            "maximum_activations": 4,
            "retry_on": list(retry_on),
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _claim_recording_runner(log: Path) -> str:
    """Return the claim-recording runner writing its evidence below *log*."""

    return _CLAIM_RECORDING_RUNNER.replace("@LOG@", str(log))


def _attached(root: Path) -> WorkflowWorkspace:
    """Attach one more independent view of the workspace at *root*."""

    return WorkflowWorkspace(root)


def _backdate_heartbeat(workspace: WorkflowWorkspace, manager_id: str, age: float) -> None:
    """Rewrite one manager's heartbeat as if it had stopped *age* seconds ago."""

    path = workspace.control / "managers" / manager_id / "heartbeat.json"
    heartbeat = json.loads(path.read_text(encoding="utf-8"))
    stopped = datetime.now(UTC) - timedelta(seconds=age)
    heartbeat["updated_at"] = stopped.isoformat(timespec="microseconds").replace("+00:00", "Z")
    path.write_text(json.dumps(heartbeat), encoding="utf-8")


def _stop_attempts(manager: TaskManager) -> None:
    """Kill and reap every attempt this manager still owns, ignoring its state."""

    for attempt in list(manager._running.values()):
        try:
            os.killpg(attempt.process.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            attempt.process.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - depends on the host
            pass


def _interleave(managers: tuple[TaskManager, ...], *, until: Callable[[], bool], timeout: float = 60.0) -> None:
    """Tick every manager in a fixed round-robin order until *until* holds."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for manager in managers:
            manager.tick()
        if until():
            return
        time.sleep(0.02)
    raise AssertionError("the interleaved managers never reached the expected state")


def _kinds(workspace: WorkflowWorkspace) -> dict[str, str]:
    """Return the current state kind of every job in the workspace, by job id."""

    return {marker.job_id: marker.kind for marker in workspace.scan_markers()}


def _launched_by(caplog: pytest.LogCaptureFixture) -> dict[str, str]:
    """Return which manager launched each job, from the structured log records."""

    return {
        str(getattr(record, "job_key")): str(getattr(record, "manager_id"))
        for record in caplog.records
        if getattr(record, "event", None) == "launch"
    }


def _walk_chain(workspace: WorkflowWorkspace, marker: Marker) -> list[dict[str, object]]:
    """Return the frames of one job's history, newest first, from its marker.

    Walking backwards from the authoritative marker is the only sanctioned way
    to read a job's history, so this is also the assertion that the chain the
    transitions left behind is intact and terminates at the submission.
    """

    frames: list[dict[str, object]] = []
    record_ref: str | None = marker.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        frames.append(frame)
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)
    generations = [int(str(frame["state_generation"])) for frame in frames]
    assert generations == sorted(generations, reverse=True)
    assert generations[-1] == 1
    return frames


# ---------------------------------------------------------------------------
# 1. Contended claiming
# ---------------------------------------------------------------------------


def test_two_interleaved_managers_claim_every_ready_job_exactly_once(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    log = tmp_path / "claimlog"
    submitting = _attached(root)
    job_ids = []
    for index in range(6):
        payload, job_id = _payload(tmp_path / "source", _claim_recording_runner(log), tag=f"contended-{index}")
        submitting.submit(payload, f"project/contended/{index}")
        job_ids.append(job_id)

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01, maximum_workers=3) as manager_a:
        with TaskManager(workspace_b, heartbeat_interval=0.01, maximum_workers=3) as manager_b:
            _interleave(
                (manager_a, manager_b),
                until=lambda: all(kind == "succeeded" for kind in _kinds(workspace_a).values())
                and len(_kinds(workspace_a)) == len(job_ids),
            )

    assert sorted(path.name for path in (log / "claims").iterdir()) == sorted(job_ids)
    assert list((log / "duplicates").iterdir()) == []
    for job_id in job_ids:
        marker = workspace_a.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "succeeded"
        # One claim per job means one attempt per job, whichever manager won it.
        assert workspace_a.read_state(marker)["total_attempts"] == 1
    assert workspace_a.check().ok


def test_a_lost_claim_race_leaves_exactly_one_winner_and_a_clean_loser(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    log = tmp_path / "claimlog"
    payload, job_id = _payload(tmp_path / "source", _claim_recording_runner(log), tag="raced")
    _attached(root).submit(payload, "project/raced")

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01) as manager_a:
        with TaskManager(workspace_b, heartbeat_interval=0.01) as manager_b:
            # Both managers register submissions and then scan for ready work
            # before either of them acts on what it saw.
            manager_a._register_submissions()
            eligible_a = manager_a._eligible_ready()
            eligible_b = manager_b._eligible_ready()
            assert len(eligible_a) == len(eligible_b) == 1
            assert eligible_a[0].path == eligible_b[0].path

            lost: list[TransitionLostError] = []
            won: list[TaskManager] = []
            for manager, marker in ((manager_a, eligible_a[0]), (manager_b, eligible_b[0])):
                try:
                    manager._claim_and_launch(marker)
                except TransitionLostError as exc:
                    lost.append(exc)
                else:
                    won.append(manager)

            assert len(won) == 1 and len(lost) == 1
            # The loser holds nothing: no local attempt, no half-written state.
            loser = manager_b if won[0] is manager_a else manager_a
            assert loser._running == {}
            # And it keeps working: its very next tick is an ordinary one.
            loser.tick()
            _interleave(
                (manager_a, manager_b),
                until=lambda: _kinds(workspace_a).get(job_id) == "succeeded",
            )

    assert len(list((log / "claims").iterdir())) == 1
    assert list((log / "duplicates").iterdir()) == []
    marker = workspace_a.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert len(list(workspace_a.scan_markers())) == 1
    assert workspace_a.check().ok


def test_two_threads_ticking_one_workspace_launch_every_job_exactly_once(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    log = tmp_path / "claimlog"
    submitting = _attached(root)
    job_ids = []
    for index in range(8):
        payload, job_id = _payload(tmp_path / "source", _claim_recording_runner(log), tag=f"threaded-{index}")
        submitting.submit(payload, f"project/threaded/{index}")
        job_ids.append(job_id)

    failures: list[BaseException] = []

    def serve() -> None:
        try:
            with TaskManager(_attached(root), heartbeat_interval=0.01, maximum_workers=2) as manager:
                manager.run_until_idle(timeout=90.0, poll_interval=0.01)
        except BaseException as exc:  # pragma: no cover - reported by the assertion below
            failures.append(exc)

    threads = [threading.Thread(target=serve, name=f"manager-{index}") for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120.0)
        assert not thread.is_alive()
    assert failures == []

    workspace = _attached(root)
    assert sorted(path.name for path in (log / "claims").iterdir()) == sorted(job_ids)
    assert list((log / "duplicates").iterdir()) == []
    for job_id in job_ids:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "succeeded"
    assert workspace.check().ok


# ---------------------------------------------------------------------------
# 2. Contended operator requests and joins
# ---------------------------------------------------------------------------


def test_an_operator_request_is_applied_by_exactly_one_manager(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    submitting = _attached(root)
    # A pool neither manager serves keeps every job in ready, so the request
    # pass is the only thing that ever moves a marker in this test.
    for index in range(6):
        payload, _ = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag=f"requested-{index}", pool="nobody")
        submitting.submit(payload, f"project/requested/{index}")

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01) as manager_a:
        with TaskManager(workspace_b, heartbeat_interval=0.01) as manager_b:
            manager_a._register_submissions()
            ready = sorted(workspace_a.scan_markers(("ready",)), key=lambda item: item.job_key)
            assert len(ready) == 6
            for marker in ready:
                submitting.publish_request(
                    {
                        "format": "httk-workflow-request",
                        "format_version": 1,
                        "request_id": str(uuid.uuid4()),
                        "job_id": marker.job_id,
                        "job_key": marker.job_key,
                        "expected_generation": marker.generation,
                        "expected_record_ref": marker.record_ref,
                        "action": "pause",
                        "operator": "tester",
                        "reason": "contended request",
                        "created_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
                    }
                )

            with caplog.at_level(logging.INFO, logger="httk.workflow"):
                barrier = threading.Barrier(2)

                def handle(manager: TaskManager) -> None:
                    barrier.wait(timeout=30.0)
                    for _ in range(20):
                        manager._handle_requests()

                threads = [threading.Thread(target=handle, args=(manager,)) for manager in (manager_a, manager_b)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=60.0)
                    assert not thread.is_alive()

    handled = [record for record in caplog.records if getattr(record, "event", None) == "request_handled"]
    # Each request was claimed by exactly one manager and applied exactly once:
    # six handled records for six requests, and every job exactly one
    # generation beyond the ready state it was paused from.
    assert len(handled) == 6
    assert len({getattr(record, "request") for record in handled}) == 6
    paused = sorted(workspace_a.scan_markers(("paused",)), key=lambda item: item.job_key)
    assert len(paused) == 6
    for marker, before in zip(paused, ready, strict=True):
        assert marker.job_key == before.job_key
        assert marker.generation == before.generation + 1
    assert list((workspace_a.control / "requests" / "ready").iterdir()) == []
    assert workspace_a.check().ok


def test_a_join_resolves_when_the_children_run_on_the_other_manager(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    payload, parent_id = _payload(
        tmp_path / "source",
        _SPAWNING_RUNNER,
        tag="parent",
        pool="alpha",
        initial_step="spawn",
    )
    _attached(root).submit(payload, "project/parent")

    workspace_a, workspace_b = _attached(root), _attached(root)
    with caplog.at_level(logging.INFO, logger="httk.workflow"):
        with TaskManager(workspace_a, heartbeat_interval=0.01, pools=("alpha",)) as manager_a:
            with TaskManager(workspace_b, heartbeat_interval=0.01, pools=("beta",)) as manager_b:
                _interleave(
                    (manager_a, manager_b),
                    until=lambda: _kinds(workspace_a).get(parent_id) == "succeeded",
                )
                alpha, beta = manager_a.manager_id, manager_b.manager_id

    parent = workspace_a.find_marker_by_id(parent_id)
    assert parent is not None and parent.kind == "succeeded"
    children = [marker for marker in workspace_a.scan_markers() if marker.job_key.startswith("child--")]
    assert len(children) == 1 and children[0].kind == "succeeded"

    by_manager = _launched_by(caplog)
    assert by_manager[parent.job_key] == alpha
    assert by_manager[children[0].job_key] == beta
    # The parent's join observed the child the other manager ran, by label.
    summary = workspace_a.read_state(parent)["join_summary"]
    assert [item["label"] for item in summary] == ["only"]
    assert [item["kind"] for item in summary] == ["succeeded"]
    assert workspace_a.check().ok


def test_both_managers_drain_without_stranding_the_work_they_started(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    submitting = _attached(root)
    job_ids = []
    for index in range(4):
        payload, job_id = _payload(tmp_path / "source", _SLOW_SUCCEED_RUNNER, tag=f"drained-{index}")
        submitting.submit(payload, f"project/drained/{index}")
        job_ids.append(job_id)

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01) as manager_a:
        with TaskManager(workspace_b, heartbeat_interval=0.01) as manager_b:
            _interleave(
                (manager_a, manager_b),
                until=lambda: bool(manager_a._running) and bool(manager_b._running),
            )
            manager_a._draining = True
            manager_b._draining = True
            _interleave(
                (manager_a, manager_b),
                until=lambda: not manager_a._running and not manager_b._running,
            )
            assert not manager_a._running and not manager_b._running

    kinds = _kinds(workspace_a)
    assert set(kinds) == set(job_ids)
    assert sorted(kinds.values()) == ["ready", "ready", "succeeded", "succeeded"]
    assert workspace_a.check().ok


# ---------------------------------------------------------------------------
# 3. One manager dies, the other recovers
# ---------------------------------------------------------------------------


class _StoppedManager(Exception):
    """Raised in place of the work a manager never got to do."""


def _stop_before_launching(*arguments: object, **keywords: object) -> None:
    """Stand in for a manager that died between its claim and its launch."""

    raise _StoppedManager()


def test_a_claim_abandoned_mid_launch_is_recovered_by_the_other_manager(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root, policy={"lease_seconds": 2.0})
    log = tmp_path / "claimlog"
    payload, job_id = _payload(tmp_path / "source", _claim_recording_runner(log), tag="abandoned")
    _attached(root).submit(payload, "project/abandoned")

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01) as manager_a:
        with TaskManager(workspace_b, heartbeat_interval=0.01) as manager_b:
            # Manager A stops existing between the claim rename and the launch.
            monkeypatch.setattr(TaskManager, "_launch_claimed", _stop_before_launching)
            with pytest.raises(_StoppedManager):
                manager_a.tick()
            monkeypatch.undo()

            claimed = workspace_b.find_marker_by_id(job_id)
            assert claimed is not None and claimed.kind == "claimed"
            assert workspace_b.read_state(claimed)["manager_id"] == manager_a.manager_id

            # Nothing is taken from a manager that is still heartbeating.
            manager_b.tick()
            still = workspace_b.find_marker_by_id(job_id)
            assert still is not None and still.kind == "claimed"

            _backdate_heartbeat(workspace_b, manager_a.manager_id, age=10.0)
            manager_b._recover_abandoned_claims()
            released = workspace_b.find_marker_by_id(job_id)
            assert released is not None and released.kind == "ready"
            state = workspace_b.read_state(released)
            assert state["reason"] == "claim_abandoned"
            # The abandoned claim consumed no budget: the recovered job is at
            # the attempt ordinal it had before manager A ever touched it.
            assert state["attempt_ordinal"] == 0 and state["total_attempts"] == 0

            _interleave((manager_b,), until=lambda: _kinds(workspace_b).get(job_id) == "succeeded")

    assert len(list((log / "claims").iterdir())) == 1
    assert list((log / "duplicates").iterdir()) == []
    assert workspace_b.check().ok


def test_a_persistent_attempt_is_adopted_only_once_its_writer_is_proven_gone(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root, policy={"lease_seconds": 2.0})
    payload, job_id = _payload(
        tmp_path / "source",
        _SLEEPING_RUNNER,
        tag="persistent-writer",
        workdir_mode="persistent",
        # A pool manager B does not serve keeps the adopted job in ready rather
        # than relaunching it inside the same test.
        pool="alpha",
        retry_on=("lease_lost",),
    )
    _attached(root).submit(payload, "project/persistent-writer")

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01, pools=("alpha",)) as manager_a:
        with TaskManager(
            workspace_b,
            heartbeat_interval=0.01,
            pools=("beta",),
            takeover_grace_factor=1.0,
        ) as manager_b:
            _interleave((manager_a,), until=lambda: _kinds(workspace_a).get(job_id) == "running")
            _backdate_heartbeat(workspace_b, manager_a.manager_id, age=600.0)

            # A silent lease is not evidence that a persistent workdir is free:
            # a second writer there would corrupt it, so B leaves it alone.
            manager_b.tick()
            running = workspace_b.find_marker_by_id(job_id)
            assert running is not None and running.kind == "running"

            _stop_attempts(manager_a)
            manager_b.tick()

    adopted = workspace_b.find_marker_by_id(job_id)
    assert adopted is not None and adopted.kind == "ready"
    state = workspace_b.read_state(adopted)
    assert state["takeover_evidence"]["evidence"] == "writer_process_dead"
    assert state["unclean_restart"] is True
    assert workspace_b.check().ok


def test_an_isolated_attempt_is_adopted_on_the_grace_and_its_zombie_is_fenced(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root, policy={"lease_seconds": 2.0})
    payload, job_id = _payload(
        tmp_path / "source",
        _HANGS_THEN_SUCCEEDS_RUNNER,
        tag="isolated-writer",
        workdir_mode="isolated",
        retry_on=("lease_lost",),
    )
    _attached(root).submit(payload, "project/isolated-writer")

    workspace_a, workspace_b = _attached(root), _attached(root)
    with TaskManager(workspace_a, heartbeat_interval=0.01) as manager_a:
        with TaskManager(workspace_b, heartbeat_interval=0.01, takeover_grace_factor=2.0) as manager_b:
            _interleave((manager_a,), until=lambda: _kinds(workspace_a).get(job_id) == "running")
            zombie = workspace_a.find_marker_by_id(job_id)
            assert zombie is not None
            zombie_state = workspace_a.read_state(zombie)
            zombie_attempt = str(zombie_state["attempt_id"])
            zombie_activation = str(zombie_state["activation_id"])
            zombie_control = workspace_a.payload_path(zombie.placement, zombie.job_key) / str(
                zombie_state["attempt_control"]
            )

            # Manager A stops heartbeating. One expired lease is not yet enough:
            # the takeover grace is what an isolated attempt is left alone for.
            _backdate_heartbeat(workspace_b, manager_a.manager_id, age=3.0)
            manager_b._poll_running()
            waited = workspace_b.find_marker_by_id(job_id)
            assert waited is not None and waited.kind == "running"

            _backdate_heartbeat(workspace_b, manager_a.manager_id, age=600.0)
            manager_b._poll_running()
            adopted = workspace_b.find_marker_by_id(job_id)
            assert adopted is not None and adopted.kind == "ready"
            assert workspace_b.read_state(adopted)["takeover_evidence"]["evidence"] == "lease_grace_expired"

            # The zombie attempt of manager A now publishes what it computed.
            temporary = zombie_control / "outcome.tmp.zombie"
            temporary.mkdir()
            (temporary / "outcome.json").write_text(
                json.dumps(
                    {
                        "format": "httk-workflow-outcome",
                        "format_version": 1,
                        "job_id": zombie.job_id,
                        "activation_id": zombie_activation,
                        "attempt_id": zombie_attempt,
                        "action": "succeed",
                    }
                ),
                encoding="utf-8",
            )
            os.rename(temporary, zombie_control / "outcome.ready")

            try:
                _interleave((manager_b,), until=lambda: _kinds(workspace_b).get(job_id) == "succeeded")
            finally:
                _stop_attempts(manager_a)
                _stop_attempts(manager_b)

            final = workspace_b.find_marker_by_id(job_id)
            assert final is not None
            live = manager_b._read_frame(final)
            assert live.attempt_id != zombie_attempt

            # The commit path itself refuses the zombie's outcome: the attempt
            # identity in it names an attempt no marker points at any more.
            with pytest.raises(FormatError, match="attempt_id"):
                manager_b._read_outcome(zombie_control / "outcome.ready" / "outcome.json", final, live)

            # And no frame of the job's own history was ever built from it: the
            # succeeded state descends from the attempt manager B launched.
            chain = _walk_chain(workspace_b, final)
            attempts = {frame.get("attempt_id") for frame in chain} - {None}
            assert zombie_attempt in attempts and live.attempt_id in attempts
            assert [frame["kind"] for frame in chain[:2]] == ["succeeded", "committing"]
            assert all(frame.get("attempt_id") == live.attempt_id for frame in chain[:2])

    assert workspace_b.check().ok


# ---------------------------------------------------------------------------
# 4. The derived marker index under contention
# ---------------------------------------------------------------------------


def test_a_stale_marker_index_never_reports_a_false_absence(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    WorkflowWorkspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="indexed")
    _attached(root).submit(payload, "project/indexed")

    workspace_a, workspace_b = _attached(root), _attached(root)
    submitted = workspace_a.find_marker_by_id(job_id)
    assert submitted is not None and submitted.kind == "submitted"
    job = workspace_b.load_job(submitted)

    def transition_on_b(marker: Marker, kind: str, **updates: object) -> Marker:
        with JournalWriter(workspace_b.control) as writer:
            return workspace_b.transition(writer, marker, kind, updates)

    ready = transition_on_b(
        submitted,
        "ready",
        step=job.initial_step,
        activation_id=str(uuid.uuid4()),
        activation_ordinal=1,
        attempt_ordinal=0,
        total_attempts=0,
        data_generation=None,
    )

    # Manager A's cached entry now names a marker that is not there any more.
    # The targeted placement probe answers without a rescan, and never absence.
    assert workspace_a._marker_index is not None
    assert job_id in workspace_a._marker_index
    found = workspace_a.find_marker_by_id(job_id)
    assert found is not None and found.kind == "ready" and found.path == ready.path
    assert workspace_a.find_markers(ready.job_key) == [found]

    claimed = transition_on_b(
        ready,
        "claimed",
        step=job.initial_step,
        activation_id=str(uuid.uuid4()),
        activation_ordinal=1,
        attempt_ordinal=1,
        total_attempts=1,
        data_generation=None,
    )
    # The placement-hinted lookup a join uses follows the same rule.
    at_placement = workspace_a.find_marker_at(claimed.job_key, claimed.placement)
    assert at_placement is not None and at_placement.kind == "claimed"

    # Even a cached entry naming a placement the job never had falls back to a
    # complete rescan rather than reporting the job as gone.
    entry = _IndexEntry(kind="ready", placement=PurePosixPath("project/elsewhere"), basename=claimed.path.name)
    assert workspace_a._marker_index is not None
    workspace_a._marker_index[job_id] = entry
    rescanned = workspace_a.find_marker_by_id(job_id)
    assert rescanned is not None and rescanned.path == claimed.path

    # Absence is still reported when the job really has left the state tree.
    workspace_b.quarantine(claimed.path, reason="removed by the other manager")
    assert workspace_a.find_marker_by_id(job_id) is None
    assert workspace_a.find_markers(claimed.job_key) == []
