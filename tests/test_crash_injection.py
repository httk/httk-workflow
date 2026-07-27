"""Killing a manager at every step of the required outcome-processing order.

The specification fixes nine steps for committing one outcome and pairs every
interruption point with exactly one recovery rule. Each case below stops a
manager dead at one of those points, throws that manager away, attaches a fresh
incarnation, and demands that the commit completes *exactly* once: one applied
transaction, one registered child, one activation, and a job history that still
walks backwards from the authoritative marker to the submission.

The second half injects the two storage failures the verified-transition
algorithm exists for — a rename that happened but reported failure, and a
destination that is not visible yet — because a manager that cannot tell those
apart from a lost race either duplicates work or loses a job.
"""

import errno
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow import transactions as transactions_module
from httk.workflow._logging import reset_logging
from httk.workflow.errors import TransitionLostError, WorkspaceUnavailableError
from httk.workflow.journal import read_record
from httk.workflow.models import Marker

# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

#: One job whose single commit exercises every step of the outcome-processing
#: order: a multi-operation transaction, a spawned child, a join to wait on it,
#: and a second activation that finishes. Its child runs the same runner file.
_COMMIT_HEAVY_RUNNER = """#!/usr/bin/env python3
import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job_dir = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
with (run / "steps.log").open("a") as stream:
    stream.write(context["step"] + " " + context["attempt_id"] + "\\n")
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}
if context["step"] == "prepare":
    payload = temporary / "transaction" / "payload"
    payload.mkdir(parents=True)
    operations = []
    for index in range(3):
        name = "result-" + str(index) + ".txt"
        content = ("result " + str(index) + "\\n").encode()
        (payload / name).write_bytes(content)
        operations.append({
            "id": "put-" + str(index),
            "op": "put-file",
            "source": "payload/" + name,
            "path": name,
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    (temporary / "transaction" / "manifest.json").write_text(json.dumps({
        "format": "httk-workflow-transaction",
        "format_version": 1,
        "id": "transaction",
        "expected_data_generation": context["data_generation"],
        "operations": operations,
    }))
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
        "name": "Interrupted child",
        "workflow": "tests.crash",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
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
        "expected_data_generation": context["data_generation"],
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

#: Records that it started, waits long enough for its manager to be killed
#: under it, and then publishes its outcome into the workspace anyway.
_ORPHANABLE_RUNNER = """#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
with (run / "steps.log").open("a") as stream:
    stream.write(context["step"] + "\\n")
time.sleep(3.0)
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


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep records propagating to the capture handlers pytest installs."""

    reset_logging()
    yield
    reset_logging()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


#: What every rename this file intercepts is actually called with.
_RenamePath = str | os.PathLike[str]


class _KilledManager(Exception):
    """Stands in for a manager process disappearing at one exact point.

    It deliberately derives from nothing the manager catches, so injecting it
    is indistinguishable from the process ceasing to exist mid-commit.
    """


def _payload(
    root: Path,
    runner_source: str,
    *,
    tag: str,
    data_mode: str = "none",
    initial_step: str = "only",
    pool: str = "default",
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
        "name": f"Crash-injection job {tag}",
        "workflow": "tests.crash",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": data_mode},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": pool, "required_capabilities": []},
        "retry_policy": {
            "maximum_attempts_per_activation": 3,
            "maximum_total_attempts": 6,
            "maximum_activations": 4,
            "retry_on": [],
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _destination_kind(workspace: Workspace, destination: Path) -> str:
    """Return the state kind one marker destination path belongs to."""

    return destination.relative_to(workspace.control / "state").parts[0]


def _kill_at_marker_rename(
    monkeypatch: pytest.MonkeyPatch,
    *,
    job_key: str,
    kind: str,
    after: bool,
) -> None:
    """Kill the manager at the one rename that moves *job_key* into *kind*.

    With ``after`` the rename is performed first, which is the interruption the
    protocol calls "after marker rename": the new state is already
    authoritative and only cleanup was lost.
    """

    real = Workspace._verified_marker_rename
    armed = [True]

    def hooked(self: Workspace, marker: Marker, destination: Path) -> Marker:
        if armed[0] and marker.job_key == job_key and _destination_kind(self, destination) == kind:
            armed[0] = False
            if after:
                real(self, marker, destination)
            raise _KilledManager()
        return real(self, marker, destination)

    monkeypatch.setattr(Workspace, "_verified_marker_rename", hooked)


def _kill_on_entry(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Kill the manager as soon as it enters one of its own methods."""

    def hooked(self: TaskManager, *arguments: object, **keywords: object) -> None:
        raise _KilledManager()

    monkeypatch.setattr(TaskManager, name, hooked)


def _kill_after(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Kill the manager the instant one of its own methods has finished."""

    real = getattr(TaskManager, name)

    def hooked(self: TaskManager, *arguments: object, **keywords: object) -> None:
        real(self, *arguments, **keywords)
        raise _KilledManager()

    monkeypatch.setattr(TaskManager, name, hooked)


def _kill_at_transaction_operation(monkeypatch: pytest.MonkeyPatch, ordinal: int) -> None:
    """Kill the manager part way through replaying a multi-operation transaction."""

    real = transactions_module._rename_verified
    calls = [0]

    def hooked(source: Path, destination: Path, **keywords: object) -> None:
        calls[0] += 1
        if calls[0] >= ordinal:
            raise _KilledManager()
        real(source, destination, **keywords)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(transactions_module, "_rename_verified", hooked)


def _tick_until_killed(manager: TaskManager, *, timeout: float = 60.0) -> None:
    """Tick until the injected interruption fires, or fail saying it never did."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        manager.tick()
        time.sleep(0.01)
    raise AssertionError("the injected interruption never fired")


def _stop_attempts(manager: TaskManager) -> None:
    """Kill and reap every attempt one abandoned manager still owns locally."""

    for attempt in list(manager._running.values()):
        try:
            os.killpg(attempt.process.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            attempt.process.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - depends on the host
            pass


def _walk_chain(workspace: Workspace, marker: Marker) -> list[dict[str, object]]:
    """Return one job's history, newest first, walked back from its marker."""

    frames: list[dict[str, object]] = []
    record_ref: str | None = marker.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        frames.append(frame)
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)
    generations = [int(str(frame["state_generation"])) for frame in frames]
    assert generations == sorted(generations, reverse=True), generations
    assert generations[-1] == 1
    return frames


def _drive_until(workspace: Workspace, manager: TaskManager, job_id: str, kinds: set[str]) -> Marker:
    """Tick until one job reaches any of *kinds*, returning its marker."""

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        manager.tick()
        marker = workspace.find_marker_by_id(job_id)
        if marker is not None and marker.kind in kinds:
            return marker
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} never reached {sorted(kinds)}")


# ---------------------------------------------------------------------------
# 1. Every interruption point of the outcome-processing order
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Interruption:
    """One point of the outcome-processing order, and where it leaves the job."""

    name: str
    arm: Callable[[pytest.MonkeyPatch, str], None]
    kind_after_kill: str


_INTERRUPTIONS = (
    _Interruption(
        "after-committing-frame-append",
        lambda monkeypatch, job_key: _kill_at_marker_rename(
            monkeypatch, job_key=job_key, kind="committing", after=False
        ),
        "running",
    ),
    _Interruption(
        "after-running-to-committing-rename",
        lambda monkeypatch, job_key: _kill_on_entry(monkeypatch, "_process_committing"),
        "committing",
    ),
    _Interruption(
        "mid-transaction-replay",
        lambda monkeypatch, job_key: _kill_at_transaction_operation(monkeypatch, ordinal=2),
        "committing",
    ),
    _Interruption(
        "after-children-registered",
        lambda monkeypatch, job_key: _kill_after(monkeypatch, "_register_children"),
        "committing",
    ),
    _Interruption(
        "after-destination-frame-append",
        lambda monkeypatch, job_key: _kill_at_marker_rename(monkeypatch, job_key=job_key, kind="waiting", after=False),
        "committing",
    ),
    _Interruption(
        "after-destination-marker-rename",
        lambda monkeypatch, job_key: _kill_at_marker_rename(monkeypatch, job_key=job_key, kind="waiting", after=True),
        "waiting",
    ),
)

#: The complete history of the interrupted job once it has finished, newest
#: first. It is the same list whatever was interrupted: a commit that ran twice,
#: an activation that repeated, or a step that was rerun would all show here.
_EXPECTED_CHAIN = [
    "succeeded",
    "committing",
    "running",
    "claimed",
    "ready",
    "waiting",
    "committing",
    "running",
    "claimed",
    "ready",
]


@pytest.mark.parametrize("interruption", _INTERRUPTIONS, ids=lambda item: item.name)
def test_a_fresh_manager_completes_an_interrupted_commit_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interruption: _Interruption,
) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root, extensions=["transactional-data-v1"])
    payload, job_id = _payload(
        tmp_path / "source",
        _COMMIT_HEAVY_RUNNER,
        tag="interrupted",
        data_mode="transactional",
        initial_step="prepare",
    )
    submitted = Workspace(root).submit(payload, "project/interrupted")

    with TaskManager(Workspace(root), heartbeat_interval=0.01) as dying:
        interruption.arm(monkeypatch, submitted.job_key)
        with pytest.raises(_KilledManager):
            _tick_until_killed(dying)
        _stop_attempts(dying)
    monkeypatch.undo()

    workspace = Workspace(root)
    interrupted = workspace.find_marker_by_id(job_id)
    assert interrupted is not None and interrupted.kind == interruption.kind_after_kill
    interrupted_payload = workspace.payload_path(interrupted.placement, interrupted.job_key)
    if interruption.name == "mid-transaction-replay":
        # The manager really did die between two operations of one transaction.
        assert sorted(path.name for path in (interrupted_payload / "data").iterdir()) == ["result-0.txt"]
    if interruption.name == "after-children-registered":
        # And here it really did die with the child already registered.
        assert len([m for m in workspace.scan_markers() if m.job_key.startswith("child--")]) == 1

    with TaskManager(Workspace(root), heartbeat_interval=0.01) as fresh:
        fresh.run_until_idle(timeout=90.0)

    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"

    # The child was registered exactly once: one bundle, one marker, one run.
    children = [marker for marker in workspace.scan_markers() if marker.job_key.startswith("child--")]
    assert len(children) == 1 and children[0].kind == "succeeded"
    assert [path.name for path in sorted((root / "project" / "children").iterdir())] == [children[0].job_key]

    # The transaction is all or nothing at the attempt boundary, and it advanced
    # the data generation exactly once however far into it the manager died.
    parent_payload = workspace.payload_path(parent.placement, parent.job_key)
    data = parent_payload / "data"
    assert sorted(path.name for path in data.iterdir()) == ["result-0.txt", "result-1.txt", "result-2.txt"]
    for index in range(3):
        assert (data / f"result-{index}.txt").read_text(encoding="utf-8") == f"result {index}\n"
    assert workspace.read_state(parent)["data_generation"] == 1

    # No activation was duplicated and no step was rerun: the runner recorded
    # one line per step, and both were run by the attempt that committed.
    steps = (parent_payload / "run" / "steps.log").read_text(encoding="utf-8").splitlines()
    assert [line.split()[0] for line in steps] == ["prepare", "gather"]
    assert len({line.split()[1] for line in steps}) == 2

    chain = _walk_chain(workspace, parent)
    assert [frame["kind"] for frame in chain] == _EXPECTED_CHAIN
    assert workspace.check().ok


def test_a_commit_resumes_after_its_registered_child_has_already_started(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child registered by an interrupted commit is a job in its own right.

    Registration is the point of no return for a spawn set, so once the child
    carries its own marker another manager may claim it at once. When the
    interrupted parent commit is resumed, the child's payload has therefore
    already grown a workdir and an attempt of its own, and the resumption must
    verify that the child is *registered* rather than that its payload still
    hashes to what the outcome published.
    """

    root = tmp_path / "workspace"
    Workspace.initialize(root, extensions=["transactional-data-v1"])
    payload, job_id = _payload(
        tmp_path / "source",
        _COMMIT_HEAVY_RUNNER,
        tag="overtaken",
        data_mode="transactional",
        initial_step="prepare",
    )
    submitted = Workspace(root).submit(payload, "project/overtaken")

    with TaskManager(Workspace(root), heartbeat_interval=0.01) as dying:
        _kill_after(monkeypatch, "_register_children")
        with pytest.raises(_KilledManager):
            _tick_until_killed(dying)
        _stop_attempts(dying)
    monkeypatch.undo()

    workspace = Workspace(root)
    interrupted = workspace.find_marker_by_id(job_id)
    assert interrupted is not None and interrupted.kind == "committing"

    # Another manager runs the registered child to completion, and deliberately
    # leaves the interrupted parent commit alone while it does so.
    child_key = next(marker.job_key for marker in workspace.scan_markers() if marker.job_key.startswith("child--"))
    real_process_committing = TaskManager._process_committing

    def only_the_child(self: TaskManager, marker: Marker) -> None:
        if marker.job_key != submitted.job_key:
            real_process_committing(self, marker)

    monkeypatch.setattr(TaskManager, "_process_committing", only_the_child)
    with TaskManager(Workspace(root), heartbeat_interval=0.01) as child_runner:
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            child_runner.tick()
            found = workspace.find_markers(child_key)
            if found and found[0].kind == "succeeded":
                break
            time.sleep(0.01)
    monkeypatch.undo()
    assert workspace.find_markers(child_key)[0].kind == "succeeded"

    with TaskManager(Workspace(root), heartbeat_interval=0.01) as fresh:
        fresh.run_until_idle(timeout=90.0)

    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    assert len(workspace.find_markers(child_key)) == 1
    assert [frame["kind"] for frame in _walk_chain(workspace, parent)] == _EXPECTED_CHAIN
    assert workspace.check().ok


# ---------------------------------------------------------------------------
# 2. A real process, killed with SIGKILL
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_a_sigkilled_manager_process_leaves_a_job_a_fresh_manager_finishes(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _ORPHANABLE_RUNNER, tag="sigkilled")
    Workspace(root).submit(payload, "project/sigkilled")

    source_root = Path(__file__).resolve().parents[1] / "src"
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = f"{source_root}{os.pathsep}{existing}" if existing else str(source_root)
    process = subprocess.Popen(
        [sys.executable, "-m", "httk.workflow.cli", "run", str(root), "--until-idle", "--poll-interval", "0.05"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    state_running = root / ".httk-workflow" / "state" / "running"
    try:
        running = _wait_for(lambda: any(path.is_file() for path in state_running.rglob("*")))
        if running:
            os.kill(process.pid, signal.SIGKILL)
        else:  # pragma: no cover - only on a host too slow to launch an attempt
            process.terminate()
    finally:
        process.wait(timeout=60)
    if not running:  # pragma: no cover - only on a host too slow to launch an attempt
        pytest.skip("the manager subprocess never reached a running attempt on this host")
    assert process.returncode == -signal.SIGKILL

    # The runner was started in its own session, so it outlives its manager and
    # publishes the outcome the killed manager never got to see.
    published = _wait_for(lambda: bool(list((root / "project" / "sigkilled").rglob("outcome.ready"))), timeout=60.0)
    if not published:  # pragma: no cover - only on a host where the orphan was reaped
        pytest.skip("the orphaned runner never published its outcome on this host")

    workspace = Workspace(root)
    with TaskManager(workspace, heartbeat_interval=0.01) as fresh:
        fresh.run_until_idle(timeout=90.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    steps = (workspace.payload_path(marker.placement, marker.job_key) / "run" / "steps.log").read_text(encoding="utf-8")
    # Recovery committed the published outcome instead of rerunning the step.
    assert steps.splitlines() == ["only"]
    assert workspace.check().ok


def _wait_for(condition: Callable[[], bool], *, timeout: float = 60.0) -> bool:
    """Poll *condition* generously, reporting whether it ever became true."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# 3. Renames that lie, the way a network filesystem lies
# ---------------------------------------------------------------------------


def _lying_rename(
    monkeypatch: pytest.MonkeyPatch,
    *,
    source_kind: str,
    perform: bool,
) -> list[int]:
    """Make the one rename leaving ``state/<source_kind>/`` report a failure.

    With ``perform`` the rename is carried out first, which is exactly what a
    retransmitted NFS rename whose first reply was lost does: the operation
    happened, and the caller is told it did not.
    """

    real = os.rename
    fired = [0]

    def rename(source: _RenamePath, destination: _RenamePath) -> None:
        if not fired[0] and f"{os.sep}state{os.sep}{source_kind}{os.sep}" in str(source):
            fired[0] += 1
            if perform:
                real(source, destination)
            raise OSError(errno.EIO, "simulated retransmitted rename whose reply was lost")
        real(source, destination)

    monkeypatch.setattr(os, "rename", rename)
    return fired


def test_a_claim_whose_rename_happened_but_reported_failure_is_won(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="claimed")
    workspace.submit(payload, "project/claimed")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        fired = _lying_rename(monkeypatch, source_kind="ready", perform=True)
        manager.run_until_idle(timeout=60.0)
    monkeypatch.undo()

    assert fired == [1]
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    # One marker, one claim, one intact history: the actor that renamed the
    # marker verified the destination and correctly concluded that it had won.
    assert len(list(workspace.scan_markers())) == 1
    assert [frame["kind"] for frame in _walk_chain(workspace, marker)] == [
        "succeeded",
        "committing",
        "running",
        "claimed",
        "ready",
    ]
    assert workspace.check().ok


def test_an_outcome_commit_whose_rename_happened_but_reported_failure_is_won(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="committed")
    workspace.submit(payload, "project/committed")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        fired = _lying_rename(monkeypatch, source_kind="running", perform=True)
        manager.run_until_idle(timeout=60.0)
    monkeypatch.undo()

    assert fired == [1]
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert len(list(workspace.scan_markers())) == 1
    assert [frame["kind"] for frame in _walk_chain(workspace, marker)] == [
        "succeeded",
        "committing",
        "running",
        "claimed",
        "ready",
    ]
    assert workspace.check().ok


def test_a_cancellation_fence_whose_rename_happened_but_reported_failure_is_won(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SLEEPING_RUNNER, tag="cancelled")
    workspace.submit(payload, "project/cancelled")

    with TaskManager(workspace, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as manager:
        running = _drive_until(workspace, manager, job_id, {"running"})
        workspace.publish_request(
            {
                "format": "httk-workflow-request",
                "format_version": 1,
                "request_id": str(uuid.uuid4()),
                "job_id": running.job_id,
                "job_key": running.job_key,
                "placement": running.placement.as_posix(),
                "expected_generation": running.generation,
                "expected_record_ref": running.record_ref,
                "action": "cancel",
                "operator": "tester",
                "reason": "rename injection",
                "created_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            }
        )
        fired = _lying_rename(monkeypatch, source_kind="running", perform=True)
        manager._handle_requests()
        monkeypatch.undo()
        # The fence was verified, not retried: the attempt is fenced exactly once.
        assert fired == [1]
        fenced = workspace.find_marker_by_id(job_id)
        assert fenced is not None and fenced.kind == "cancelling"
        _drive_until(workspace, manager, job_id, {"cancelled"})

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "cancelled"
    assert len(list(workspace.scan_markers())) == 1
    assert workspace.read_state(marker)["cancellation"]["verified"] in {
        "process_exited",
        "process_group_absent",
    }
    assert workspace.check().ok


def test_a_rename_that_really_failed_is_simply_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="retried")
    workspace.submit(payload, "project/retried")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        # The rename genuinely did not happen, so the source is still there and
        # the same rename is simply attempted again.
        fired = _lying_rename(monkeypatch, source_kind="ready", perform=False)
        manager.run_until_idle(timeout=60.0)
    monkeypatch.undo()

    assert fired == [1]
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert len(list(workspace.scan_markers())) == 1
    assert workspace.check().ok


def test_a_rename_that_failed_because_another_actor_won_is_reported_as_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="lost")
    workspace.submit(payload, "project/lost")

    rival = Workspace(root)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"

        # Another manager appends its own claim frame and wins the very rename
        # this one is about to attempt.
        writer = rival.open_journal_writer()
        rival_ref = writer.append(
            {
                "format": "httk-workflow-state",
                "format_version": 1,
                "workspace_id": rival.workspace_id,
                "job_id": ready.job_id,
                "job_key": ready.job_key,
                "placement": ready.placement.as_posix(),
                "state_generation": ready.generation + 1,
                "kind": "claimed",
                "previous_record_ref": ready.record_ref,
                "created_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "priority": ready.priority,
            }
        )
        rival_path = rival.marker_path(
            "claimed", ready.placement, ready.job_key, ready.priority, ready.generation + 1, rival_ref
        )
        real = os.rename
        fired = [0]

        def rename(source: _RenamePath, destination: _RenamePath) -> None:
            if not fired[0] and str(source) == str(ready.path):
                fired[0] += 1
                rival_path.parent.mkdir(parents=True, exist_ok=True)
                real(source, rival_path)
                raise OSError(errno.ENOENT, "simulated rename onto a pruned destination parent")
            real(source, destination)

        monkeypatch.setattr(os, "rename", rename)
        with pytest.raises(TransitionLostError):
            manager._claim_and_launch(ready)
        monkeypatch.undo()
        writer.close()

    assert fired == [1]
    # Nothing was lost and nothing was duplicated: the job carries exactly the
    # one marker the winning transition left it at.
    assert [marker.path for marker in workspace.scan_markers()] == [rival_path]
    assert workspace.check().ok


# ---------------------------------------------------------------------------
# 4. Destinations that are not visible yet
# ---------------------------------------------------------------------------


def _blind_to_destination(monkeypatch: pytest.MonkeyPatch, *, kind: str, job_key: str, times: int) -> list[int]:
    """Hide every ``state/<kind>/`` marker of *job_key* from the next probes.

    This is the stale-attribute-cache case: the rename succeeded, and the
    client simply cannot see the destination yet however often it looks.
    """

    real = Path.is_file
    probes = [0]
    remaining = [times]

    def is_file(self: Path) -> bool:
        text = str(self)
        if remaining[0] and job_key in self.name and f"{os.sep}state{os.sep}{kind}{os.sep}" in text:
            remaining[0] -= 1
            probes[0] += 1
            return False
        return real(self)

    monkeypatch.setattr(Path, "is_file", is_file)
    return probes


def test_a_transition_concludes_correctly_once_a_late_destination_becomes_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="invisible")
    submitted = workspace.submit(payload, "project/invisible")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        probes = _blind_to_destination(monkeypatch, kind="ready", job_key=submitted.job_key, times=4)
        manager._register_submissions()
        monkeypatch.undo()
        # The destination probe, the placement probe, and the rescan behind it
        # all came back empty before the marker finally became visible.
        assert probes[0] >= 3
        ready = workspace.find_marker_by_id(job_id)
        assert ready is not None and ready.kind == "ready"
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert len(list(workspace.scan_markers())) == 1
    assert workspace.check().ok


def test_a_destination_invisible_past_the_deadline_is_contained_by_the_tick(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root, policy={"visibility_deadline_seconds": 0.05})
    refused, refused_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="refused")
    contained, contained_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="contained")
    workspace.submit(refused, "project/refused")
    workspace.submit(contained, "project/contained")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = workspace.find_marker_by_id(refused_id)
        assert ready is not None and ready.kind == "ready"
        # Nothing this job's marker moves to will ever become visible.
        _blind_to_destination(monkeypatch, kind="claimed", job_key="", times=10_000)

        # The workspace layer refuses to guess: an unresolvable rename is
        # reported as an unavailable workspace, never as a lost race.
        with pytest.raises(WorkspaceUnavailableError):
            manager._claim_and_launch(ready)

        # A whole tick contains exactly that condition for the next job, and the
        # manager keeps serving the workspace afterwards.
        with caplog.at_level("ERROR", logger="httk.workflow"):
            manager.tick()
        monkeypatch.undo()
        assert any("cannot claim or launch" in record.getMessage() for record in caplog.records)
        assert manager.tick() is not None

    # Both jobs are intact: one marker each, resolving to their own frames.
    kinds = {marker.job_id: marker.kind for marker in workspace.scan_markers()}
    assert kinds == {refused_id: "claimed", contained_id: "claimed"}
    assert workspace.check().ok
