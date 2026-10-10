"""Several managers over one workspace: contended claims, requests, joins, drains and recovery.

Every test here drives at least two independent :class:`TaskManager` owners
against one workspace directory, because contested multi-writer operation over
a shared filesystem is what the job kernel arbitrates. Whether a job was
launched once or twice is never inferred from manager bookkeeping: the runner
itself records its attempt against the job id with an exclusive create, so a
second launch of the same job leaves durable evidence even if every job ends up
looking right.
"""

import json
import os
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _kernel, _requests, _store
from test_crash_injection import _all_jobs, _log

pytestmark = [pytest.mark.slow, pytest.mark.timing, pytest.mark.xdist_group("concurrency-timing")]

#: Records the attempt that ran this job into ``parameters.log`` with an exclusive create keyed on the job id, so
#: a second attempt of the same job lands in ``duplicates/`` instead; then sleeps ``parameters.delay`` seconds
#: (sleeping for good on a first attempt with ``parameters.hang``) and succeeds.
_CLAIM_RECORDING_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, time

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job = json.loads((pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
parameters = job["parameters"]
log = pathlib.Path(parameters["log"])
(log / "claims").mkdir(parents=True, exist_ok=True)
(log / "duplicates").mkdir(parents=True, exist_ok=True)
try:
    handle = os.open(str(log / "claims" / context["job_id"]), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
except FileExistsError:
    (log / "duplicates" / (context["job_id"] + "." + context["attempt_id"])).write_text(context["attempt_id"])
else:
    with os.fdopen(handle, "w") as stream:
        stream.write(context["attempt_id"])
pathlib.Path("started").write_text(str(os.getpid()))
if parameters.get("hang") and not context["is_restart"]:
    time.sleep(600)
time.sleep(parameters.get("delay", 0))
draft = control / "outcome.tmp.x"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def recording(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "recording", name="recording", executables={"run": _CLAIM_RECORDING_RUNNER})


def _submit(
    ws: Workspace,
    installed: _store.Installed,
    log: Path,
    count: int,
    *,
    pool: str = "default",
    delay: float = 0.0,
    hang: bool = False,
    **options: Any,
) -> list[str]:
    parameters = {"log": str(log), "delay": delay, "hang": hang}
    return [
        h.submit(
            ws,
            installed,
            {"start": "succeed"},
            placement=f"project/{index}",
            pool=pool,
            parameters=parameters,
            **options,
        ).job_id
        for index in range(count)
    ]


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


def _states(ws: Workspace) -> dict[str, str]:
    return {ref.job_id: ref.state for ref in _all_jobs(ws)}


def _assert_each_ran_once(ws: Workspace, log: Path, job_ids: list[str]) -> None:
    assert sorted(path.name for path in (log / "claims").iterdir()) == sorted(job_ids)
    assert list((log / "duplicates").iterdir()) == []
    for job_id in job_ids:
        ref = h.find(ws, job_id)
        assert ref.state == "succeeded"
        # One claim log entry per job means one attempt per job, whichever manager won it.
        assert h.state_of(ref).counters["attempts_total"] == 1
        assert [line["event"] for line in _log(ref)].count("launched") == 1


def _launched_by(ref: _kernel.JobRef) -> list[object]:
    return [line["owner_id"] for line in _log(ref) if line["event"] == "launched"]


def _bury(ws: Workspace, owner_id: str) -> None:
    """Make an abandoned in-process manager look like a dead process to the death proof.

    A test "kills" a manager by throwing its instance away inside the test process,
    so its ``owner.json`` still names a live pid. Pointing it at a reaped child's
    pid gives the probe of its successors the evidence a real crash leaves.
    """

    process = subprocess.Popen(["true"])
    process.wait()
    path = ws.control / "owners" / owner_id / "owner.json"
    record = json.loads(path.read_text(encoding="utf-8"))
    record["pid"] = process.pid
    path.write_text(json.dumps(record), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Contended claiming
# ---------------------------------------------------------------------------


def test_two_interleaved_managers_claim_every_ready_job_exactly_once(
    ws: Workspace, recording: _store.Installed, tmp_path: Path
) -> None:
    log = tmp_path / "claimlog"
    job_ids = _submit(ws, recording, log, 6)
    with (
        TaskManager(ws, heartbeat_interval=0.01, maximum_workers=3) as manager_a,
        TaskManager(ws, heartbeat_interval=0.01, maximum_workers=3) as manager_b,
    ):
        _interleave(
            (manager_a, manager_b),
            until=lambda: sorted(_states(ws).values()) == ["succeeded"] * len(job_ids),
        )
    _assert_each_ran_once(ws, log, job_ids)


def test_a_lost_claim_race_leaves_exactly_one_winner_and_a_clean_loser(
    ws: Workspace, recording: _store.Installed, tmp_path: Path
) -> None:
    log = tmp_path / "claimlog"
    (job_id,) = _submit(ws, recording, log, 1)
    with (
        TaskManager(ws, heartbeat_interval=0.01) as manager_a,
        TaskManager(ws, heartbeat_interval=0.01) as manager_b,
    ):
        # Both managers saw the same ready job before either of them acted on it.
        seen = h.only(ws, "ready")
        results = [manager._claim_and_launch(seen) for manager in (manager_a, manager_b)]
        assert results == [True, False]
        # The loser holds nothing: no local attempt, no owned job, no half-written state.
        assert manager_b.running_attempts == 0 and not manager_b.owner.owned()
        assert manager_a.running_attempts == 1
        # And it keeps working: its very next tick is an ordinary one.
        manager_b.tick()
        _interleave((manager_a, manager_b), until=lambda: _states(ws).get(job_id) == "succeeded")
    _assert_each_ran_once(ws, log, [job_id])
    assert len(_all_jobs(ws)) == 1


def test_two_threads_ticking_one_workspace_launch_every_job_exactly_once(
    ws: Workspace, recording: _store.Installed, tmp_path: Path
) -> None:
    log = tmp_path / "claimlog"
    job_ids = _submit(ws, recording, log, 8)
    failures: list[BaseException] = []

    def serve() -> None:
        try:
            with TaskManager(Workspace(ws.root, durable=False), heartbeat_interval=0.01, maximum_workers=2) as manager:
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
    _assert_each_ran_once(ws, log, job_ids)


# ---------------------------------------------------------------------------
# 2. Contended operator requests and joins
# ---------------------------------------------------------------------------


def test_an_operator_request_is_applied_by_exactly_one_manager(
    ws: Workspace, recording: _store.Installed, tmp_path: Path
) -> None:
    # A pool neither manager serves keeps every job in ready, so the request pass is the only thing that ever
    # moves a job in this test.
    job_ids = _submit(ws, recording, tmp_path / "claimlog", 6, pool="nobody")
    for index, job_id in enumerate(job_ids):
        _requests.post(
            ws, action="pause", job_id=job_id, placement=f"project/{index}", operator="tester", reason="contended"
        )

    with (
        TaskManager(ws, heartbeat_interval=0.01) as manager_a,
        TaskManager(ws, heartbeat_interval=0.01) as manager_b,
    ):
        barrier = threading.Barrier(2)
        failures: list[BaseException] = []

        def handle(manager: TaskManager) -> None:
            try:
                barrier.wait(timeout=30.0)
                for _ in range(20):
                    manager._requests_pass()
            except BaseException as exc:  # pragma: no cover - reported by the assertion below
                failures.append(exc)

        threads = [threading.Thread(target=handle, args=(manager,)) for manager in (manager_a, manager_b)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60.0)
            assert not thread.is_alive()
        assert failures == []

    # Each request was applied exactly once, by whichever manager claimed its job first.
    for job_id in job_ids:
        ref = h.find(ws, job_id)
        assert ref.state == "paused"
        applied = [line for line in _log(ref) if line["event"] == "request_applied"]
        assert len(applied) == 1
        # The release recorded the applied request, so a leftover file could never apply twice. A manager that
        # listed the request before its file was deleted may later claim the job, find the file gone and prune
        # the id on release, which is equally safe.
        detail = applied[0]["detail"]
        assert isinstance(detail, dict) and h.state_of(ref).applied_requests in ((detail["request_id"],), ())
    assert list((ws.control / "requests").iterdir()) == []


def test_a_join_resolves_when_the_children_run_on_the_other_manager(ws: Workspace, tmp_path: Path) -> None:
    installed = h.install(ws, tmp_path / "demo")
    spawn = {"children": [{"label": "only", "script": {"start": "succeed"}, "placement": "children/only"}]}
    parent = h.submit(ws, installed, {"start": "spawn", "gather": "succeed"}, parameters={"spawn": spawn})
    with (
        TaskManager(ws, heartbeat_interval=0.01, placement_prefixes=("project",)) as manager_a,
        TaskManager(ws, heartbeat_interval=0.01, placement_prefixes=("children",)) as manager_b,
    ):
        _interleave((manager_a, manager_b), until=lambda: _states(ws).get(parent.job_id) == "succeeded")
        alpha, beta = manager_a.manager_id, manager_b.manager_id

    done = h.find(ws, parent.job_id)
    (child,) = [ref for ref in _all_jobs(ws) if ref.job_id != parent.job_id]
    assert child.state == "succeeded"
    assert _launched_by(done) == [alpha, alpha]
    assert _launched_by(child) == [beta]
    # The parent's join observed, by label, the child the other manager ran.
    doc = h.state_of(done)
    assert [(item["label"], item["state"]) for item in doc.observations] == [("only", "succeeded")]


def test_both_managers_drain_without_stranding_the_work_they_started(
    ws: Workspace, recording: _store.Installed, tmp_path: Path
) -> None:
    job_ids = _submit(ws, recording, tmp_path / "claimlog", 4, delay=0.4)
    with (
        TaskManager(ws, heartbeat_interval=0.01) as manager_a,
        TaskManager(ws, heartbeat_interval=0.01) as manager_b,
    ):
        _interleave(
            (manager_a, manager_b), until=lambda: bool(manager_a.running_attempts and manager_b.running_attempts)
        )
        manager_a._draining = True
        manager_b._draining = True
        # A drain completes once its own attempts are reaped and committed: each manager commits only the
        # outcomes it owns, and claims nothing new.
        _interleave(
            (manager_a, manager_b),
            until=lambda: all(
                not manager.running_attempts and not manager.owner.owned() for manager in (manager_a, manager_b)
            ),
        )
    states = _states(ws)
    assert set(states) == set(job_ids)
    assert sorted(states.values()) == ["ready", "ready", "succeeded", "succeeded"]


# ---------------------------------------------------------------------------
# 3. One manager dies, the other recovers
# ---------------------------------------------------------------------------


class _StoppedManager(Exception):
    """Raised in place of the work a manager never got to do."""


def test_a_claim_abandoned_mid_launch_is_recovered_by_the_other_manager(
    ws: Workspace, recording: _store.Installed, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = tmp_path / "claimlog"
    (job_id,) = _submit(ws, recording, log, 1)
    manager_a = TaskManager(ws, heartbeat_interval=0.01)
    with TaskManager(ws, heartbeat_interval=0.01) as manager_b:
        # Manager A stops existing between its claim and its launch.
        with monkeypatch.context() as patch:

            def stop(*arguments: object) -> None:
                raise _StoppedManager()

            patch.setattr(TaskManager, "_launch", stop)
            with pytest.raises(_StoppedManager):
                manager_a.tick()
        abandoned = h.find(ws, job_id)
        assert abandoned.state == _kernel.OWNED and abandoned.owner_id == manager_a.manager_id

        # Nothing is taken from an owner that is not proven dead: its owner.json still names a live process.
        manager_b.tick()
        assert h.find(ws, job_id).owner_id == manager_a.manager_id

        # Once its process is provably gone (and it started no launch), the other manager recovers the claim.
        _bury(ws, manager_a.manager_id)
        manager_b.tick()
        assert h.find(ws, job_id).owner_id != manager_a.manager_id
        _interleave((manager_b,), until=lambda: _states(ws).get(job_id) == "succeeded")

    _assert_each_ran_once(ws, log, [job_id])
    # The abandoned claim consumed no budget: the job ran its first attempt.
    doc = h.state_of(h.find(ws, job_id))
    assert doc.attempt is not None and doc.attempt["ordinal"] == 1 and doc.attempt["unclean"] is False
    (tombstone,) = [item for item in _kernel.list_owners(ws) if item.tombstone is not None]
    assert tombstone.owner_id == manager_a.manager_id and tombstone.tombstone is not None
    assert tombstone.tombstone["by"] == "probe"


def test_a_running_attempt_is_recovered_only_once_its_process_group_is_proven_gone(
    ws: Workspace, recording: _store.Installed, tmp_path: Path
) -> None:
    log = tmp_path / "claimlog"
    (job_id,) = _submit(ws, recording, log, 1, pool="alpha", hang=True, retry_policy={"retry_on": ["owner_lost"]})
    manager_a = TaskManager(ws, heartbeat_interval=0.01, pools=("alpha",))
    # A pool manager B does not serve keeps the recovered job in ready rather than relaunching it here.
    with TaskManager(ws, heartbeat_interval=0.01, pools=("beta",)) as manager_b:
        _interleave((manager_a,), until=lambda: any((ws.jobs / "owned").glob("*/*/run/started")))
        (attempt,) = manager_a._running.values()
        _bury(ws, manager_a.manager_id)

        # The manager process is gone, but its attempt still runs: a second writer there would corrupt the job,
        # so the death proof does not hold and B leaves it alone.
        manager_b.tick()
        assert h.find(ws, job_id).owner_id == manager_a.manager_id

        os.killpg(attempt.process.pid, 9)
        attempt.process.wait(timeout=30)
        manager_b.tick()

    recovered = h.find(ws, job_id)
    assert recovered.state == "ready"
    doc = h.state_of(recovered)
    # Recovery changed nothing: the next claimant decides what the dead attempt means.
    assert doc.phase["kind"] == "running" and doc.owner_id == manager_a.manager_id

    h.run(ws, pools=("alpha",))
    done = h.find(ws, job_id)
    assert done.state == "succeeded"
    doc = h.state_of(done)
    assert [item["code"] for item in doc.failure_history] == ["owner_lost"]
    assert doc.attempt is not None and doc.attempt["unclean"] is True
    assert len(list((log / "duplicates").iterdir())) == 1
