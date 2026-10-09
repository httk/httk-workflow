"""One owner per job: an owner declared dead stops without touching the jobs recovered from it.

A job leaves ``owned/<owner>/`` only by its owner's release, or by recovery
after the owner is proven (or attested) dead. Nothing is taken over on a
timeout, so the only way two writers could meet in one job is a false death
verdict: an operator's attestation, or a broken probe, of an owner that is in
fact still running. That owner must fail-stop at its next step — kill every
attempt it started and touch no job — so the work its successor finished is
exactly what one owner would have left.
"""

import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _data, _kernel, _store
from httk.workflow.errors import WorkflowError
from test_crash_injection import _all_jobs, _log


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "demo")


pytestmark = pytest.mark.slow


def _declare_dead_and_recover(ws: Workspace, owner_id: str) -> None:
    """What an operator's (false) attestation and any recoverer do to a live owner."""

    _kernel.attest_dead(ws, owner_id, by="operator", evidence=[], reason="mistaken attestation")
    with h.cli_owner(ws) as recoverer:
        _kernel.recover(ws, recoverer, owner_id)


def _lines_after_recovery(ref: _kernel.JobRef, owner_id: str) -> list[dict[str, object]]:
    """The run-log lines the frozen owner wrote after its job was recovered and claimed by another owner."""

    log = _log(ref)
    first_foreign = next(index for index, line in enumerate(log) if line["owner_id"] != owner_id)
    return [line for line in log[first_foreign:] if line["owner_id"] == owner_id]


def _gone(pid: int) -> bool:
    """Whether the process is gone (a killed process may linger briefly as a zombie of its reaper)."""

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.02)
    return False


def _wait_until(manager: TaskManager, condition: Callable[[], bool]) -> None:
    for _ in range(2000):
        manager.tick()
        if condition():
            return
    raise AssertionError("condition not reached")


def test_a_manager_declared_dead_mid_attempt_fail_stops_without_touching_the_recovered_job(
    ws: Workspace, installed: _store.Installed
) -> None:
    submitted = h.submit(ws, installed, {"start": "crash_once"}, retry_policy={"retry_on": ["owner_lost"]})
    frozen = TaskManager(ws, heartbeat_interval=0.01, cancel_grace_seconds=1.0)
    _wait_until(frozen, lambda: any((ws.jobs / "owned").glob("*/*/run/sleeping")))
    (sleeping,) = (ws.jobs / "owned").glob("*/*/run/sleeping")
    pid = int(sleeping.read_text(encoding="utf-8"))

    # The owner is declared dead while its attempt runs. Recovery cannot know the attempt still runs (it trusts
    # the tombstone), so the job goes back to ready with its attempt recorded as running.
    _declare_dead_and_recover(ws, frozen.manager_id)
    returned = h.find(ws, submitted.job_id)
    assert returned.state == "ready" and h.state_of(returned).phase["kind"] == "running"

    # The owner sees its tombstone at its next tick: it kills its attempt and stops, writing nothing.
    with pytest.raises(_kernel.OwnerDeclaredDead):
        frozen.tick()
    frozen.close()
    assert _gone(pid)
    with pytest.raises(WorkflowError, match="closed"):
        frozen.tick()
    unchanged = h.find(ws, submitted.job_id)
    assert unchanged.path == returned.path and h.state_of(unchanged) == h.state_of(returned)

    # The next owner treats the recorded attempt as lost and reruns it (as an unclean restart) under the retry
    # policy; the frozen owner wrote nothing after the recovery.
    h.run(ws)
    final = h.find(ws, submitted.job_id)
    assert final.state == "succeeded"
    events = _log(final)
    assert [line["event"] for line in events].count("launched") == 2
    recovered = [line for line in events if line["event"] == "recovered"]
    assert recovered and recovered[0]["detail"] == frozen.manager_id
    assert [item["code"] for item in h.state_of(final).failure_history] == ["owner_lost"]
    assert not _lines_after_recovery(final, frozen.manager_id)


def test_a_manager_declared_dead_mid_commit_never_writes_the_recovered_job(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A frozen owner wakes in the middle of a commit its successor already finished.

    The owner wrote its commit intent and is about to apply the committed
    transaction when it is declared dead. A successor recovers the job,
    replays the intent and releases it. When the frozen owner resumes, its job
    directory is gone: it must not recreate, write or move anything, and it
    fail-stops at its next tick.
    """

    puts = {"data/result.txt": "result\n"}
    submitted = h.submit(ws, installed, {"start": "succeed"}, parameters={"put": {"start": puts}})
    frozen = TaskManager(ws, heartbeat_interval=0.01)
    real = _data.apply_transactions
    fired: list[Path] = []

    def freeze_then_apply(job: _kernel.OwnedJob) -> int:
        if job.owner.owner_id == frozen.manager_id and not fired:
            fired.append(job.path)
            # While the owner is frozen here, it is declared dead and a successor finishes the commit.
            _declare_dead_and_recover(ws, frozen.manager_id)
            h.run(ws)
        return real(job)

    monkeypatch.setattr(_data, "apply_transactions", freeze_then_apply)
    with pytest.raises(_kernel.OwnerLost):
        frozen.run_until_idle(timeout=60)
    frozen.close()
    monkeypatch.undo()

    (stale,) = fired
    assert not os.path.lexists(stale) and not (ws.jobs / "owned" / frozen.manager_id).exists()
    (done,) = _all_jobs(ws)
    assert done.job_id == submitted.job_id and done.state == "succeeded"
    assert (done.path / "data" / "result.txt").read_text(encoding="utf-8") == "result\n"
    doc = h.state_of(done)
    assert doc.owner_id != frozen.manager_id and doc.seal is not None and doc.failure is None
    events = [line["event"] for line in _log(done)]
    assert events.count("launched") == 1 and events[-1] == "released"
    assert not _lines_after_recovery(done, frozen.manager_id)


def test_a_manager_declared_dead_between_its_claim_and_its_launch_launches_nothing(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    frozen = TaskManager(ws, heartbeat_interval=0.01)
    real = TaskManager._launch

    def declared_dead_first(self: TaskManager, *arguments: object) -> None:
        if self is frozen:
            _declare_dead_and_recover(ws, frozen.manager_id)
        real(self, *arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(TaskManager, "_launch", declared_dead_first)
    with pytest.raises(_kernel.OwnerDeclaredDead):
        frozen.tick()
    frozen.close()
    monkeypatch.undo()

    # The claim was returned unchanged: no attempt, no state written, no runner started.
    returned = h.find(ws, submitted.job_id)
    assert returned.state == "ready"
    assert not (returned.path / "state.json").exists() and not (returned.path / "attempts").exists()
    assert [line["event"] for line in _log(returned)] == ["claimed"]
    h.run(ws)
    done = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    doc = h.state_of(done)
    assert doc.counters["attempts_total"] == 1 and doc.attempt is not None and doc.attempt["ordinal"] == 1
