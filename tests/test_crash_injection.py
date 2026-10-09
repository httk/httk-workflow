"""Killing a manager at every step of the commit and replaying it from ``state.json``.

The commit of one outcome is decided once, in the ``commit`` intent of
``state.json``, and then executed by idempotent steps: the committed
transactions are applied, the children published, the job sealed, and the job
released by the release rule. Each case below stops a manager dead at one of
those points (the owner fail-stops, as it does when it is declared dead), has an
operator attest it dead and recover its jobs, and lets a fresh owner finish the
work. The commit must complete *exactly* once: every transaction entry applied
once, one child published, no step rerun, and one terminal job.

The second half injects the storage failures the contested and owned moves of
the kernel exist for — a rename that happened but reported failure, a rename
that another actor won, and a destination that is not visible yet — because a
manager that cannot tell those apart either duplicates work or loses a job.
"""

import errno
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _death, _fs, _kernel, _store

pytestmark = pytest.mark.slow

SOURCE = Path(__file__).resolve().parents[1] / "src"

#: The payload a heavy job starts with: committed transactions merge into ``data/`` rather than replace it.
_MEMBERS = {"data/README": "inputs\n"}
#: What the first step stages through one committed transaction.
_PUTS = {f"data/result-{index}.txt": f"result {index}\n" for index in range(3)}


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "demo")


def _submit_heavy(ws: Workspace, installed: _store.Installed) -> _kernel.JobRef:
    """Submit one job whose first commit exercises every commit step: transactions, a child and a join."""

    return h.submit(
        ws,
        installed,
        {"start": "spawn", "gather": "succeed"},
        members=_MEMBERS,
        parameters={
            "spawn": {"children": [{"label": "only", "script": {"start": "succeed"}}]},
            "put": {"start": _PUTS},
        },
    )


def _all_jobs(ws: Workspace) -> list[_kernel.JobRef]:
    """Every job directory of the workspace, unowned or owned."""

    refs = [ref for state in _kernel.UNOWNED_STATES for ref in _kernel.list_jobs(ws, state)]
    owned = ws.jobs / "owned"
    for owner in sorted(owned.iterdir()) if owned.is_dir() else ():
        refs += [_kernel.JobRef.from_path(path, state=_kernel.OWNED, owner_id=owner.name) for path in owner.iterdir()]
    return refs


def _children(ws: Workspace, parent_id: str) -> list[_kernel.JobRef]:
    return [ref for ref in _all_jobs(ws) if ref.job_id != parent_id]


def _parent(ws: Workspace, parent_id: str) -> _kernel.JobRef:
    (ref,) = [ref for ref in _all_jobs(ws) if ref.job_id == parent_id]
    return ref


def _log(ref: _kernel.JobRef) -> list[dict[str, object]]:
    lines = (ref.path / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


@contextmanager
def _fault(hook: Callable[[str, str, _fs.Loc | None, _fs.Loc | None], None]) -> Iterator[None]:
    """Install a fault injector at the crash points of :mod:`httk.workflow._fs` for the duration."""

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(_fs, "_fault", hook)
        yield


def _crash() -> None:
    raise _kernel.OwnerLost("simulated crash")


def _crash_and_recover(ws: Workspace, arm: Callable[[pytest.MonkeyPatch], None]) -> str:
    """Run a manager until it fail-stops at the armed point, attest it dead and recover its jobs.

    :return: The dead manager's owner id.
    """

    with pytest.MonkeyPatch.context() as patch:
        arm(patch)
        manager = TaskManager(ws, heartbeat_interval=0.01)
        with pytest.raises(_kernel.OwnerLost):
            manager.run_until_idle(timeout=60)
        manager.close()
    # The dead manager left its jobs exactly as they were: nothing ran after the crash.
    assert not manager.running_attempts
    _kernel.attest_dead(ws, manager.manager_id, by="operator", evidence=[], reason="test crash")
    with h.cli_owner(ws) as owner:
        _kernel.recover(ws, owner, manager.manager_id)
    return manager.manager_id


def _crash_on_entry(
    target: object, name: str, *, when: Callable[..., bool] = lambda *_a, **_k: True
) -> Callable[[pytest.MonkeyPatch], None]:
    """Arm a crash at the entry of *target.name*, the first time *when* holds for its arguments."""

    def arm(patch: pytest.MonkeyPatch) -> None:
        real = getattr(target, name)
        fired: list[bool] = []

        def crash(*arguments: object, **options: object) -> object:
            if not fired and when(*arguments, **options):
                fired.append(True)
                _crash()
            return real(*arguments, **options)

        patch.setattr(target, name, crash)

    return arm


def _crash_at_rename(
    phase: str, matches: Callable[[_fs.Loc, _fs.Loc], bool], *, ordinal: int = 1
) -> Callable[[pytest.MonkeyPatch], None]:
    """Arm a crash at the *ordinal*-th ``_fs`` rename (``before`` or ``after`` it) that *matches*."""

    def arm(patch: pytest.MonkeyPatch) -> None:
        seen = [0]

        def hook(op: str, at: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
            if op != "rename" or at != phase or src is None or dst is None or not matches(src, dst):
                return
            seen[0] += 1
            if seen[0] == ordinal:
                _crash()

        patch.setattr(_fs, "_fault", hook)

    return arm


# ---------------------------------------------------------------------------
# 1. Every interruption point of the commit
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Interruption:
    """One point of the commit, where it leaves the parent, and whether the next owner recovers it."""

    name: str
    arm: Callable[[str], Callable[[pytest.MonkeyPatch], None]]
    state_after_kill: str
    recovered: bool = True


def _is_parent(parent_id: str) -> Callable[..., bool]:
    def when(_self: object, owned: object, *_rest: object, **_options: object) -> bool:
        return getattr(owned, "job_id", None) == parent_id

    return when


def _second_call(parent_id: str) -> Callable[..., bool]:
    calls = [0]

    def when(_self: object, owned: object, *_rest: object, **_options: object) -> bool:
        if getattr(owned, "job_id", None) == parent_id:
            calls[0] += 1
        return calls[0] == 2

    return when


_INTERRUPTIONS = (
    # The attempt ended and its outcome is published, but no intent is written: the outcome is committed, not rerun.
    _Interruption(
        "before-the-commit-intent",
        lambda parent: _crash_on_entry(TaskManager, "_commit_outcome", when=_is_parent(parent)),
        "ready",
    ),
    # Between two entries of one committed transaction.
    _Interruption(
        "mid-transaction",
        lambda parent: _crash_at_rename("before", lambda src, dst: "/txn/" in str(src.path), ordinal=2),
        "ready",
    ),
    # The child is published; the parent still carries its intent.
    _Interruption(
        "after-the-child-is-published",
        lambda parent: _crash_at_rename("after", lambda src, dst: "/children/jobs/" in str(src.path)),
        "ready",
    ),
    # The final state is computed but neither written nor released.
    _Interruption(
        "before-the-release",
        lambda parent: _crash_on_entry(TaskManager, "_release", when=_is_parent(parent)),
        "ready",
    ),
    # The release rename happened: the parent waits, and nothing is left to recover.
    _Interruption(
        "after-the-release-rename",
        lambda parent: _crash_at_rename("after", lambda src, dst: "/jobs/waiting/" in str(dst.path)),
        "waiting",
        recovered=False,
    ),
    # The second commit sealed the parent, then died before its attempt directory went.
    _Interruption(
        "after-the-seal",
        lambda parent: _crash_on_entry(TaskManager, "_remove_attempt", when=_second_call(parent)),
        "ready",
    ),
)


def _assert_completed_exactly_once(ws: Workspace, parent_id: str) -> _kernel.JobRef:
    """The heavy job finished once: one child, each entry applied once, each step run once."""

    parent = _parent(ws, parent_id)
    assert parent.state == "succeeded"
    (child,) = _children(ws, parent_id)
    assert child.state == "succeeded"
    assert len([line for line in _log(child) if line["event"] == "launched"]) == 1
    data = parent.path / "data"
    assert sorted(path.name for path in data.iterdir()) == ["README", *(Path(name).name for name in sorted(_PUTS))]
    for name, text in _PUTS.items():
        assert (parent.path / name).read_text(encoding="utf-8") == text
    log = _log(parent)
    started = [line for line in log if line["event"] == "attempt_started"]
    # No step was rerun: one attempt of the spawning step, one of the gathering step.
    assert [line["step"] for line in started] == ["start", "gather"]
    doc = h.state_of(parent)
    assert doc.counters == {"activations": 2, "attempts_total": 2}
    assert [str(entry["label"]) for entry in doc.children] == ["only"]
    assert [item["state"] for item in doc.observations] == ["succeeded"]
    assert doc.commit is None and doc.failure is None and doc.failure_history == ()
    seal = parent.path / ".httk-job" / "seal.json"
    assert doc.seal is not None and seal.is_file()
    # Nothing is left owned, staged or half-removed.
    assert not list((ws.jobs / "owned").glob("*/*"))
    assert not (parent.path / "attempts").exists()
    return parent


@pytest.mark.parametrize("interruption", _INTERRUPTIONS, ids=lambda item: item.name)
def test_a_fresh_manager_completes_an_interrupted_commit_exactly_once(
    ws: Workspace, installed: _store.Installed, interruption: _Interruption
) -> None:
    submitted = _submit_heavy(ws, installed)
    dead = _crash_and_recover(ws, interruption.arm(submitted.job_id))

    interrupted = _parent(ws, submitted.job_id)
    assert interrupted.state == interruption.state_after_kill
    doc = h.state_of(interrupted)
    if interruption.name == "before-the-commit-intent":
        # The dead owner left a running attempt whose outcome is published.
        assert doc.phase["kind"] == "running" and doc.commit is None
    elif interruption.name == "after-the-release-rename":
        assert doc.commit is None and doc.join is not None
    else:
        assert doc.commit is not None
    if interruption.name == "mid-transaction":
        # The manager really did die between two entries of one transaction.
        assert sorted(path.name for path in (interrupted.path / "data").iterdir()) == ["README", "result-0.txt"]
    if interruption.name in ("after-the-child-is-published", "before-the-release"):
        (child,) = _children(ws, submitted.job_id)
        assert child.state == "ready"

    h.run(ws)

    parent = _assert_completed_exactly_once(ws, submitted.job_id)
    log = _log(parent)
    recovered = [line for line in log if line["event"] == "recovered"]
    if interruption.recovered:
        # The next claimant recognised the dead owner's unfinished work.
        assert recovered and recovered[0]["detail"] == dead
    else:
        assert not recovered
    # The dead manager keeps only its tombstone.
    (tombstone,) = [item for item in _kernel.list_owners(ws) if item.tombstone is not None]
    assert tombstone.owner_id == dead and sorted(path.name for path in tombstone.path.iterdir()) == ["dead.json"]


def test_a_commit_replayed_by_an_owner_that_then_dies_completes_exactly_once(
    ws: Workspace, installed: _store.Installed
) -> None:
    """A successor killed in the middle of its own replay is itself replayed."""

    submitted = _submit_heavy(ws, installed)
    first = _crash_and_recover(ws, _crash_at_rename("before", lambda src, dst: "/txn/" in str(src.path), ordinal=2))
    # The second owner resumes the transactions and dies right before it publishes the child.
    second = _crash_and_recover(ws, _crash_at_rename("before", lambda src, dst: "/children/jobs/" in str(src.path)))
    interrupted = _parent(ws, submitted.job_id)
    assert interrupted.state == "ready" and h.state_of(interrupted).commit is not None
    assert not _children(ws, submitted.job_id)
    assert sorted(path.name for path in (interrupted.path / "data").iterdir()) == [
        "README",
        *(Path(name).name for name in sorted(_PUTS)),
    ]

    h.run(ws)

    parent = _assert_completed_exactly_once(ws, submitted.job_id)
    recovered = [line for line in _log(parent) if line["event"] == "recovered"]
    # Each claimant names the owner whose state.json it found unfinished: the second owner died before it wrote
    # any, so the third also finds the first owner's intent.
    assert [line["detail"] for line in recovered] == [first, first]
    assert recovered[0]["owner_id"] == second and recovered[1]["owner_id"] not in (first, second)


def test_a_commit_resumes_after_its_published_child_has_already_run(ws: Workspace, installed: _store.Installed) -> None:
    """A child published by an interrupted commit is a job in its own right.

    Publication is the point of no return for a child, so another manager may
    run it at once. When the interrupted parent commit is resumed, the staged
    child is gone (it was published), and the replay must not publish or
    validate it again.
    """

    submitted = _submit_heavy(ws, installed)
    _crash_and_recover(ws, _crash_at_rename("after", lambda src, dst: "/children/jobs/" in str(src.path)))
    (child,) = _children(ws, submitted.job_id)
    assert child.state == "ready"

    # Another manager serves only the child's subtree and runs it to completion, leaving the parent alone.
    placement = child.placement
    assert placement is not None
    h.run(ws, placement_prefixes=(placement.as_posix(),))
    (child,) = _children(ws, submitted.job_id)
    assert child.state == "succeeded"
    assert h.state_of(_parent(ws, submitted.job_id)).commit is not None

    h.run(ws)

    _assert_completed_exactly_once(ws, submitted.job_id)


# ---------------------------------------------------------------------------
# 2. A real process, killed with SIGKILL
# ---------------------------------------------------------------------------

#: Records that it started, waits long enough for its manager to be killed under it, and then publishes its
#: outcome anyway: the attempt runs in its own process group, so it outlives its manager.
_ORPHANABLE_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, time

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
with open("steps.log", "a") as stream:
    stream.write(context["step"] + "\\n")
pathlib.Path("started").touch()
time.sleep(3.0)
draft = control / "outcome.tmp.x"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


def _wait_for(condition: Callable[[], bool], *, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


@pytest.mark.timing
def test_a_sigkilled_manager_process_leaves_a_job_a_fresh_manager_finishes(ws: Workspace, tmp_path: Path) -> None:
    installed = h.install(ws, tmp_path / "orphan", executables={"run": _ORPHANABLE_RUNNER})
    submitted = h.submit(ws, installed, {"start": "succeed"}, retry_policy={"retry_on": ["owner_lost"]})
    code = (
        "from httk.workflow import TaskManager, Workspace\n"
        f"TaskManager(Workspace({str(ws.root)!r}, durable=False)).run_until_idle(timeout=120)\n"
    )
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    process = subprocess.Popen([sys.executable, "-c", code], env=environment)
    try:
        assert _wait_for(lambda: any((ws.jobs / "owned").glob("*/*/run/started")))
    finally:
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=60)
    assert process.returncode == -signal.SIGKILL
    (dead,) = [item.owner_id for item in _kernel.list_owners(ws) if item.record is not None]

    with TaskManager(ws, heartbeat_interval=0.01) as fresh:
        # The manager is gone, but its attempt still runs: the owner is not proven dead, so nothing is recovered.
        fresh.tick()
        assert _parent(ws, submitted.job_id).state == _kernel.OWNED
        assert _kernel.probe_owner(ws, dead, scheduler=fresh._scheduler) is not _death.Liveness.DEAD

        # Once the orphaned attempt published its outcome and exited, the death proof holds.
        def finished() -> bool:
            fresh.tick()
            return _parent(ws, submitted.job_id).state == "succeeded"

        assert _wait_for(finished, timeout=90.0)

    done = _parent(ws, submitted.job_id)
    # Recovery committed the published outcome instead of rerunning the step.
    assert (done.path / "run" / "steps.log").read_text(encoding="utf-8").splitlines() == ["start"]
    doc = h.state_of(done)
    assert doc.counters["attempts_total"] == 1 and doc.failure_history == ()
    log = _log(done)
    assert [line["event"] for line in log].count("launched") == 1
    assert any(line["event"] == "recovered" and line["detail"] == dead for line in log)
    (tombstone,) = [item for item in _kernel.list_owners(ws) if item.tombstone is not None]
    assert tombstone.owner_id == dead and tombstone.tombstone is not None and tombstone.tombstone["by"] == "probe"


# ---------------------------------------------------------------------------
# 3. Renames that lie, the way a network filesystem lies
# ---------------------------------------------------------------------------

_RenamePath = str | os.PathLike[str]


def _lying_rename(patch: pytest.MonkeyPatch, *, source: str, destination: str = "", perform: bool) -> list[int]:
    """Make the first rename from a path containing *source* (to one containing *destination*) report a failure.

    With ``perform`` the rename is carried out first, which is exactly what a
    retransmitted NFS rename whose first reply was lost does: the operation
    happened, and the caller is told it did not.
    """

    real = os.rename
    fired = [0]

    def rename(src: _RenamePath, dst: _RenamePath, **keywords: int | None) -> None:
        if not fired[0] and source in os.fspath(src) and destination in os.fspath(dst):
            fired[0] += 1
            if perform:
                real(src, dst, **keywords)
            raise OSError(errno.EIO, "simulated retransmitted rename whose reply was lost")
        real(src, dst, **keywords)

    patch.setattr(os, "rename", rename)
    return fired


def _single_success(ws: Workspace, job_id: str) -> _kernel.JobRef:
    """The job succeeded with one claim and one attempt, and it is the workspace's only job."""

    (done,) = _all_jobs(ws)
    assert done.job_id == job_id and done.state == "succeeded"
    events = [line["event"] for line in _log(done)]
    assert events.count("claimed") == 1 and events.count("launched") == 1
    return done


def test_a_claim_whose_rename_happened_but_reported_failure_is_won(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    with pytest.MonkeyPatch.context() as patch:
        fired = _lying_rename(patch, source="/jobs/ready/", perform=True)
        h.run(ws)
    assert fired == [1]
    # The claimant observed its own destination and correctly concluded that it had won.
    _single_success(ws, submitted.job_id)


def test_an_outcome_release_whose_rename_happened_but_reported_failure_is_done(
    ws: Workspace, installed: _store.Installed
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    with pytest.MonkeyPatch.context() as patch:
        fired = _lying_rename(patch, source="/jobs/owned/", destination="/jobs/succeeded/", perform=True)
        h.run(ws)
    assert fired == [1]
    # An owned move is decided by its source alone: gone means done, so it is neither retried nor lost.
    done = _single_success(ws, submitted.job_id)
    assert [line["event"] for line in _log(done)].count("released") == 1


def test_a_rename_that_really_failed_is_simply_retried(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    with pytest.MonkeyPatch.context() as patch:
        # The rename genuinely did not happen, so the source is still there and the same rename is simply
        # attempted again.
        fired = _lying_rename(patch, source="/jobs/ready/", perform=False)
        h.run(ws)
    assert fired == [1]
    _single_success(ws, submitted.job_id)


def test_a_rename_that_failed_because_another_actor_won_is_reported_as_lost(
    ws: Workspace, installed: _store.Installed
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    rival = h.cli_owner(ws)
    real = os.rename
    fired: list[Path] = []

    def rename(src: _RenamePath, dst: _RenamePath, **keywords: int | None) -> None:
        if not fired and os.fspath(src) == str(submitted.path):
            # Another owner wins the very rename this one is about to attempt, and this one's reply is a
            # pruned destination parent.
            name = _kernel.format_job_name(submitted.job_key, submitted.priority, _fs.fresh_token(), "ready")
            won = ws.jobs / "owned" / rival.owner_id / name
            won.parent.mkdir(parents=True, exist_ok=True)
            real(src, won)
            fired.append(won)
            raise OSError(errno.ENOENT, "simulated rename onto a pruned destination parent")
        real(src, dst, **keywords)

    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "rename", rename)
            assert manager._claim_and_launch(submitted) is False
        # The loser holds nothing: no attempt, no owned job, no state written.
        assert manager.running_attempts == 0 and not manager.owner.owned()
        (won,) = fired
        assert [ref.path for ref in rival.owned()] == [won]
        assert not (won / "state.json").exists() and not (won / "logs").exists()
        # And it keeps working: its very next tick is an ordinary one.
        manager.tick()
    # The rival closes; its unheld job goes back unchanged, and runs once.
    rival.close()
    h.run(ws)
    _single_success(ws, submitted.job_id)


# ---------------------------------------------------------------------------
# 4. Destinations that are not visible yet
# ---------------------------------------------------------------------------


def _hide(patch: pytest.MonkeyPatch, *, matches: Callable[[Path], bool], times: int) -> list[int]:
    """Make the next *times* existence probes of matching paths report absence (a stale attribute cache)."""

    real = _fs.exists
    hidden = [0]

    def exists(target: _fs.Loc) -> bool:
        if hidden[0] < times and target.at is None and matches(target.path):
            hidden[0] += 1
            return False
        return real(target)

    patch.setattr(_fs, "exists", exists)
    return hidden


def test_a_claim_whose_destination_is_not_visible_yet_is_adopted_by_self_healing(
    ws: Workspace, installed: _store.Installed
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        with pytest.MonkeyPatch.context() as patch:
            owned = ws.jobs / "owned" / manager.manager_id
            hidden = _hide(patch, matches=lambda path: path.parent == owned, times=1)
            # Source gone, destination not visible yet: a claim decides without waiting, so it concludes LOST.
            assert manager._claim_and_launch(submitted) is False
        assert hidden == [1] and manager.running_attempts == 0
        # The won claim was not stranded: it is in owned/<self>/, where the next tick's self-healing adopts it.
        (stranded,) = manager.owner.owned()
        assert stranded.job_id == submitted.job_id
        manager.run_until_idle(timeout=60)
    (done,) = _all_jobs(ws)
    assert done.state == "succeeded"
    events = _log(done)
    assert [line["event"] for line in events].count("launched") == 1
    assert any(line["event"] == "recovered" and line["detail"] == "self-healed" for line in events)
