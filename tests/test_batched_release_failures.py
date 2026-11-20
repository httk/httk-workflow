"""The failure table, interrupts and the crash matrix of batched Release and Drop chunks (plan WORKFLOW_LOCAL_COMMANDS
phase L3b; amendments 11, 12, 17-20 and 24).

Every row of the failure table from C0 to D2 is a test: a commit point fails through the collector's fault seam,
each job ends where the table says, with the outcome it gives, and the command stops. Interrupts finish the applied
jobs through the remaining commit points and give back the unapplied ones, their requests withdrawn first while held.
A process killed before and after each commit point (a subprocess that exits from the fault seam without unwinding),
then dead-owner recovery and the next claimant, ends where the serial code would.
"""

import contextvars
import errno
import json
import os
import signal
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

import v3_helpers as v3
from httk.workflow import Workspace, _fs, _kernel, removal
from httk.workflow._state import read_state_unowned
from kernel._journal import record, where
from test_batched_release import _jobs, _requests_left, _where
from test_cli_job_request import _new_workspace
from test_local_commands import _identity  # noqa: F401  (_identity: an autouse fixture)

#: The commit points of a chunk, in order (the fault seam is called once per commit point).
_POINTS = ("C0", "A", "B", "C1", "C2", "D1", "D2")
_JOBS = 9


@pytest.fixture(autouse=True)
def _syncfs_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The protocol predicate holds (host-independent; ``syncfs`` is not really called), and no fault is left set."""

    monkeypatch.setattr(_fs, "_resolve_sync", lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4"))
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: None)
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    yield
    _fs.set_fault_injector(None)


def _at_commit(point: str, moment: str, action: Callable[[], None]) -> None:
    """Call *action* at the fault seam of *point*'s commit (``before`` or ``after`` it is durable)."""

    seen = {"before": 0, "after": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op != "commit":
            return
        seen[phase] += 1
        if phase == moment and seen[phase] == _POINTS.index(point) + 1:
            action()

    _fs.set_fault_injector(fault)


def _eio() -> None:
    raise _fs.SyncFailed(errno.EIO, "cannot sync to disk: Input/output error")


def _serve(
    workspace: Workspace, refs: list[_kernel.JobRef]
) -> tuple[list[removal.RequestOutcome], _kernel.Owner, BaseException | None]:
    """Request ``cancel`` of *refs* as one command; the outcomes reported, the owner, and what it raised."""

    reported: list[removal.RequestOutcome] = []
    raised: BaseException | None = None
    owner = v3.cli_owner(workspace)
    try:
        with owner:
            removal.request_jobs(workspace, owner, refs, "cancel", "batch", report=reported.extend)
    except (Exception, KeyboardInterrupt) as exc:
        raised = exc
    return reported, owner, raised


def _temporaries(workspace: Workspace) -> list[str]:
    return [
        str(path)
        for path in workspace.root.rglob("*.tmp")
        if path.name.startswith(".state.json.") or path.parent.name == "requests"
    ]


def _owned(workspace: Workspace, owner: _kernel.Owner) -> list[str]:
    directory = workspace.jobs / "owned" / owner.owner_id
    return sorted(os.listdir(directory)) if directory.is_dir() else []


# -- the failure table (amendments 11, 17-20) --------------------------------------------------------------------------


@pytest.mark.parametrize("point", _POINTS)
def test_a_failed_commit_point_ends_each_job_as_its_row_says(tmp_path: Path, point: str) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "f", _JOBS)
    _at_commit(point, "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    _fs.set_fault_injector(None)
    statuses = [outcome.status for outcome in outcomes]
    where = [_where(workspace, ref.job_id) for ref in refs]
    assert _temporaries(workspace) == []  # no request or state temporary is left behind
    assert _owned(workspace, owner) == []  # nothing is held
    assert not owner.latched  # every job decided: no obligation and no latch left
    if point in ("C0", "A"):
        # Nothing is held; every post is reported uncertain and the L1 withdrawal applies; the error propagates.
        assert isinstance(raised, _fs.SyncFailed)
        assert statuses == ["uncertain"] * _JOBS
        expected = "the request was not posted" if point == "C0" else "the request's post is uncertain"
        assert all(outcome.explanation.startswith(expected) for outcome in outcomes)
        assert where == ["ready/p500"] * _JOBS and _requests_left(workspace) == []
        return
    # Each job got its outcome, then the failed commit point stopped the command (amendments 3, 22; E6).
    assert isinstance(raised, removal._CommitStopped) and "Input/output error" in str(raised)
    if point in ("B", "C1"):
        # Unapplied: withdrawn while held, then given back, uncertain (E1, E2). After C1 the batch had logged their
        # application: a request_withdrawn line follows it.
        assert statuses == ["uncertain"] * _JOBS
        assert all("Input/output error" in outcome.explanation for outcome in outcomes)
        assert where == ["ready/p500"] * _JOBS and _requests_left(workspace) == []
        for ref in refs:
            events = _events(workspace, ref.job_id)
            assert events == ([] if point == "B" else ["claimed", "request_applied", "released", "request_withdrawn"])
    else:
        # C2: the serial apply finishes every job (never a give-back); D1: requests/ resealed, the moves finished;
        # D2: the moves resealed.
        assert statuses == ["applied"] * _JOBS
        assert where == ["cancelled/p500"] * _JOBS and _requests_left(workspace) == []


def _events(workspace: Workspace, job_id: str) -> list[str]:
    found = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
    assert found is not None
    runlog = found.path / "logs" / "runlog.jsonl"
    return [json.loads(line)["event"] for line in runlog.read_text().splitlines()] if runlog.is_file() else []


def test_the_c2_row_never_gives_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Amendment 18: the C2 row calls OwnedJob.release for every job still held, with the in-memory document.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "g", _JOBS)
    gave_back: list[str] = []
    give_back = _kernel.OwnedJob.give_back

    def recording(self: _kernel.OwnedJob) -> _kernel.JobRef:
        gave_back.append(self.job_id)
        return give_back(self)

    monkeypatch.setattr(_kernel.OwnedJob, "give_back", recording)
    _at_commit("C2", "before", _eio)
    outcomes, _owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._CommitStopped) and gave_back == []
    assert [outcome.status for outcome in outcomes] == ["applied"] * _JOBS
    for ref in refs:
        found = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
        assert found is not None
        doc, _damage = read_state_unowned(found.path / "state.json")
        assert doc is not None and len(doc.applied_requests) == 1 and doc.history_tail[-1]["event"] == "released"


def _fails_from(count: int, error: BaseException) -> Callable[..., object]:
    calls = {"n": 0}

    def maybe(*args: object, **kwargs: object) -> None:
        calls["n"] += 1
        if calls["n"] >= count:
            raise error

    return maybe


def test_a_finish_that_fails_keeps_its_job_and_fail_stops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "h", _JOBS)
    release = _kernel.OwnedJob.release
    failing = _fails_from(3, OSError(errno.EIO, "dead disk"))

    def releasing(self: _kernel.OwnedJob, *args: object, **kwargs: object) -> _kernel.JobRef:
        failing()
        return release(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel.OwnedJob, "release", releasing)
    _at_commit("C2", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop)  # after reporting every job
    assert [outcome.status for outcome in outcomes] == ["applied"] * 2 + ["uncertain"] * (_JOBS - 2)
    assert "stays held for recovery" in outcomes[2].explanation
    assert len(_owned(workspace, owner)) == _JOBS - 2  # held, nothing given back
    assert (owner.path / "owner.json").is_file() and owner.latched


def test_a_reseal_that_fails_keeps_the_jobs_and_fail_stops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # D1: requests/ cannot be resealed: no move is finished.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "i", _JOBS)
    monkeypatch.setattr(_kernel.Owner, "seal_obligations", lambda self: _eio())
    _at_commit("D1", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop)
    assert [outcome.status for outcome in outcomes] == ["uncertain"] * _JOBS
    assert len(_owned(workspace, owner)) == _JOBS and (owner.path / "owner.json").is_file()


def test_moves_whose_reseal_fails_are_reported_unresolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "j", _JOBS)
    monkeypatch.setattr(_kernel.Owner, "seal_obligations", lambda self: _eio())
    _at_commit("D2", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop)
    assert all(outcome.explanation.startswith("moved, durability unresolved") for outcome in outcomes)
    assert [_where(workspace, ref.job_id) for ref in refs] == ["cancelled/p500"] * _JOBS
    assert (owner.path / "owner.json").is_file() and owner.latched


def test_a_finish_whose_rename_happened_but_not_its_fsync_is_unresolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Amendment 20: the handle stays registered with the obligation recorded; the owner fail-stops.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "k", _JOBS)
    fsync_parents = _fs._fsync_parents
    calls = {"n": 0}

    def failing(src: _fs.Loc, dst: _fs.Loc) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(errno.EIO, "dead disk")
        fsync_parents(src, dst)

    monkeypatch.setattr(_fs, "_fsync_parents", failing)
    _at_commit("C2", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop)
    assert outcomes[0].status == "uncertain" and outcomes[0].explanation.startswith("moved, durability unresolved")
    assert _where(workspace, refs[0].job_id) == "cancelled/p500"
    assert any(handle.job_id == refs[0].job_id for handle in owner._held.values())  # still registered
    assert owner.latched and (owner.path / "owner.json").is_file()
    assert [outcome.status for outcome in outcomes[1:]] == ["uncertain"] * (_JOBS - 1)


@pytest.mark.parametrize("step", ["stage", "replace before its rename", "os.replace"])
def test_a_failed_step_before_the_replace_withdraws_while_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    # Amendments 19 and 28 (G2, G3): the job leaves the batch, stays held and is uncertain; its state was not replaced,
    # so its request is withdrawn while held, with request_withdrawn after the batch's request_applied.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "l", _JOBS)
    failing = refs[2].job_id
    if step == "stage":
        stage = _kernel.OwnedJob.stage_state

        def staging(self: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
            if self.job_id == failing:
                raise OSError(errno.ENOSPC, "No space left on device")
            return stage(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(_kernel.OwnedJob, "stage_state", staging)
    elif step == "replace before its rename":
        replace_state = _kernel.OwnedJob.replace_state

        def replacing(self: _kernel.OwnedJob, *args: object, **kwargs: object) -> None:
            if self.job_id == failing:
                raise OSError(errno.EIO, "Input/output error")  # its temporary remains
            replace_state(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(_kernel.OwnedJob, "replace_state", replacing)
    else:
        # os.replace fails without taking effect: replace_staged removes the temporary, which proves nothing (G2).
        replace = os.replace

        def failing_replace(src: object, dst: object, **kwargs: object) -> None:
            if str(dst).endswith("/state.json") and failing in str(dst):
                raise OSError(errno.ENOSPC, "No space left on device")
            replace(src, dst, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(os, "replace", failing_replace)
    outcomes, owner, raised = _serve(workspace, refs)
    # As at HEAD: the job stays held, so the owner cannot close.
    assert isinstance(raised, _kernel.WorkflowError) and "still holds 1 jobs" in str(raised)
    assert [outcome.status for outcome in outcomes] == ["applied"] * 2 + ["uncertain"] + ["applied"] * (_JOBS - 3)
    assert len(_owned(workspace, owner)) == 1
    assert _requests_left(workspace) == [] and _temporaries(workspace) == []
    assert _events(workspace, failing) == ["claimed", "request_applied", "released", "request_withdrawn"]
    found = _kernel.locate(workspace, failing, placement_hint=None, exhaustive=True)
    assert found is not None
    doc, _damage = read_state_unowned(found.path / "state.json")
    assert doc is None or not doc.applied_requests  # never applied


def test_a_failed_required_withdrawal_keeps_its_job_and_gives_nothing_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # G1: an unlink error in the B row's withdrawal is not "kept on purpose": the job stays held, the owner latches,
    # and nothing at all is given back.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "g", _JOBS)
    remove = _fs.remove_file
    requests = str(workspace.control / "requests")

    def removing(target: _fs.Loc, **kwargs: object) -> None:
        if str(target.path).startswith(f"{requests}/{refs[2].job_id}."):
            raise OSError(errno.EIO, "Input/output error")
        remove(target, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_fs, "remove_file", removing)
    _at_commit("B", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop) and owner.latched
    assert (owner.path / "owner.json").is_file()
    assert len(_owned(workspace, owner)) == _JOBS  # nothing given back
    assert outcomes[2].explanation.startswith("the withdrawal of its request failed")
    assert all(outcome.status == "uncertain" for outcome in outcomes)
    left = {name.split(".")[0] for name in _requests_left(workspace)}
    assert refs[2].job_id in left  # still armed, its job held


def test_owner_loss_during_a_withdrawal_stops_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # G4: OwnerLost on the first of two withdrawals: the owner latches, the second request is untouched, and the
    # first job, claimed for its withdrawal, is not given back.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "h", 2)
    posted = [(ref, _kernel.post_request(workspace, _requests_compose(ref))) for ref in refs]
    kept = removal._kept_for_recovery
    calls: list[str] = []

    def losing(handle: _kernel.OwnedJob, path: Path, request_id: str, **kwargs: object) -> str | None:
        calls.append(request_id)
        if len(calls) == 1:
            raise _kernel.OwnerLost("recovered meanwhile")
        return kept(handle, path, request_id, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "_kept_for_recovery", losing)
    owner = v3.cli_owner(workspace)
    progress = removal._Progress(_kernel.LocationIndex(workspace))
    with pytest.raises(_kernel.OwnerLost):
        removal._withdraw(workspace, lambda: owner, posted, progress, stopped=True)
    assert owner.latched and len(calls) == 1
    assert all(path.exists() for _ref, path in posted)  # nothing unlinked
    assert _where(workspace, refs[0].job_id) == "owned/p500"  # claimed for its withdrawal, never given back
    assert _where(workspace, refs[1].job_id) == "ready/p500"  # never claimed


def _requests_compose(ref: _kernel.JobRef) -> dict[str, object]:
    from httk.workflow import _requests

    return dict(
        _requests.compose(action="cancel", job_id=ref.job_id, placement=ref.placement or "", operator="cli", reason="w")
    )


def test_an_owner_lost_stops_the_chunk_and_gives_nothing_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "m", _JOBS)
    monkeypatch.setattr(
        _kernel.OwnedJob, "stage_state", lambda self, *args, **kwargs: _fails_from(1, _kernel.OwnerLost("gone"))()
    )
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, _kernel.OwnerLost)
    assert len(_owned(workspace, owner)) == _JOBS and (owner.path / "owner.json").is_file()
    assert outcomes == []


# -- interrupts (amendment 12) -----------------------------------------------------------------------------------------


def _interrupting(monkeypatch: pytest.MonkeyPatch, cls: object, name: str, at: int, *, after: bool = False) -> None:
    """Raise ``KeyboardInterrupt`` from the *at*-th call of ``cls.name``, before (or after) it runs."""

    original = getattr(cls, name)
    calls = {"n": 0}

    def wrapper(*args: object, **kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] == at and not after:
            raise KeyboardInterrupt
        result = original(*args, **kwargs)
        if calls["n"] == at:
            raise KeyboardInterrupt
        return result

    monkeypatch.setattr(cls, name, wrapper)


@pytest.mark.parametrize(
    ("phase", "after", "cancelled"),
    [
        ("B", False, 0),
        ("C1", False, 0),
        ("C2", False, 2),
        ("C2", True, 3),
        ("D1", False, _JOBS),
        ("D2", False, _JOBS),
        ("D2", True, _JOBS),
    ],
)
def test_an_interrupt_finishes_the_applied_jobs_and_gives_back_the_others(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    after: bool,
    cancelled: int,
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "n", _JOBS)
    if phase == "B":
        _interrupting(monkeypatch, _kernel, "claim", 3)
    else:
        name = {"C1": "stage_state", "C2": "replace_state", "D1": "unlink_requests", "D2": "move_out"}[phase]
        _interrupting(monkeypatch, _kernel.OwnedJob, name, 3, after=after)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt)
    where = [_where(workspace, ref.job_id) for ref in refs]
    assert where.count("cancelled/p500") == cancelled and where.count("ready/p500") == _JOBS - cancelled
    assert where[:cancelled] == ["cancelled/p500"] * cancelled  # the applied ones are the first, in input order
    assert [outcome.status for outcome in outcomes] == ["applied"] * cancelled
    # The unapplied ones' requests are withdrawn while held, also when C1 logged their application (E2): it is the
    # batch's own, and their state was never replaced; a request_withdrawn line follows it.
    assert _requests_left(workspace) == []
    for ref in refs[cancelled:]:
        events = _events(workspace, ref.job_id)
        assert not events or events[-1] == "request_withdrawn"
    assert _temporaries(workspace) == []
    assert not owner.path.exists()  # every job decided: the owner closed on the interrupt


def test_an_interrupt_after_a_failed_commit_point_follows_the_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The interrupt arrives while C2's commit point fails: deferred, delivered after it, and the C2 row applies.
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "o", _JOBS)

    def interrupted_and_failing() -> None:
        signal.raise_signal(signal.SIGINT)
        _eio()

    _at_commit("C2", "before", interrupted_and_failing)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt)
    assert [outcome.status for outcome in outcomes] == ["applied"] * _JOBS
    assert [_where(workspace, ref.job_id) for ref in refs] == ["cancelled/p500"] * _JOBS
    assert not owner.path.exists()


def test_a_second_interrupt_during_the_cleanup_is_recorded_not_acted_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "p", _JOBS)
    _interrupting(monkeypatch, _kernel.OwnedJob, "stage_state", 3)
    give_back = _kernel.OwnedJob.give_back
    interrupts = {"n": 0}

    def interrupted(self: _kernel.OwnedJob) -> _kernel.JobRef:
        if not interrupts["n"]:
            interrupts["n"] += 1
            signal.raise_signal(signal.SIGINT)  # a second Ctrl-C, while the cleanup gives the jobs back
        return give_back(self)

    monkeypatch.setattr(_kernel.OwnedJob, "give_back", interrupted)
    _outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt) and interrupts["n"] == 1
    assert [_where(workspace, ref.job_id) for ref in refs] == ["ready/p500"] * _JOBS  # the cleanup ran to its end
    assert not owner.path.exists()


# -- E1: a failed B withdraws while the jobs are held ------------------------------------------------------------------


def test_a_failed_b_withdraws_durably_before_the_first_give_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "w", _JOBS)
    journal = record(monkeypatch)
    requests = str(workspace.control / "requests")
    give_back = _kernel.OwnedJob.give_back
    checked: list[bool] = []

    def giving_back(self: _kernel.OwnedJob) -> _kernel.JobRef:
        if not checked:
            # At the first give-back every request is unlinked and requests/ fsynced after the last unlink.
            unlinks = [at for at, (op, path) in enumerate(journal) if op == "unlink" and path.startswith(requests)]
            assert len(unlinks) == _JOBS and not _requests_left(workspace)
            assert journal.index(("fsync", requests), unlinks[-1]) > unlinks[-1]
            checked.append(True)
        return give_back(self)

    monkeypatch.setattr(_kernel.OwnedJob, "give_back", giving_back)
    _at_commit("B", "before", _eio)
    journal.clear()
    outcomes, owner, raised = _serve(workspace, refs)
    assert checked and isinstance(raised, removal._CommitStopped)
    assert [outcome.status for outcome in outcomes] == ["uncertain"] * _JOBS
    # The B row seals the claims by directory first: the claims' directories are fsynced before any request unlink.
    first_unlink = next(at for at, (op, path) in enumerate(journal) if op == "unlink" and path.startswith(requests))
    sealed = {path for op, path in journal[:first_unlink] if op == "fsync"}
    assert str(workspace.jobs / "owned" / owner.owner_id) in sealed
    assert {str(Path(ref.path).parent) for ref in refs} <= sealed


def test_a_competing_claimant_after_a_failed_b_finds_no_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A manager that claims each job the moment it is given back must find nothing to apply (Codex 1).
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "x", _JOBS)
    give_back = _kernel.OwnedJob.give_back
    applied: list[removal.RequestOutcome] = []

    def giving_back(self: _kernel.OwnedJob) -> _kernel.JobRef:
        ref = give_back(self)

        def compete() -> None:
            with v3.cli_owner(workspace) as competitor:
                removal.serve(workspace, competitor, ref, outcomes=applied)

        contextvars.Context().run(compete)  # another process: none of this command's serving state
        return ref

    monkeypatch.setattr(_kernel.OwnedJob, "give_back", giving_back)
    _at_commit("B", "before", _eio)
    _outcomes, _owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._CommitStopped)
    assert applied == [] and [_where(workspace, ref.job_id) for ref in refs] == ["ready/p500"] * _JOBS


# -- E3, E4: owner loss and unresolved finishes fail-stop --------------------------------------------------------------


def test_an_owner_lost_in_an_evicted_jobs_serial_finish_stops_the_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "y", _JOBS)
    (refs[1].path / "logs").write_text("planted")  # evicted after B, served serially

    def lost(*args: object, **kwargs: object) -> bool:
        raise _kernel.OwnerLost("recovered meanwhile")

    monkeypatch.setattr(removal, "_serve_claimed", lost)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, _kernel.OwnerLost) and owner.latched
    assert outcomes == [] and (owner.path / "owner.json").is_file()
    # Nothing more was done: no job got a C1 line, every job stays held.
    assert len(_owned(workspace, owner)) == _JOBS
    assert all(
        not (path / "logs" / "runlog.jsonl").exists() for path in (workspace.jobs / "owned" / owner.owner_id).iterdir()
    )


def _failing_once(monkeypatch: pytest.MonkeyPatch, owner: object, name: str, when: Callable[..., bool]) -> None:
    original = getattr(owner, name)
    failed: list[bool] = []

    def wrapper(*args: object) -> object:
        if not failed and when(*args):
            failed.append(True)
            raise _fs.SyncFailed(errno.EIO, "cannot sync to disk: Input/output error")
        return original(*args)

    monkeypatch.setattr(owner, name, wrapper)


def test_an_evicted_finish_whose_fsync_fails_after_its_move_is_unresolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "z", _JOBS)
    (refs[1].path / "logs").write_text("planted")  # evicted: its serial release is the first durable move
    _failing_once(monkeypatch, _fs, "_fsync_parents", lambda src, dst: True)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop)
    by_job = {outcome.job_id: outcome for outcome in outcomes}
    assert by_job[refs[1].job_id].explanation.startswith("moved, durability unresolved")
    assert _where(workspace, refs[1].job_id) == "cancelled/p500"
    assert any(handle.job_id == refs[1].job_id for handle in owner._held.values())  # still registered
    assert owner.latched and (owner.path / "owner.json").is_file()
    # Fail-stop before any further work: the others stay held, reported uncertain, never given back.
    others = [ref for ref in refs if ref is not refs[1]]
    assert all(by_job[ref.job_id].status == "uncertain" for ref in others)
    assert len(_owned(workspace, owner)) == _JOBS - 1


@pytest.fixture
def late_device(monkeypatch: pytest.MonkeyPatch) -> Callable[[Path], int]:
    """Make every path below a directory report another ``st_dev``: a filesystem the chunk's collector finds late."""

    lstat, stat_, fstat = os.lstat, os.stat, os.fstat
    prefixes: list[str] = []

    def moved(status: os.stat_result, path: str) -> os.stat_result:
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in prefixes):
            return status
        fields = list(status[:10])
        fields[2] = status.st_dev ^ 0x5A5A
        return os.stat_result(fields)

    def lstating(path: object, *, dir_fd: int | None = None) -> os.stat_result:
        return moved(lstat(path, dir_fd=dir_fd), where(path, dir_fd))  # type: ignore[arg-type]

    def stating(path: object, *, dir_fd: int | None = None, follow_symlinks: bool = True) -> os.stat_result:
        status = stat_(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)  # type: ignore[arg-type]
        return status if isinstance(path, int) else moved(status, where(path, dir_fd))

    def fstating(descriptor: int) -> os.stat_result:
        return moved(fstat(descriptor), os.readlink(f"/proc/self/fd/{descriptor}"))

    monkeypatch.setattr(os, "lstat", lstating)
    monkeypatch.setattr(os, "stat", stating)
    monkeypatch.setattr(os, "fstat", fstating)

    def add(directory: Path) -> int:
        prefixes.append(str(directory))
        return lstat(directory.parent).st_dev ^ 0x5A5A

    return add


def test_a_late_device_gets_inline_fsyncs_and_the_others_one_syncfs_each(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late_device: Callable[[Path], int]
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "d", _JOBS)
    late = workspace.jobs / "cancelled" / "project"
    assert not late.exists()  # created by the first release move: found late, never primed
    late_device(late)
    journal = record(monkeypatch)
    outcomes, owner, raised = _serve(workspace, refs)
    assert raised is None and [outcome.status for outcome in outcomes] == ["applied"] * _JOBS
    syncfs = journal.positions("syncfs")
    assert len(syncfs) == 8  # the prime and seven commit points, on the one device primed
    d1, d2 = syncfs[-2], syncfs[-1]
    inline = [path for op, path in journal[d1 + 1 : d2] if op == "fsync"]
    # The late filesystem: each created placement's parent, then each destination after its rename, inline.
    assert inline and all(path == str(late) or path.startswith(f"{late}/") for path in inline)
    for ref in refs:
        rename = next(
            at
            for at, (op, path) in enumerate(journal)
            if op == "rename" and ref.job_id in path and "/cancelled/" in path
        )
        destination = str(Path(journal[rename][1].split(" -> ")[1]).parent)
        assert d1 < rename < journal.index(("fsync", destination), rename) < d2
    assert not owner.path.exists()


def test_a_d2_move_whose_late_fsync_fails_is_unresolved_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late_device: Callable[[Path], int]
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "v", _JOBS)
    late = workspace.jobs / "cancelled" / "project"
    late_device(late)
    # The inline fsync of the first job's destination, recorded after its rename (amendment 20).
    _failing_once(monkeypatch, _fs, "_seal_record", lambda target: str(target) == str(late / "v0"))
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop)
    assert outcomes[0].explanation.startswith("moved, durability unresolved")
    assert _where(workspace, refs[0].job_id) == "cancelled/p500"
    assert any(handle.job_id == refs[0].job_id for handle in owner._held.values())
    assert owner.latched and (owner.path / "owner.json").is_file()
    # Nothing after it was moved: the others stay held, reported uncertain.
    assert [_where(workspace, ref.job_id) for ref in refs[1:]] == ["owned/p500"] * (_JOBS - 1)
    assert all(outcome.status == "uncertain" for outcome in outcomes[1:])


# -- E6, E7: a failed commit point stops the command; a latched owner withdraws only what it holds ---------------------


def test_a_failed_commit_point_stops_the_command_and_withdraws_the_tail(tmp_path: Path) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first = _jobs(tmp_path, workspace, "f", 8)
    backlog = _jobs(tmp_path, workspace, "b", 1)[0]
    _requests_post(workspace, backlog)
    tail = _jobs(tmp_path, workspace, "t", 2)
    refs = [*first, backlog, *tail]
    _at_commit("B", "before", _eio)
    outcomes, _owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._CommitStopped)
    assert [outcome.status for outcome in outcomes] == ["uncertain"] * 8  # the batch's; nothing after it was served
    assert [_where(workspace, ref.job_id) for ref in [backlog, *tail]] == ["ready/p500"] * 3
    # The tail's requests were withdrawn; only the backlog's own older request (not this command's) is left.
    left = _requests_left(workspace)
    assert len(left) == 1 and left[0].startswith(backlog.job_id)


def _requests_post(workspace: Workspace, ref: _kernel.JobRef) -> None:
    from httk.workflow import _requests

    _requests.post(
        workspace,
        action="set_priority",
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="t",
        reason="older",
        priority=700,
    )


def test_a_latched_owner_withdraws_only_what_it_holds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The B row's first give-back moves its job but cannot fsync it: unresolved, fail-stop. The command's withdrawal
    # after that claims nothing (the chunk's unserved tail keeps its requests) and never touches the unresolved job
    # (Opus F2).
    workspace, _name = _new_workspace(tmp_path)
    first = _jobs(tmp_path, workspace, "u", 8)
    backlog = _jobs(tmp_path, workspace, "b", 1)[0]
    _requests_post(workspace, backlog)
    tail = _jobs(tmp_path, workspace, "t", 2)
    claims: list[str] = []
    claim = _kernel.claim

    def claiming(*args: object, **kwargs: object) -> object:
        if kwargs.get("commit") is None:
            claims.append(str(args[2]))
        return claim(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel, "claim", claiming)
    _failing_once(monkeypatch, _fs, "_fsync_parents", lambda src, dst: True)
    _at_commit("B", "before", _eio)
    outcomes, owner, raised = _serve(workspace, [*first, backlog, *tail])
    assert isinstance(raised, removal._FailStop) and owner.latched
    assert outcomes[0].explanation.startswith("moved, durability unresolved")
    assert claims == []
    assert _where(workspace, first[0].job_id) == "ready/p500"  # moved back, its move not durable
    assert len(_owned(workspace, owner)) == 7
    left = {name.split(".")[0] for name in _requests_left(workspace)}
    assert left == {backlog.job_id, *(ref.job_id for ref in tail)}  # nothing it does not hold is withdrawn


# -- E8, E9 ------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("how", ["signal", "raised"])
def test_an_interrupt_while_the_batch_ends_leaves_it_decided_and_unlatched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, how: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "e", _JOBS)
    prune = _kernel._prune_placement
    calls = {"n": 0}

    def pruning(*args: object, **kwargs: object) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            if how == "signal":
                signal.raise_signal(signal.SIGINT)
            else:
                raise KeyboardInterrupt
        prune(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel, "_prune_placement", pruning)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt)
    assert [outcome.status for outcome in outcomes] == ["applied"] * _JOBS
    assert not owner.latched and not owner.path.exists()


@pytest.mark.parametrize("failure", [None, "B"])
def test_a_damaged_claim_is_quarantined_once_b_is_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str | None
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "q", _JOBS)
    snapshot = removal._snapshot
    damaged: list[bool] = []

    def damaging(*args: object, **kwargs: object) -> object:
        if not damaged:
            damaged.append(True)  # after the post, before the claim
            (refs[2].path / "job.json").write_text("{")
        return snapshot(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "_snapshot", damaging)
    if failure is not None:
        _at_commit(failure, "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert outcomes[2].status == "uncertain" and outcomes[2].explanation.startswith("quarantined ")
    assert len(list((workspace.control / "quarantine").iterdir())) == 1
    assert _where(workspace, refs[2].job_id) is None and _owned(workspace, owner) == []
    assert (raised is None) == (failure is None)


# -- interrupts: the window of a cut replace's obligation ---------------------------------------------------------------


def test_an_interrupt_between_a_replaces_obligation_and_its_rename_seals_before_returning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "r", _JOBS)
    replace_staged = _fs.replace_staged
    replaces = {"n": 0}

    def cut(staged: _fs.Staged, **kwargs: object) -> None:
        if staged.target.name() == "state.json":
            replaces["n"] += 1
            if replaces["n"] == 3:
                raise KeyboardInterrupt  # registered as an obligation of the job directory, never renamed
        replace_staged(staged, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_fs, "replace_staged", cut)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt)
    assert [_where(workspace, ref.job_id) for ref in refs] == ["cancelled/p500"] * 2 + ["ready/p500"] * (_JOBS - 2)
    assert [outcome.status for outcome in outcomes] == ["applied"] * 2
    assert _requests_left(workspace) == [] and _temporaries(workspace) == []
    assert not owner.path.exists()


# -- the failure rows' call order ---------------------------------------------------------------------------------------


def test_the_d1_row_reseals_requests_before_any_move(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "s", _JOBS)
    journal = record(monkeypatch)
    failed: list[int] = []

    def failing() -> None:
        failed.append(len(journal))
        _eio()

    _at_commit("D1", "before", failing)
    _outcomes, _owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._CommitStopped)
    after = journal[failed[0] :]
    reseal = after.index(("fsync", str(workspace.control / "requests")))
    moves = [at for at, (op, path) in enumerate(after) if op == "rename" and "/cancelled/" in path]
    assert len(moves) == _JOBS and reseal < moves[0]
    assert not [entry for entry in after[:reseal] if entry[0] in ("rename", "unlink", "replace")]


# -- the crash matrix (amendment 24) -----------------------------------------------------------------------------------

#: Runs in a separate process the batch of the jobs named, and exits from the fault seam at one commit point without
#: unwinding anything: no handler, no owner exit, no temporary removed.
_CRASHING = """
import os, sys
from pathlib import Path
from httk.workflow import Workspace, _fs, _kernel, removal

root, point, moment, *job_ids = sys.argv[1:]
_fs._resolve_sync = lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4")
_fs._syncfs = lambda descriptor: None
seen = {"before": 0, "after": 0}

def fault(op, phase, src, dst):
    if op == "commit":
        seen[phase] += 1
        if phase == moment and seen[phase] == int(point):
            os._exit(17)

workspace = Workspace(Path(root))
refs = [_kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True) for job_id in job_ids]
owner = _kernel.register_owner(workspace, kind="cli", label="crash", allocation=None, advertised={})
print(owner.owner_id, flush=True)
_fs.set_fault_injector(fault)
with owner:
    removal.request_jobs(workspace, owner, refs, "cancel", "crash")
os._exit(0)
"""


@pytest.mark.parametrize("phase", ["before", "after"])
@pytest.mark.parametrize("point", _POINTS)
def test_a_crash_at_a_commit_point_recovers_to_what_the_serial_code_reaches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str, phase: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "c", _JOBS)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    child = subprocess.run(
        [sys.executable, "-c", _CRASHING, str(workspace.root), str(_POINTS.index(point) + 1), phase]
        + [ref.job_id for ref in refs],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert child.returncode == 17, child.stderr
    dead_id = child.stdout.split()[0]
    dead_dir = workspace.control / "owners" / dead_id
    assert (dead_dir / "owner.json").is_file() and (dead_dir / ".batched").is_file()
    # Recovery itself seals (amendment 27): it is recorded issuing the per-owner seal, with the placements of a
    # batched owner, before it returns any job.
    calls: list[str] = []
    seal, return_jobs = _fs.seal_directories, _kernel._return_jobs

    def sealing(directories: list[Path], *args: object, **kwargs: object) -> None:
        calls.append("seal")
        assert workspace.control / "requests" in directories
        assert workspace.jobs / "owned" / dead_id in directories
        assert workspace.jobs / "ready" in directories  # a batched owner: every placement directory too
        seal(directories, *args, **kwargs)  # type: ignore[arg-type]

    def returning(*args: object, **kwargs: object) -> None:
        calls.append("return")
        return_jobs(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_fs, "seal_directories", sealing)
    monkeypatch.setattr(_kernel, "_return_jobs", returning)
    _kernel.attest_dead(workspace, dead_id, by="operator", evidence=[], operator="t", reason="crash")
    with v3.cli_owner(workspace) as rescuer:
        _kernel.recover(workspace, rescuer, dead_id)
    assert calls[:2] == ["seal", "return"]
    owned = workspace.jobs / "owned" / dead_id
    assert not owned.exists() and not (dead_dir / "owner.json").exists()
    # A commit point's phase made its mutations before it: "before" and "after" differ only in durability, which a
    # process death keeps (the order is pinned by the journal tests). Recovery returns every held job unchanged.
    moved, posted, unlinked = point == "D2", point != "C0", point in ("D1", "D2")
    assert [_where(workspace, ref.job_id) for ref in refs] == ["cancelled/p500" if moved else "ready/p500"] * _JOBS
    assert len(_requests_left(workspace)) == (_JOBS if posted and not unlinked else 0)
    # The next claimant: today's serve of each job, which applies its request, or finishes the release recorded at C2;
    # then a second pass over every job, as a later manager's, which must find nothing to apply (G5).
    evaluated: list[str] = []
    apply = removal._requests.apply

    def applying(*args: object, **kwargs: object) -> object:
        new, effect = apply(*args, **kwargs)  # type: ignore[arg-type]
        if not isinstance(effect, removal._requests.Defer):
            evaluated.append(str(args[4].request_id))  # type: ignore[attr-defined]
        return new, effect

    monkeypatch.setattr(removal._requests, "apply", applying)
    with v3.cli_owner(workspace) as claimant:
        for ref in refs:
            found = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
            assert found is not None
            if found.state == "ready":
                assert removal.serve(workspace, claimant, found)
    successor = list(evaluated)
    with v3.cli_owner(workspace) as later:
        for ref in refs:
            found = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
            assert found is not None
            assert removal.serve(workspace, later, found)
    assert evaluated == successor  # nothing applied again, not even an applied leftover
    assert [_where(workspace, ref.job_id) for ref in refs] == ["cancelled/p500" if posted else "ready/p500"] * _JOBS
    # Applied exactly once: by the successor before C2 (the dead batch had replaced no state), never after.
    replaced = point in ("C2", "D1", "D2")
    assert len(successor) == (_JOBS if posted and not replaced else 0) and len(set(successor)) == len(successor)
    for ref in refs:
        found = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
        assert found is not None
        doc, _damage = read_state_unowned(found.path / "state.json")
        history = (
            [] if doc is None else [entry for entry in doc.history_tail if entry.get("event") == "request_applied"]
        )
        assert len(history) == (1 if posted else 0)  # one application in the job's own history
        # The run log: the dead batch's record from C1 on, and the successor's application before C2.
        logged = _events(workspace, ref.job_id).count("request_applied")
        assert logged == {"C0": 0, "A": 1, "B": 1, "C1": 2}.get(point, 1)
    # A request whose state replace was durable before its unlink (C2) was left as an applied leftover; the later
    # pass's listing deleted it on sight without applying it (and its id left applied_requests). None is left.
    assert _requests_left(workspace) == []
