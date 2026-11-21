"""The failure table, interrupts and the crash matrix of batched Discard chunks and of chunks that mix Drop and
Discard (plan WORKFLOW_LOCAL_COMMANDS phase L3c; amendments 9-12, 17-20, 24 and 28).

Up to and including D2 a Discard job is unapplied: withdrawn while held, then given back; it never goes on to D′. A
failed D′ reseals by directory and finishes the trash path, or fail-stops with the trash scratch and ``owner.json``
kept; a failed D″ reseals ``requests/`` and otherwise only warns. An interrupt finishes every trash-moved job through
D′ and D″ and gives back the others, withdrawn while held. A process killed before and after each commit point ends,
after dead-owner recovery and the next claimant, with every job deleted at most once and never half-deleted.
"""

import errno
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as v3
from httk.workflow import Workspace, _fs, _kernel, removal
from httk.workflow.workflow_cli import command
from test_batched_discard import _terminal
from test_batched_release import _jobs, _requests_left, _where
from test_batched_release_failures import _eio, _events, _interrupting, _owned, _temporaries
from test_cli_job_request import _context, _new_workspace
from test_local_commands import _identity  # noqa: F401  (_identity: an autouse fixture)

#: The commit points of a chunk of Discard jobs, and of a chunk mixing Drop and Discard jobs, in order.
_DISCARD = ("C0", "A", "B", "C1", "D′", "D″")
_MIXED = ("C0", "A", "B", "C1", "C2", "D1", "D2", "D′", "D″")
_JOBS = 9


@pytest.fixture(autouse=True)
def _syncfs_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The protocol predicate holds (host-independent; ``syncfs`` is not really called), and no fault is left set."""

    monkeypatch.setattr(_fs, "_resolve_sync", lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4"))
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: None)
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    yield
    _fs.set_fault_injector(None)


def _at_commit(points: tuple[str, ...], point: str, moment: str, action: Callable[[], None]) -> None:
    """Call *action* at the fault seam of *point*'s commit (``before`` or ``after`` it is durable)."""

    seen = {"before": 0, "after": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op != "commit":
            return
        seen[phase] += 1
        if phase == moment and seen[phase] == points.index(point) + 1:
            action()

    _fs.set_fault_injector(fault)


def _serve(
    workspace: Workspace, refs: list[_kernel.JobRef]
) -> tuple[list[removal.RequestOutcome], _kernel.Owner, BaseException | None]:
    """Request ``delete`` of *refs* as one command; the outcomes reported, the owner, and what it raised."""

    reported: list[removal.RequestOutcome] = []
    raised: BaseException | None = None
    owner = v3.cli_owner(workspace)
    try:
        with owner:
            removal.request_jobs(workspace, owner, refs, "delete", "batch", report=reported.extend)
    except (Exception, KeyboardInterrupt) as exc:
        raised = exc
    return reported, owner, raised


def _mixed(tmp_path: Path, workspace: Workspace) -> tuple[list[_kernel.JobRef], list[_kernel.JobRef]]:
    """Four ready jobs (a delete is a Drop) and five cancelled ones (a Discard), interleaved."""

    ready = _jobs(tmp_path, workspace, "r", 4)
    cancelled = _terminal(tmp_path, workspace, "c", 5)
    return [
        ready[0],
        cancelled[0],
        ready[1],
        cancelled[1],
        cancelled[2],
        ready[2],
        cancelled[3],
        ready[3],
        cancelled[4],
    ], cancelled


def _trash_spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The jobs a batch moved into the trash (a batched ``move_to_trash``)."""

    moved: list[str] = []
    move_to_trash = _kernel.OwnedJob.move_to_trash

    def recording(self: _kernel.OwnedJob, **kwargs: Any) -> object:
        if kwargs.get("commit") is not None:
            moved.append(self.job_id)
        return move_to_trash(self, **kwargs)

    monkeypatch.setattr(_kernel.OwnedJob, "move_to_trash", recording)
    return moved


def _trash_trees(workspace: Workspace, owner_id: str) -> list[str]:
    tmp = workspace.control / "tmp"
    return sorted(
        f"{scratch}/{name}"
        for scratch in (os.listdir(tmp) if tmp.is_dir() else [])
        if scratch.startswith(f"{owner_id}.trash.")
        for name in os.listdir(tmp / scratch)
    )


# -- the failure table (amendments 11, 17-20) --------------------------------------------------------------------------


@pytest.mark.parametrize("point", _DISCARD)
def test_a_failed_commit_point_of_a_delete_chunk_ends_each_job_as_its_row_says(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "f", _JOBS)
    trashed = _trash_spy(monkeypatch)
    _at_commit(_DISCARD, point, "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    _fs.set_fault_injector(None)
    statuses = [outcome.status for outcome in outcomes]
    where = [_where(workspace, ref.job_id) for ref in refs]
    assert _temporaries(workspace) == [] and _owned(workspace, owner) == [] and not owner.latched
    assert _requests_left(workspace) == []
    if point in ("C0", "A"):
        assert isinstance(raised, _fs.SyncFailed) and statuses == ["uncertain"] * _JOBS
        assert where == ["cancelled/p500"] * _JOBS and trashed == []
        return
    assert isinstance(raised, removal._CommitStopped) and "Input/output error" in str(raised)
    if point in ("B", "C1"):
        # Unapplied: withdrawn while held, then given back, never trashed.
        assert statuses == ["uncertain"] * _JOBS and where == ["cancelled/p500"] * _JOBS and trashed == []
        for ref in refs:
            events = _events(workspace, ref.job_id)
            if point == "C1":
                assert events[-3:] == ["claimed", "request_applied", "request_withdrawn"]
            else:
                assert "request_applied" not in events
    else:
        # D′: owned/<owner-id>/ and the trash resealed by directory, the trash path finished; D″: requests/ resealed.
        assert statuses == ["applied"] * _JOBS and where == [None] * _JOBS and len(trashed) == _JOBS
        assert _trash_trees(workspace, owner.owner_id) == []


@pytest.mark.parametrize("point", _MIXED)
def test_a_failed_commit_point_of_a_mixed_chunk_never_takes_a_discard_job_to_d_prime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs, cancelled = _mixed(tmp_path, workspace)
    discard = {ref.job_id for ref in cancelled}
    trashed = _trash_spy(monkeypatch)
    _at_commit(_MIXED, point, "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    _fs.set_fault_injector(None)
    assert _temporaries(workspace) == [] and _owned(workspace, owner) == [] and not owner.latched
    assert _requests_left(workspace) == []
    by_job = {outcome.job_id: outcome for outcome in outcomes}
    drops = [ref for ref in refs if ref.job_id not in discard]
    assert all(_where(workspace, ref.job_id) == "ready/p500" for ref in drops)
    if point in ("C0", "A"):
        assert isinstance(raised, _fs.SyncFailed) and trashed == []
        return
    assert isinstance(raised, removal._CommitStopped)
    if point in ("D′", "D″"):
        assert sorted(trashed) == sorted(discard) and all(by_job[job].status == "applied" for job in discard)
        assert all(_where(workspace, job) is None for job in discard)
        return
    # Up to and including D2 (amendment 19, mixed chunks): the Discard jobs are unapplied, withdrawn while held, then
    # given back; none goes on to D′.
    assert trashed == []
    for ref in cancelled:
        assert by_job[ref.job_id].status == "uncertain" and "Input/output error" in by_job[ref.job_id].explanation
        assert _where(workspace, ref.job_id) == "cancelled/p500"
        events = _events(workspace, ref.job_id)
        assert point == "B" or events[-2:] == ["request_applied", "request_withdrawn"]
    expected = "uncertain" if point in ("B", "C1") else "refused"
    assert all(by_job[ref.job_id].status == expected for ref in drops)


def test_a_failed_d_prime_reseal_fail_stops_with_the_trash_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "g", _JOBS)
    monkeypatch.setattr(_kernel.Owner, "seal_obligations", lambda self: _eio())
    _at_commit(_DISCARD, "D′", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop) and owner.latched
    assert [outcome.status for outcome in outcomes] == ["uncertain"] * _JOBS
    assert all(outcome.explanation.startswith("moved to trash, durability unresolved") for outcome in outcomes)
    # The trash scratch and owner.json stay; no tree was unlinked, no request touched.
    assert len(_trash_trees(workspace, owner.owner_id)) == _JOBS
    assert all(
        (workspace.control / "tmp" / tree / "job.json").is_file() for tree in _trash_trees(workspace, owner.owner_id)
    )
    assert (owner.path / "owner.json").is_file() and len(_requests_left(workspace)) == _JOBS
    assert all(_where(workspace, ref.job_id) is None for ref in refs)


def test_after_a_d_prime_fail_stop_recovery_disposes_of_the_trash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "h", _JOBS)
    seal = _kernel.Owner.seal_obligations
    failing = {"on": True}

    def sealing(self: _kernel.Owner) -> None:
        if failing["on"]:
            _eio()
        seal(self)

    monkeypatch.setattr(_kernel.Owner, "seal_obligations", sealing)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    _at_commit(_DISCARD, "D′", "before", _eio)
    _outcomes, owner, raised = _serve(workspace, refs)
    _fs.set_fault_injector(None)
    failing["on"] = False
    assert isinstance(raised, removal._FailStop)
    _kernel.attest_dead(workspace, owner.owner_id, by="operator", evidence=[], operator="t", reason="fail-stop")
    with v3.cli_owner(workspace) as rescuer:
        _kernel.recover(workspace, rescuer, owner.owner_id)
    # The trash is gone with the dead owner; each request left names no job (deleted once, never half-deleted).
    assert os.listdir(workspace.control / "tmp") == [] and not (owner.path / "owner.json").exists()
    assert all(_where(workspace, ref.job_id) is None for ref in refs)
    assert {name.split(".")[0] for name in _requests_left(workspace)} == {ref.job_id for ref in refs}


def test_a_failed_d_double_prime_reseal_fail_stops_and_recovery_seals_requests_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # J1: the request unlinks of deleted jobs stay obligations; the owner fail-stops with owner.json kept, and
    # recovery's per-owner seal makes requests/ durable before it removes anything. A job restored afterwards under
    # one of the deleted ids is then not deleted by any request.
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "i", _JOBS)
    restored = tmp_path / "restored"
    shutil.copytree(refs[4].path, restored)  # a copy of the job, restored under its id afterwards
    seal = _kernel.Owner.seal_obligations
    failing = {"on": True}

    def sealing(self: _kernel.Owner) -> None:
        if failing["on"]:
            _eio()
        seal(self)

    monkeypatch.setattr(_kernel.Owner, "seal_obligations", sealing)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    _at_commit(_DISCARD, "D″", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    _fs.set_fault_injector(None)
    failing["on"] = False
    assert isinstance(raised, removal._FailStop) and owner.latched and (owner.path / "owner.json").is_file()
    assert [outcome.status for outcome in outcomes] == ["uncertain"] * _JOBS
    assert all(
        outcome.explanation.startswith("job deleted; its request unlink is not yet durable") for outcome in outcomes
    )
    requests = workspace.control / "requests"
    assert {item.directory for item in owner._obligations.values()} == {requests}  # kept, never forgone
    assert all(_where(workspace, ref.job_id) is None for ref in refs) and _requests_left(workspace) == []
    calls: list[str] = []
    seal_directories = _fs.seal_directories

    def recording(directories: list[Path], *args: object, **kwargs: object) -> None:
        if requests in directories and (owner.path / "owner.json").is_file():
            calls.append("seal requests/")
        seal_directories(directories, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_fs, "seal_directories", recording)
    _kernel.attest_dead(workspace, owner.owner_id, by="operator", evidence=[], operator="t", reason="fail-stop")
    with v3.cli_owner(workspace) as rescuer:
        _kernel.recover(workspace, rescuer, owner.owner_id)
    assert calls[:1] == ["seal requests/"] and not (owner.path / "owner.json").exists()
    # The restored job: no request names it, and the next claimant applies nothing to it.
    destination = refs[4].path.parent / refs[4].path.name
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(restored, destination)
    with v3.cli_owner(workspace) as claimant:
        found = v3.find(workspace, refs[4].job_id)
        assert removal.serve(workspace, claimant, found)
    assert _where(workspace, refs[4].job_id) == "cancelled/p500"


def test_a_tree_that_cannot_be_removed_is_left_for_gc_and_the_owner_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # J3: a persistent permission error. The job is deleted (D′ was sealed); its tree leaves the owner's trash for a
    # tmp/trash.<token> entry, which workspace gc removes once aged, and the owner's close does not meet it again.
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "t", _JOBS)
    locked = refs[3].path / "locked"
    locked.mkdir()
    (locked / "kept").write_text("x")
    locked.chmod(0o500)
    try:
        report = removal.remove_jobs(workspace, refs)
        outcome = report.outcomes[3]
        assert outcome.removed and outcome.reason is not None
        assert outcome.reason.startswith("job deleted; its tree is left for workspace gc: [Errno 13]")
        assert all(item.removed and item.reason is None for item in report.outcomes[:3] + report.outcomes[4:])
        assert not list((workspace.control / "owners").iterdir())  # the owner closed
        tmp = workspace.control / "tmp"
        (left,) = os.listdir(tmp)
        assert left.startswith("trash.") and len(left) == len("trash.") + 16
        assert all(_where(workspace, ref.job_id) is None for ref in refs) and _requests_left(workspace) == []
    finally:
        for path in workspace.root.rglob("locked"):
            path.chmod(0o700)


def test_a_trash_move_whose_rename_happened_but_not_its_fsync_fail_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "k", _JOBS)
    move_owned = _fs.move_owned
    tmp = f"{workspace.control / 'tmp'}/"
    trash_moves = {"n": 0}

    def moving(source: _fs.Loc, target: _fs.Loc, **kwargs: Any) -> object:
        result = move_owned(source, target, **kwargs)
        if str(target.path).startswith(tmp) and kwargs.get("commit") is not None:
            trash_moves["n"] += 1
            if trash_moves["n"] == 3:
                raise _fs.SyncFailed(errno.EIO, "cannot sync to disk: Input/output error")
        return result

    monkeypatch.setattr(_fs, "move_owned", moving)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, removal._FailStop) and owner.latched
    assert outcomes[2].explanation.startswith("moved to trash, durability unresolved: [Errno 5] cannot sync")
    assert any(handle.job_id == refs[2].job_id for handle in owner._held.values())  # still registered
    assert all(
        outcome.explanation == "moved to trash, durability unresolved: the batch stopped" for outcome in outcomes[:2]
    )
    assert all("stays held for recovery" in outcome.explanation for outcome in outcomes[3:])
    assert (owner.path / "owner.json").is_file() and len(_trash_trees(workspace, owner.owner_id)) == 3
    assert len(_owned(workspace, owner)) == _JOBS - 3 and len(_requests_left(workspace)) == _JOBS


def test_a_trash_move_that_fails_before_its_rename_withdraws_while_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "l", _JOBS)
    failing = refs[2].job_id
    trash_tree = _kernel.Owner.trash_tree

    def trashing(self: _kernel.Owner, path: Any, **kwargs: Any) -> object:
        if failing in str(path):
            raise OSError(errno.ENOSPC, "No space left on device")
        return trash_tree(self, path, **kwargs)

    monkeypatch.setattr(_kernel.Owner, "trash_tree", trashing)
    outcomes, owner, raised = _serve(workspace, refs)
    # As at HEAD: the job stays held, so the owner cannot close.
    assert isinstance(raised, _kernel.WorkflowError) and "still holds 1 jobs" in str(raised)
    assert [outcome.status for outcome in outcomes] == ["applied"] * 2 + ["uncertain"] + ["applied"] * (_JOBS - 3)
    assert len(_owned(workspace, owner)) == 1 and _requests_left(workspace) == []
    assert _events(workspace, failing)[-2:] == ["request_applied", "request_withdrawn"]


# -- interrupts (amendments 12, 28) ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "at", "after", "deleted"),
    [
        ("append_log_line", 5, False, 0),  # C1: every job unapplied
        ("move_to_trash", 3, False, 2),  # D′: the third job not yet moved
        ("move_to_trash", 3, True, 3),  # D′: the third job moved
        ("retire_discarded", 2, False, _JOBS),  # D″: every job trash-moved
        ("unlink_requests", 4, True, _JOBS),
    ],
)
def test_an_interrupt_finishes_the_trash_moved_jobs_and_gives_back_the_others(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, at: int, after: bool, deleted: int
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "n", _JOBS)
    _interrupting(monkeypatch, _kernel.OwnedJob, name, at, after=after)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt)
    where = [_where(workspace, ref.job_id) for ref in refs]
    assert where == [None] * deleted + ["cancelled/p500"] * (_JOBS - deleted)
    assert [outcome.status for outcome in outcomes] == ["applied"] * deleted
    assert _requests_left(workspace) == [] and _temporaries(workspace) == []
    for ref in refs[deleted:]:
        events = _events(workspace, ref.job_id)
        assert "request_applied" not in events or events[-1] == "request_withdrawn"
    assert not owner.path.exists() and os.listdir(workspace.control / "tmp") == []


def test_an_interrupt_of_a_mixed_chunk_before_d_prime_gives_the_discard_jobs_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs, cancelled = _mixed(tmp_path, workspace)
    trashed = _trash_spy(monkeypatch)
    _interrupting(monkeypatch, _kernel.OwnedJob, "move_out", 2)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt) and trashed == []
    discard = {ref.job_id for ref in cancelled}
    assert [outcome.status for outcome in outcomes] == ["refused"] * 4  # the Drop jobs, finished through D2
    for ref in refs:
        assert _where(workspace, ref.job_id) == ("cancelled/p500" if ref.job_id in discard else "ready/p500")
    for ref in cancelled:
        assert _events(workspace, ref.job_id)[-1] == "request_withdrawn"
    assert _requests_left(workspace) == [] and not owner.path.exists()


def test_an_interrupt_whose_d_prime_seal_fails_fail_stops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "p", _JOBS)
    _interrupting(monkeypatch, _kernel.OwnedJob, "move_to_trash", 3, after=True)
    monkeypatch.setattr(_kernel.Owner, "seal_obligations", lambda self: _eio())
    _at_commit(_DISCARD, "D′", "before", _eio)
    outcomes, owner, raised = _serve(workspace, refs)
    assert isinstance(raised, KeyboardInterrupt) and owner.latched and (owner.path / "owner.json").is_file()
    assert [outcome.explanation.split(":")[0] for outcome in outcomes[:3]] == [
        "moved to trash, durability unresolved"
    ] * 3
    assert len(_trash_trees(workspace, owner.owner_id)) == 3 and len(_owned(workspace, owner)) == _JOBS - 3
    assert len(_requests_left(workspace)) == _JOBS  # nothing withdrawn or unlinked: fail-stop


# -- the crash matrix (amendment 24) -----------------------------------------------------------------------------------

#: Runs in a separate process the delete of the jobs named, and exits from the fault seam at one commit point without
#: unwinding anything: no handler, no owner exit, no temporary removed.
_CRASHING = """
import os, sys
from pathlib import Path
from httk.workflow import Workspace, _fs, _kernel, removal
from httk.workflow.workflow_cli import command

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
    removal.request_jobs(workspace, owner, refs, "delete", "crash")
os._exit(0)
"""


@pytest.mark.parametrize("moment", ["before", "after"])
@pytest.mark.parametrize(
    ("mixed", "point"),
    [(False, point) for point in ("A", "B", "C1", "D′", "D″")] + [(True, point) for point in _MIXED[1:]],
)
def test_a_crash_at_a_commit_point_recovers_with_each_job_deleted_at_most_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mixed: bool, point: str, moment: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    if mixed:
        refs, cancelled = _mixed(tmp_path, workspace)
    else:
        refs = cancelled = _terminal(tmp_path, workspace, "c", _JOBS)
    discard = {ref.job_id for ref in cancelled}
    drops = [ref for ref in refs if ref.job_id not in discard]
    trees = {ref.job_id: {str(path.relative_to(ref.path)) for path in ref.path.rglob("*")} for ref in refs}
    points = _MIXED if mixed else _DISCARD
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    child = subprocess.run(
        [sys.executable, "-c", _CRASHING, str(workspace.root), str(points.index(point) + 1), moment]
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
    trashed_by_dead = len(_trash_trees(workspace, dead_id))
    _kernel.attest_dead(workspace, dead_id, by="operator", evidence=[], operator="t", reason="crash")
    with v3.cli_owner(workspace) as rescuer:
        _kernel.recover(workspace, rescuer, dead_id)
    # The state recovery reaches: the trash disposed of; a job before D′ back with its request; after D′, a deleted
    # job whose request is left naming no job (before D″) or gone (D″ on).
    assert os.listdir(workspace.control / "tmp") == []
    assert not (workspace.jobs / "owned" / dead_id).exists() and not (dead_dir / "owner.json").exists()
    trashed = point in ("D′", "D″")
    # The dead batch's trash held each moved tree whole until D′ was sealed; its tree unlinks come only after.
    assert trashed_by_dead == (len(discard) if point == "D′" else 0)
    for job_id in discard:
        assert (_where(workspace, job_id) is None) == trashed
    requests = {name.split(".")[0] for name in _requests_left(workspace)}
    discard_left = requests & discard
    assert discard_left == (discard if point == "D′" else set() if point == "D″" else discard)
    # The next claimant: today's serve of each job still there, which deletes a Discard job (once) and drops a
    # Drop job's delete; then a later pass must find nothing to apply.
    deleted: list[str] = []
    discard_call = _kernel.OwnedJob.discard

    def discarding(self: _kernel.OwnedJob) -> None:
        deleted.append(self.job_id)
        discard_call(self)

    monkeypatch.setattr(_kernel.OwnedJob, "discard", discarding)
    with v3.cli_owner(workspace) as claimant:
        for ref in refs:
            found = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
            if found is not None:
                # Never half-deleted: a job still reachable is whole.
                assert {str(path.relative_to(found.path)) for path in found.path.rglob("*")} >= trees[ref.job_id]
                assert removal.serve(workspace, claimant, found)
    assert sorted(deleted) == ([] if trashed else sorted(discard))
    assert all(_where(workspace, job_id) is None for job_id in discard)
    assert all(_where(workspace, ref.job_id) == "ready/p500" for ref in drops)
    # What is left: only requests that name no job (after D′, before D″'s unlinks were made).
    left = {name.split(".")[0] for name in _requests_left(workspace)}
    assert left == (discard if point == "D′" else set())
    assert all(_kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True) is None for job_id in left)


def test_a_failed_hand_off_to_gc_keeps_the_tree_in_the_trash_and_loses_no_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    # J3b: the tree cannot be removed and its hand-off to tmp/ keeps failing (MoveFailed). The job is still reported
    # deleted, with the residue; every handle is retired and every request unlinked. The owner's close then meets the
    # tree again and raises, keeping owner.json: the owner stays recoverable.
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    workspace, name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "t", _JOBS)
    locked = refs[3].path / "locked"
    locked.mkdir()
    (locked / "kept").write_text("x")
    locked.chmod(0o500)
    tmp = workspace.control / "tmp"
    trash = _fs.trash
    failing = {"on": True}

    def handing_off(target: _fs.Loc, *, trash_dir: Path, **kwargs: Any) -> _fs.Loc:
        if failing["on"] and trash_dir == tmp:
            raise _fs.MoveFailed(f"could not move {target.path}: it kept coming back", errno.EBUSY)
        return trash(target, trash_dir=trash_dir, **kwargs)

    monkeypatch.setattr(_fs, "trash", handing_off)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    try:
        lines = _job_delete(workspace, name, refs, capsys, tmp_path)
        assert lines[:3] + lines[4:] == [f"{ref.job_key}\tcancelled\tremoved" for ref in refs[:3] + refs[4:]]
        assert lines[3].startswith(
            f"{refs[3].job_key}\tcancelled\tremoved\tjob deleted; its tree stays in this owner's trash for close or "
            "recovery: [Errno 13]"
        )
        assert all(_where(workspace, ref.job_id) is None for ref in refs) and _requests_left(workspace) == []
        (owner_id,) = os.listdir(workspace.control / "owners")
        assert (workspace.control / "owners" / owner_id / "owner.json").is_file()  # close raised on the tree
        assert not (workspace.jobs / "owned" / owner_id).exists()  # every handle retired, nothing held
        assert len(_trash_trees(workspace, owner_id)) == 1  # the residue, the owner's
    finally:
        for path in workspace.root.rglob("locked"):
            path.chmod(0o700)
    # Recoverable: once the tree can go, recovery disposes of the trash and removes the owner.
    failing["on"] = False
    _kernel.attest_dead(workspace, owner_id, by="operator", evidence=[], operator="t", reason="close failed")
    with v3.cli_owner(workspace) as rescuer:
        _kernel.recover(workspace, rescuer, owner_id)
    assert os.listdir(tmp) == [] and not (workspace.control / "owners" / owner_id / "owner.json").exists()


# -- the CLI reports what it knows before a failure propagates (J2) ----------------------------------------------------


def _job_delete(workspace: Workspace, name: str, refs: list[_kernel.JobRef], capsys: Any, root: Path) -> list[str]:
    capsys.readouterr()
    status = command(["job", "delete", "--yes", "--workspace", name, *(ref.job_id for ref in refs)], _context(root))
    assert status != 0
    return capsys.readouterr().out.splitlines()


def test_job_delete_prints_the_unresolved_jobs_of_a_d_prime_fail_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    workspace, name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "u", _JOBS)
    monkeypatch.setattr(_kernel.Owner, "seal_obligations", lambda self: _eio())
    _at_commit(_DISCARD, "D′", "before", _eio)
    lines = _job_delete(workspace, name, refs, capsys, tmp_path)
    assert len(lines) == _JOBS
    for ref, line in zip(refs, lines, strict=True):
        assert line.startswith(f"{ref.job_key}\tcancelled\tuncertain\tmoved to trash, durability unresolved")


def test_job_delete_prints_the_earlier_chunks_before_a_commit_failure_stops_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    monkeypatch.setattr(removal, "_CHUNK_JOBS", _JOBS)
    workspace, name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "e", 2 * _JOBS)
    # The second chunk's B: the first chunk made C0, A, B, C1, D′ and D″.
    seen = {"n": 0}

    def fault(op: str, phase: str, src: object, dst: object) -> None:
        if op == "commit" and phase == "before":
            seen["n"] += 1
            if seen["n"] == len(_DISCARD) + _DISCARD.index("B") + 1:
                _eio()

    _fs.set_fault_injector(fault)
    lines = _job_delete(workspace, name, refs, capsys, tmp_path)
    # The first chunk's deletions, then the stopped chunk's outcomes, in input order; no summary line.
    assert lines[:_JOBS] == [f"{ref.job_key}\tcancelled\tremoved" for ref in refs[:_JOBS]]
    assert [line.split("\t")[:3] for line in lines[_JOBS:]] == [
        [ref.job_key, "cancelled", "uncertain"] for ref in refs[_JOBS:]
    ]
    assert all("Input/output error" in line for line in lines[_JOBS:])
