"""L3a correction round 1 (plan WORKFLOW_LOCAL_COMMANDS amendment 27 and the L3a reviews).

The per-owner recovery seal and its batched marker, seal errors as failed recoveries, ancestor fallback only for
absence, retained descriptors by directory identity, no-follow seal targets, the primitives' ownership guards,
descriptor hygiene, non-durable workspaces, and the single committer. Assertions are on call order (amendment 24).
"""

import errno
import json
import os
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import _fs, _kernel
from httk.workflow._kernel import OWNED, JobRef, Owner, Release, claim, register_owner, submit
from httk.workflow._state import StateDoc
from httk.workflow.errors import WorkflowError
from kernel._journal import Journal, record


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


def _workspace(root: Path, durable: bool = True) -> FakeWorkspace:
    workspace = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", durable, 0.0)
    (workspace.control / "requests").mkdir(parents=True)
    return workspace


@pytest.fixture
def ws(tmp_path: Path) -> FakeWorkspace:
    return _workspace(tmp_path / "ws")


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    yield
    _fs.set_fault_injector(None)


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> Journal:
    return record(monkeypatch)


@pytest.fixture
def fsync_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Protocol commits and seals by fsync, host-independently (the test doubles' io.sync is auto)."""

    monkeypatch.setattr(_fs, "protocol_mode", lambda path, setting: "fsync")


def _owner(ws: FakeWorkspace) -> Owner:
    return register_owner(ws, kind="cli", label=None, allocation=None, advertised={})


def _job(ws: FakeWorkspace, owner: Owner, placement: str = "p/q") -> JobRef:
    staging = owner.scratch("build") / "job"
    staging.mkdir()
    document = {"format": "httk-workflow-job", "format_version": 3, "id": str(uuid.uuid4()), "tag": "t"}
    (staging / "job.json").write_text(json.dumps(document | {"placement": placement, "priority": 500}))
    ref = submit(ws, owner, staging)
    _fs.remove_empty_dir(_fs.loc(staging.parent))
    return ref


def _request(ws: FakeWorkspace, job_id: str) -> tuple[str, Path]:
    request_id = str(uuid.uuid4())
    path = ws.control / "requests" / f"{job_id}.{request_id}.json"
    path.write_text("{}")
    return request_id, path


def _dead(ws: FakeWorkspace, owner: Owner) -> None:
    _kernel.attest_dead(ws, owner.owner_id, by="operator", evidence=[], operator="t", reason="t")


def _first_rename_out_of(events: list[tuple[str, str]], directory: Path) -> int:
    return next(i for i, (op, where) in enumerate(events) if op == "rename" and where.startswith(f"{directory}/"))


# -- R1: the recovery seal per amendment 27 ----------------------------------------------------------------------------


def test_a_c2_interrupted_job_is_sealed_before_its_successor_cleans_its_applied_request(
    ws: FakeWorkspace, journal: Journal, fsync_mode: None
) -> None:
    dead = _owner(ws)
    ref = _job(ws, dead)
    request_id, request = _request(ws, ref.job_id)
    with _fs.Commit([ws.jobs, ws.control], "fsync") as commit:
        handle = claim(ws, dead, ref, commit=commit)
        assert handle is not None
        dead.commit(commit)  # B
        doc = StateDoc.empty(handle.job_id).with_release(Release("paused", 500, (request_id,)))
        staged = handle.stage_state(doc, commit=commit)
        dead.commit(commit)  # C1
        handle.replace_state(staged, commit=commit)  # C2's mutation; the owner dies before its commit point
    assert (dead.path / ".batched").is_file()  # written before the first batched mutation
    _dead(ws, dead)
    recoverer = _owner(ws)
    mark = len(journal)
    report = _kernel.recover(ws, recoverer, dead.owner_id)
    successor = claim(ws, recoverer, report.returned[0])
    assert successor is not None
    assert successor.request_files() == []  # the applied leftover is deleted on sight
    events = journal[mark:]
    sealed = events.index(("fsync", str(handle.path)))
    returned = _first_rename_out_of(events, ws.jobs / OWNED / dead.owner_id)
    unlinked = events.index(("unlink", str(request)))
    assert sealed < returned < unlinked
    assert not request.exists()


def _placements(ws: FakeWorkspace, count: int) -> list[Path]:
    paths = [ws.jobs / "ready" / f"p{index}" for index in range(count)]
    for path in paths:
        path.mkdir(parents=True)
    return paths


def test_a_serial_dead_owner_pays_for_what_it_held_not_for_the_placements(
    ws: FakeWorkspace, journal: Journal, fsync_mode: None
) -> None:
    dead = _owner(ws)
    held = claim(ws, dead, _job(ws, dead))
    assert held is not None
    placements = _placements(ws, 300)
    _dead(ws, dead)
    recoverer = _owner(ws)
    mark = len(journal)
    _kernel.recover(ws, recoverer, dead.owner_id)
    events = journal[mark:]
    seal = [where for op, where in events[: _first_rename_out_of(events, held.path.parent)] if op == "fsync"]
    assert seal[:4] == [str(ws.control / "requests"), str(held.path.parent), str(held.path), str(ws.control / "tmp")]
    # Then only the return's own creation of its placement (as at HEAD): jobs/ready gains p/, p/ gains q/.
    assert seal[4:] == [str(ws.jobs / "ready"), str(ws.jobs / "ready" / "p")]
    assert not {str(path) for path in placements} & {where for op, where in events if op == "fsync"}
    assert sum(op == "fsync" for op, _ in events) < 20


def test_a_batched_dead_owner_also_seals_every_surviving_placement(
    ws: FakeWorkspace, journal: Journal, fsync_mode: None
) -> None:
    dead = _owner(ws)
    held = claim(ws, dead, _job(ws, dead))
    assert held is not None
    dead.mark_batched()
    placements = _placements(ws, 300)
    _dead(ws, dead)
    recoverer = _owner(ws)
    mark = len(journal)
    _kernel.recover(ws, recoverer, dead.owner_id)
    events = journal[mark:]
    seal = {where for op, where in events[: _first_rename_out_of(events, held.path.parent)] if op == "fsync"}
    assert {str(path) for path in placements} <= seal
    assert {str(ws.jobs / "ready"), str(held.path)} <= seal


def test_the_marker_is_written_once_and_removed_at_close(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    first, second = _directories(ws.control, "first", "second")
    with owner.obliged([first], kind="owner"):
        pass
    written = [entry for entry in journal if entry[0] == "replace" and entry[1].endswith("/.batched")]
    with owner.obliged([second], kind="owner"):
        pass
    assert written == [entry for entry in journal if entry[0] == "replace" and entry[1].endswith("/.batched")]
    assert len(written) == 1
    owner.close()
    assert not owner.path.exists()


def test_close_removes_a_marker_whose_directory_fsync_failed(
    ws: FakeWorkspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _owner(ws)
    fsync = os.fsync

    def failing(descriptor: int) -> None:
        if os.readlink(f"/proc/self/fd/{descriptor}") == str(owner.path):
            raise OSError(errno.EIO, "Input/output error")
        fsync(descriptor)

    monkeypatch.setattr(os, "fsync", failing)
    with pytest.raises(OSError):
        owner.mark_batched()  # replaced onto .batched, then its directory's fsync failed
    monkeypatch.setattr(os, "fsync", fsync)
    assert (owner.path / ".batched").is_file() and not owner._batched
    owner.close()
    assert not owner.path.exists()


def test_recovery_removes_the_marker_with_the_owner_record(ws: FakeWorkspace, fsync_mode: None) -> None:
    dead = _owner(ws)
    dead.mark_batched()
    _dead(ws, dead)
    _kernel.recover(ws, _owner(ws), dead.owner_id)
    assert sorted(path.name for path in dead.path.iterdir()) == ["dead.json"]


def _directories(root: Path, *names: str) -> list[Path]:
    paths = [root / name for name in names]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
    return paths


# -- R2 and R3: seal errors fail the recovery; only absence falls back to an ancestor -------------------------------


def _lstat_failing(monkeypatch: pytest.MonkeyPatch, path: Path, error: int) -> None:
    lstat = os.lstat

    def failing(target: object, *args: object, **kwargs: object) -> os.stat_result:
        if str(target) == str(path):
            raise OSError(error, os.strerror(error), str(path))
        return lstat(target, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "lstat", failing)


def test_only_absence_falls_back_to_the_nearest_ancestor(
    tmp_path: Path, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    (a,) = _directories(tmp_path, "a")
    gone = a / "gone"
    assert _fs.seal_directory(gone) == a  # ENOENT
    _lstat_failing(monkeypatch, gone, errno.ESTALE)
    assert _fs.seal_directory(gone) == a
    for error in (errno.EACCES, errno.EIO):
        _lstat_failing(monkeypatch, a, error)
        mark = len(journal)
        with pytest.raises(_fs.SyncFailed):
            _fs.seal_directory(a)
        with pytest.raises(_fs.SyncFailed):
            _fs.seal_directories([a], "fsync")
        assert journal[mark:] == []  # never the parent instead


def test_an_inaccessible_held_job_fails_the_recovery_which_exposes_nothing(
    ws: FakeWorkspace, journal: Journal, fsync_mode: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead = _owner(ws)
    held = claim(ws, dead, _job(ws, dead))
    assert held is not None
    _dead(ws, dead)
    _lstat_failing(monkeypatch, held.path, errno.EACCES)
    mark = len(journal)
    with pytest.raises(_fs.SyncFailed):
        _kernel.recover(ws, _owner(ws), dead.owner_id)
    assert not [entry for entry in journal[mark:] if entry[0] in ("rename", "unlink", "rmdir")]
    assert held.path.is_dir() and (dead.path / "owner.json").is_file()
    assert dead.owner_id in _kernel.recoverable_owners(ws)


def test_a_failed_syncfs_open_is_a_seal_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (a,) = _directories(tmp_path, "a")
    monkeypatch.setattr(_fs, "protocol_mode", lambda path, setting: "syncfs")
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: None)
    opener = os.open

    def failing(path: object, flags: int, *args: object, **kwargs: object) -> int:
        if str(path) == str(a) and flags & os.O_DIRECTORY:
            raise OSError(errno.EIO, "I/O error", str(a))
        return opener(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "open", failing)
    with pytest.raises(_fs.SyncFailed):
        _fs.seal_directories([a], "auto")


# -- R4: retained descriptors by directory identity --------------------------------------------------------------


def test_a_directory_replaced_at_its_path_is_sealed_by_each_identity(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    (directory,) = _directories(ws.jobs / "ready", "x")
    with owner.obliged([directory], kind="owner"):
        pass
    directory.rmdir()  # pruned, then recreated by another actor's move
    directory.mkdir()
    with owner.obliged([directory], kind="owner"):
        pass
    owner._sealed_by([directory], owner._mark())  # a serial fsync by path seals only the directory there now
    assert len(owner._obligations) == 1
    with owner.obliged([directory], kind="owner"):
        pass
    mark = len(journal)
    owner.seal_obligations()
    assert sorted(where for op, where in journal[mark:] if op == "fsync") == [
        str(directory),
        f"{directory} (deleted)",
    ]
    assert not owner._obligations and not owner._retained


# -- R5: no-follow seal targets ----------------------------------------------------------------------------------------


def test_a_symlinked_directory_is_refused_and_its_target_sealed_on_its_own(tmp_path: Path, journal: Journal) -> None:
    target, other = _directories(tmp_path, "target", "other")
    link = other / "link"
    link.symlink_to(target)
    with pytest.raises(_fs.UnsafePath):
        _fs.seal_directory(link)
    with _fs.Commit([tmp_path], "fsync") as commit:
        with pytest.raises(_fs.UnsafePath):
            commit.directory(link)
        commit.directory(target)
        mark = len(journal)
        commit.commit()
    assert journal[mark:] == [("fsync", str(target))]


# -- R6: the primitives' ownership guards ------------------------------------------------------------------------------


def test_request_primitives_refuse_a_job_that_is_gone(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    _request_id, request = _request(ws, handle.job_id)
    handle.path.rename(ws.root / "recovered")  # a recoverer took it; its requests are the successor's
    with pytest.raises(_kernel.OwnerLost):
        handle.unlink_requests([request.name])
    with _fs.Commit([ws.control], "fsync") as commit, pytest.raises(_kernel.OwnerLost):
        handle.unlink_requests([request.name], commit=commit)
    assert request.exists()
    with pytest.raises(_kernel.OwnerLost):
        handle.already_applied(str(uuid.uuid4()))


def test_only_a_discarded_handle_unlinks_requests_without_its_job(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    released = claim(ws, owner, _job(ws, owner, "a/b"))
    assert released is not None
    released.release(StateDoc.empty(released.job_id), Release("paused", 500))
    _request_id, request = _request(ws, released.job_id)
    with pytest.raises(_kernel.ReleasedJobError):
        released.unlink_requests([request.name])  # released, not discarded
    discarded = claim(ws, owner, _job(ws, owner, "c/d"))
    assert discarded is not None
    with pytest.raises(WorkflowError, match="not discarded"):
        discarded.retire_discarded()
    _fs.remove_trashed(discarded.move_to_trash())
    discarded.retire_discarded()
    _other, mine = _request(ws, discarded.job_id)
    discarded.unlink_requests([mine.name])
    assert not mine.exists() and request.exists()


def test_move_out_requires_a_quiescent_job(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    handle.begin_attempt(str(uuid.uuid4()))
    with pytest.raises(WorkflowError, match="still live"):
        handle.move_out("ready", PurePosixPath("p/q"), 500)
    with _fs.Commit([ws.jobs], "fsync") as commit, pytest.raises(WorkflowError, match="still live"):
        handle.move_out("ready", PurePosixPath("p/q"), 500, commit=commit)
    assert handle.path.is_dir() and owner.holds(handle.ref)


def test_extract_refuses_a_pinned_handle(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    with _fs.Commit([handle.path], "fsync") as commit:
        handle.replace_state(handle.stage_state(StateDoc.empty(handle.job_id), commit=commit), commit=commit)
    scratch = owner.scratch("bundle")
    with pytest.raises(WorkflowError, match="stays held"):
        handle.extract(scratch / handle.path.name)
    assert handle.path.is_dir() and owner.holds(handle.ref)


# -- R7: descriptor hygiene -----------------------------------------------------------------------------------------


def _open_descriptors() -> int:
    return len(os.listdir("/proc/self/fd"))


def test_a_failed_snapshot_closes_the_descriptors_it_duplicated(
    ws: FakeWorkspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _owner(ws)
    for directory in _directories(ws.control, "one", "two"):
        with owner.obliged([directory], kind="owner"):
            pass
    before = _open_descriptors()
    dup, calls = os.dup, []

    def failing(descriptor: int) -> int:
        calls.append(descriptor)
        if len(calls) > 1:
            raise OSError(errno.EMFILE, "too many open files")
        return dup(descriptor)

    monkeypatch.setattr(os, "dup", failing)
    with pytest.raises(OSError, match="too many"):
        owner.seal_obligations()
    assert len(calls) == 2 and _open_descriptors() == before
    monkeypatch.setattr(os, "dup", dup)
    owner.seal_obligations()
    assert not owner._obligations


def test_a_batched_trash_move_refuses_an_anchored_location(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    (directory,) = _directories(ws.control, "anchor")
    (directory / "tree").mkdir()
    descriptor = _fs.open_dir(directory)
    try:
        with _fs.Commit([ws.control], "fsync") as commit, pytest.raises(ValueError, match="anchored"):
            owner.trash_tree(_fs.anchored(descriptor, "tree"), commit=commit)
    finally:
        os.close(descriptor)
    assert (directory / "tree").is_dir()


def test_one_file_recorded_three_times_is_one_record_and_one_fsync(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    with _fs.Commit([handle.path], "fsync", workers=1) as commit:
        for event in ("one", "two", "three"):
            handle.append_log_line(event, {}, commit=commit)
        assert len(commit._files) == 1  # one duplicate descriptor of the run log
        mark = len(journal)
        commit.commit()
    runlog = str(handle.path / "logs" / "runlog.jsonl")
    assert [entry for entry in journal[mark:] if entry == ("fsync", runlog)] == [("fsync", runlog)]


def test_concurrent_records_of_one_file_keep_one_duplicate(ws: FakeWorkspace) -> None:
    (directory,) = _directories(ws.control, "files")
    path = directory / "f"
    path.write_bytes(b"f")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        with _fs.Commit([directory], "fsync") as commit:
            for _ in range(20):
                start = threading.Barrier(4)

                def record(barrier: threading.Barrier) -> None:
                    barrier.wait()
                    commit.file(descriptor)

                threads = [threading.Thread(target=record, args=(start,)) for _ in range(4)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                assert len(commit._files) == 1
    finally:
        os.close(descriptor)


# -- R8: a workspace that is not durable -------------------------------------------------------------------------------


def test_a_non_durable_workspace_registers_and_syncs_nothing(tmp_path: Path, journal: Journal) -> None:
    ws = _workspace(tmp_path / "ws", durable=False)
    owner = _owner(ws)
    ref = _job(ws, owner)
    request_id, request = _request(ws, ref.job_id)
    mark = len(journal)
    with _fs.Commit([ws.jobs, ws.control], "auto", durable=False) as commit:
        handle = claim(ws, owner, ref, commit=commit)
        assert handle is not None
        handle.append_log_line("claimed", {}, commit=commit)
        staged = handle.stage_state(StateDoc.empty(handle.job_id).with_release(Release("paused", 500)), commit=commit)
        handle.replace_state(staged, commit=commit)
        handle.unlink_requests([request.name], commit=commit)
        handle.move_out("paused", PurePosixPath("p/q"), 500, commit=commit)
        owner.commit(commit)
        assert not commit.barriers and not owner._obligations and not owner.latched
    owner.seal_obligations()
    owner.close()
    assert not [entry for entry in journal[mark:] if entry[0] in ("fsync", "syncfs")]
    assert not (owner.path / ".batched").exists() and not request.exists() and request_id


# -- R9: one committer; the serial step's mark ----------------------------------------------------------------------


def test_a_serial_step_clears_only_obligations_that_finished_before_its_mark(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    (directory,) = _directories(ws.control, "d")
    with owner.obliged([directory], kind="owner"):
        pass  # finished before the mark: cleared
    mark = owner._mark()
    with owner.obliged([directory], kind="owner"):
        owner._sealed_by([directory], mark)  # this one has not finished: kept
        assert len(owner._obligations) == 1
    owner._sealed_by([directory], mark)  # finished after the mark: kept
    assert len(owner._obligations) == 1
    owner._sealed_by([directory], owner._mark())
    assert not owner._obligations


def test_one_commit_runs_at_a_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (a,) = _directories(tmp_path, "a")
    inside, overlapped = threading.Lock(), []

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "commit" and phase == "before":
            if not inside.acquire(blocking=False):
                overlapped.append(True)
                return
            threading.Event().wait(0.05)
            inside.release()

    with _fs.Commit([a], "fsync") as commit:
        commit.directory(a)
        _fs.set_fault_injector(fault)
        threads = [threading.Thread(target=commit.commit) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    assert not overlapped
