"""The batched-commit infrastructure of plan WORKFLOW_LOCAL_COMMANDS phase L3a, unused by every command path.

The protocol predicate (amendment 23), the commit collector (§4.5, §4.6, amendments 24 and 26), owner obligations
with the fail-stop latch (§4.10, amendments 10, 16 and 26) and the recovery-side seal (amendment 15). Every test
records the order of renames, unlinks, ``rmdir``, ``fsync`` and ``syncfs`` calls, not end states: a killed process
keeps its page cache, so a missing fsync passes an end-state test (amendment 24).
"""

import errno
import json
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _fs, _kernel
from httk.workflow._kernel import OWNED, JobRef, Owner, claim, format_job_name, register_owner, submit
from httk.workflow._state import StateDoc
from httk.workflow.errors import WorkflowError
from kernel._journal import Journal, record, where


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a durable workspace (no settings: ``io.sync`` is ``auto``)."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@pytest.fixture
def ws(tmp_path: Path) -> FakeWorkspace:
    root = tmp_path / "ws"
    return FakeWorkspace(root, root / ".httk-workspace", root / "jobs", True, 0.0)


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    # A prime that finds syncfs unsupported records it by st_dev: never let a test leak that to the next one.
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    yield
    _fs.set_fault_injector(None)


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> Journal:
    return record(monkeypatch)


@pytest.fixture
def syncfs_mode(monkeypatch: pytest.MonkeyPatch, journal: Journal) -> None:
    """Make every filesystem resolve as a vetted one (ext4 on Linux 6.1), host-independently; ``syncfs`` is the
    journal's recorder."""

    monkeypatch.setattr(_fs, "_resolve_sync", lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4"))


def _directories(root: Path, *names: str) -> list[Path]:
    paths = [root / name for name in names]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
    return paths


# -- the protocol predicate (amendment 23) ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("setting", "resolved", "kind", "expected"),
    [
        ("auto", "syncfs", "ext4", "syncfs"),
        ("syncfs", "syncfs", "xfs", "syncfs"),
        ("auto", "syncfs", "tmpfs", "syncfs"),
        ("auto", "syncfs", "zfs", "syncfs"),  # the ZFS gate passed
        ("auto", "fsync", "zfs", "fsync"),  # the ZFS gate failed
        ("auto", "syncfs", "lustre", "fsync"),
        ("syncfs", "syncfs", "lustre", "fsync"),  # forced on Lustre
        ("syncfs", "fsync", "nfs4", "fsync"),  # forced on a type auto does not vet
        ("auto", "fsync", None, "fsync"),  # unknown type, old kernel, no libc syncfs, syncfs refused
        ("fsync", "syncfs", "ext4", "fsync"),
        ("bogus", "syncfs", "ext4", "fsync"),
    ],
)
def test_the_protocol_predicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, setting: str, resolved: str, kind: str | None, expected: str
) -> None:
    asked: list[object] = []

    def resolve(path: Path, value: object) -> tuple[str, str, str | None]:
        asked.append(value)
        return resolved, resolved, kind

    monkeypatch.setattr(_fs, "_resolve_sync", resolve)
    assert _fs.protocol_mode(tmp_path, setting) == expected
    # Always resolved as auto would be: a forced setting never decides the protocol mode.
    assert asked in ([], ["auto"])


@pytest.mark.parametrize("setting", ["auto", "syncfs"])
def test_lustre_gives_fsync_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, journal: Journal, setting: str
) -> None:
    monkeypatch.setattr(_fs, "_filesystem_type", lambda path, device: "lustre")
    monkeypatch.setattr(_fs, "_uname", lambda: ("Linux", "5.14.0-362.el9.x86_64"))
    monkeypatch.setattr(_fs, "_syncfs_function", lambda: lambda descriptor: 0)
    assert _fs.resolve_sync(tmp_path, "auto")[0] == "syncfs"  # what a landing's barrier does there
    with _fs.Commit([tmp_path], setting) as commit:
        assert commit.mode == "fsync"
        commit.directory(tmp_path)
        commit.commit()
    assert journal == [("fsync", str(tmp_path))]  # no prime, no syncfs


# -- the commit collector (§4.5, §4.6, amendments 23, 24, 26) --------------------------------------------------------


def _write(commit: _fs.Commit, path: Path, data: bytes) -> None:
    # A file written in a phase: staged and replaced, its descriptor and directory recorded.
    _fs.replace_staged(_fs.stage_file(_fs.loc(path), data, durable=False, commit=commit), durable=False, commit=commit)


def test_syncfs_mode_primes_at_entry_and_commits_one_syncfs_per_device(
    tmp_path: Path, journal: Journal, syncfs_mode: None
) -> None:
    a, b = _directories(tmp_path, "a", "b")
    with _fs.Commit([a, b], "auto") as commit:
        assert commit.mode == "syncfs"
        assert [barrier.errors for barrier in commit.barriers.values()] == ["fail"]
        primed = list(journal)
        _write(commit, a / "x", b"x")
        _fs.move_owned(_fs.loc(a / "x"), _fs.loc(b / "x"), durable=False, commit=commit)
        before = len(journal)
        commit.commit()
    assert primed == [("syncfs", str(a))]  # one barrier for the one filesystem, primed before any write
    assert not [entry for entry in journal[len(primed) : before] if entry[0] in ("fsync", "syncfs")]
    assert journal[before:] == [("syncfs", str(a))]  # nothing fsynced one by one


def test_fsync_mode_fsyncs_files_then_each_distinct_directory_once(tmp_path: Path, journal: Journal) -> None:
    a, b = _directories(tmp_path, "a", "b")
    with _fs.Commit([a, b], "fsync", workers=1) as commit:
        assert commit.mode == "fsync"
        _write(commit, a / "x", b"x")
        _write(commit, b / "y", b"y")
        _fs.move_owned(_fs.loc(a / "x"), _fs.loc(b / "x"), durable=False, commit=commit)
        commit.directory(a)
        commit.directory(_fs.open_dir(b))  # a descriptor of a recorded path: the same directory
        writes = len(journal)
        commit.commit()
    assert not [entry for entry in journal[:writes] if entry[0] in ("fsync", "syncfs")]
    committed = journal[writes:]
    # The two writing descriptors (their temporaries were renamed since, x on to b), then a and b once each.
    assert [op for op, _ in committed] == ["fsync"] * 4
    assert [Path(path) for _, path in committed[:2]] == [b / "x", b / "y"]
    assert committed[2:] == [("fsync", str(a)), ("fsync", str(b))]


def test_fsync_mode_pools_and_dedups_shared_directories(tmp_path: Path, journal: Journal) -> None:
    directories = _directories(tmp_path, *(f"d{i}" for i in range(6)))
    with _fs.Commit(directories, "fsync", workers=4) as commit:
        for directory in directories:
            for name in ("x", "y", "z"):
                _write(commit, directory / name, b"data")
            commit.directory(directory)
        writes = len(journal)
        commit.commit()
    committed = journal[writes:]
    files, dirs = committed[:18], committed[18:]
    assert sorted(Path(path).parent.name for _, path in files) == sorted(f"d{i}" for i in range(6) for _ in "xyz")
    assert sorted(dirs) == sorted(("fsync", str(directory)) for directory in directories)  # each exactly once


def test_a_file_recorded_by_path_is_fsynced_through_anchored_no_follow_opens(tmp_path: Path, journal: Journal) -> None:
    (a,) = _directories(tmp_path, "a")
    (a / "f").write_bytes(b"f")
    (a / "link").symlink_to(a / "f")
    with _fs.Commit([a], "fsync") as commit:
        commit.file(a / "f")
        commit.commit()
        assert journal[-1] == ("fsync", str(a / "f"))
        with pytest.raises(_fs.UnsafePath):
            commit.file(a / "link")  # never followed


def test_a_late_device_gets_inline_fsync_and_no_prime(
    tmp_path: Path, journal: Journal, syncfs_mode: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, late = _directories(tmp_path, "a", "late")
    with _fs.Commit([a], "auto") as commit:
        identify = commit._identify

        def faked(target: int | Path, *, directory: bool) -> os.stat_result:
            # Everything below late/ is on a filesystem the collector did not open at entry.
            status = identify(target, directory=directory)
            if not isinstance(target, int) and Path(target).is_relative_to(late):
                return os.stat_result((status.st_mode, status.st_ino, -1, *tuple(status)[3:10]))
            return status

        monkeypatch.setattr(commit, "_identify", faked)
        primed = len(journal)
        (late / "f").write_bytes(b"f")
        commit.file(late / "f")
        commit.directory(late)
        assert journal[primed:] == [("fsync", str(late / "f")), ("fsync", str(late))]  # inline, at once
        commit.commit()
    assert journal[primed + 2 :] == [("syncfs", str(a))]  # the late device is never primed nor synced
    assert journal[:primed] == [("syncfs", str(a))]


def test_an_unsupported_prime_switches_that_device_to_fsync_before_any_write(
    tmp_path: Path, journal: Journal, syncfs_mode: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    (a,) = _directories(tmp_path, "a")

    def unsupported(descriptor: int) -> None:
        journal.append(("syncfs", where(descriptor, None)))
        raise OSError(errno.ENOSYS, "no syncfs")

    monkeypatch.setattr(_fs, "_syncfs", unsupported)
    with _fs.Commit([a], "auto") as commit:
        assert commit.mode == "fsync"
        _write(commit, a / "x", b"x")
        writes = len(journal)
        commit.commit()
    assert journal[0] == ("syncfs", str(a))  # the prime, which failed
    assert [op for op, _ in journal[writes:]] == ["fsync", "fsync"]  # the file and a, no syncfs


def test_a_failed_syncfs_fails_the_commit_whatever_io_sync_errors_says(
    tmp_path: Path, journal: Journal, syncfs_mode: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    (a,) = _directories(tmp_path, "a")
    calls: list[int] = []

    def failing(descriptor: int) -> None:
        calls.append(descriptor)
        if len(calls) > 1:
            raise OSError(errno.EIO, "writeback error")

    monkeypatch.setattr(_fs, "_syncfs", failing)
    # A workspace configured with io.sync_errors=ignore makes no difference: the collector takes no such setting,
    # and each of its barriers fails.
    with _fs.Commit([a], "auto") as commit:
        assert all(barrier.errors == "fail" for barrier in commit.barriers.values())
        with pytest.raises(_fs.SyncFailed):
            commit.commit()
    assert len(calls) == 2  # the prime, and the commit (never a second, trusting syncfs)


@pytest.mark.parametrize("when", ["before", "after"])
def test_the_fault_seam_fails_a_commit_and_clears_nothing(tmp_path: Path, journal: Journal, when: str) -> None:
    (a,) = _directories(tmp_path, "a")

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "commit" and phase == when:
            journal.append(("fault", phase))
            raise OSError(errno.EIO, "injected")

    with _fs.Commit([a], "fsync") as commit:
        commit.directory(a)
        _fs.set_fault_injector(fault)
        with pytest.raises(OSError, match="injected"):
            commit.commit()
        _fs.set_fault_injector(None)
        before = len(journal)
        commit.commit()  # the record is still there
    expected = [("fault", "before")] if when == "before" else [("fsync", str(a)), ("fault", "after")]
    assert journal[:before] == expected
    assert journal[before:] == [("fsync", str(a))]


def test_a_commit_clears_only_what_was_recorded_before_it_started(
    tmp_path: Path, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, b = _directories(tmp_path, "a", "b")
    with _fs.Commit([a, b], "fsync") as commit:
        fsync = os.fsync
        late: list[None] = []

        def recording_during(descriptor: int) -> None:
            fsync(descriptor)
            if not late:
                late.append(None)
                commit.directory(b)  # a pooled worker's step finishing while the commit runs

        commit.directory(a)
        monkeypatch.setattr(os, "fsync", recording_during)
        commit.commit()
        monkeypatch.setattr(os, "fsync", fsync)
        before = len(journal)
        commit.commit()
        assert journal[before:] == [("fsync", str(b))]  # kept for the next commit, and only it
        before = len(journal)
        commit.commit()
        assert journal[before:] == []


def test_a_commit_outside_its_context_or_on_a_missing_directory_is_refused(tmp_path: Path) -> None:
    (a,) = _directories(tmp_path, "a")
    commit = _fs.Commit([a], "fsync")
    with pytest.raises(ValueError, match="context"):
        commit.directory(a)
    with pytest.raises(ValueError, match="context"):
        commit.commit()
    with pytest.raises(FileNotFoundError), _fs.Commit([a, tmp_path / "missing"], "fsync"):
        pass


# -- owner obligations and the fail-stop latch (§4.10, amendments 10, 16, 26) ---------------------------------------


def _owner(ws: FakeWorkspace) -> Owner:
    return register_owner(ws, kind="cli", label=None, allocation=None, advertised={})


def _job(ws: FakeWorkspace, owner: Owner, placement: str = "p/q") -> JobRef:
    staging = owner.scratch("build") / "job"
    staging.mkdir()
    job_id = str(uuid.uuid4())
    document = {"format": "httk-workflow-job", "format_version": 3, "id": job_id, "tag": "t"}
    (staging / "job.json").write_text(json.dumps(document | {"placement": placement, "priority": 500}))
    ref = submit(ws, owner, staging)
    _fs.remove_empty_dir(_fs.loc(staging.parent))
    return ref


def _pinned_handle(ws: FakeWorkspace, owner: Owner, ref: JobRef) -> _kernel.OwnedJob:
    """Claim *ref* and replace its state in a batch phase whose commit point has not run: an exposure obligation."""

    handle = claim(ws, owner, ref)
    assert handle is not None
    with _fs.Commit([handle.path], "fsync") as commit:
        doc = StateDoc.empty(handle.job_id).next_activation("start", "initial")
        handle.replace_state(handle.stage_state(doc, commit=commit), commit=commit)
    return handle


@pytest.mark.parametrize("raised", [KeyboardInterrupt, SystemExit, RuntimeError])
def test_an_exit_with_a_pinned_handle_fail_stops_and_gives_nothing_back(
    ws: FakeWorkspace, journal: Journal, raised: type[BaseException]
) -> None:
    owner = _owner(ws)
    ref = _job(ws, owner)
    handle, mark = None, 0
    with pytest.raises(raised), owner:
        handle = _pinned_handle(ws, owner, ref)
        assert owner.pinned(handle) and owner.latched
        with pytest.raises(WorkflowError, match="stays held"):
            handle.give_back()
        with pytest.raises(WorkflowError, match="stays held"):
            handle._move_out("ready", handle.placement(), 500)
        mark = len(journal)
        raise raised()
    assert handle is not None
    assert handle.path.is_dir() and owner.holds(handle.ref)  # nothing given back
    assert (owner.path / "owner.json").is_file()  # owner.json stays
    assert not owner.pinned(handle)  # sealed first: the job directory fsynced, nothing moved
    assert journal[mark:] == [("fsync", str(handle.path))]


def test_a_failed_seal_at_exit_fail_stops(ws: FakeWorkspace, journal: Journal, monkeypatch: pytest.MonkeyPatch) -> None:
    owner = _owner(ws)
    ref = _job(ws, owner)
    handle = None
    with pytest.raises(KeyboardInterrupt), owner:
        handle = _pinned_handle(ws, owner, ref)
        monkeypatch.setattr(os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError(errno.EIO, "dead disk")))
        raise KeyboardInterrupt
    assert handle is not None
    assert owner.pinned(handle) and handle.path.is_dir() and (owner.path / "owner.json").is_file()


def test_an_interrupt_with_only_owner_obligations_seals_then_gives_back(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    ref = _job(ws, owner)
    mark = 0
    with pytest.raises(KeyboardInterrupt), owner:
        with _fs.Commit([ws.jobs], "fsync") as commit:
            handle = claim(ws, owner, ref, commit=commit)  # owner-gating obligations, no latch
        assert handle is not None and not owner.latched
        mark = len(journal)
        raise KeyboardInterrupt
    sealed = next(i for i in journal.positions("fsync", ref.path.parent) if i >= mark)
    assert mark <= sealed < journal.positions("rename")[-1]  # the claim's directories sealed, then given back
    assert not (owner.path / "owner.json").exists()  # and closed


def test_close_seals_first(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    ref = _job(ws, owner)
    # A job in owned/<owner-id>/ this process does not hold, for close's self-healing.
    owned = ws.jobs / OWNED / owner.owner_id / format_job_name(ref.job_key, 500, _fs.fresh_token(), "ready")
    assert _fs.move_once(_fs.loc(ref.path), _fs.loc(owned), durable=True) is _fs.Moved.WON
    shared = ws.control / "shared"
    shared.mkdir()
    with owner.obliged([shared], kind="owner"):
        (shared / "entry").write_bytes(b"")
    mark = len(journal)
    owner.close()
    events = journal[mark:]
    assert events[0] == ("fsync", str(shared))  # before anything else
    assert events.index(("fsync", str(shared))) < next(i for i, (op, _) in enumerate(events) if op == "rename")
    assert events.index(("fsync", str(shared))) < events.index(("rmdir", str(owned.parent)))
    assert not (owner.path / "owner.json").exists()


def test_a_failed_seal_in_close_keeps_owner_json(
    ws: FakeWorkspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _owner(ws)
    shared = ws.control / "shared"
    shared.mkdir()
    with owner.obliged([shared], kind="owner"):
        pass
    fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError(errno.EIO, "dead disk")))
    with pytest.raises(OSError, match="dead disk"):
        owner.close()
    assert (owner.path / "owner.json").is_file()
    monkeypatch.setattr(os, "fsync", fsync)
    owner.close()
    assert not owner.path.exists()


def test_a_seal_by_path_of_a_gone_directory_fsyncs_the_nearest_existing_ancestor(
    ws: FakeWorkspace, journal: Journal
) -> None:
    owner = _owner(ws)
    deep = ws.jobs / "ready" / "a" / "b"
    deep.mkdir(parents=True)
    job_dir = ws.jobs / "ready" / "a" / "b" / "job"
    job_dir.mkdir()

    class Handle:
        path = job_dir  # a job-private directory: sealed by path, not through a retained descriptor

    with owner.obliged([job_dir], kind="exposure", job=Handle()):  # type: ignore[arg-type]
        pass
    job_dir.rmdir()
    deep.rmdir()
    (ws.jobs / "ready" / "a").rmdir()
    mark = len(journal)
    owner.seal_obligations()
    assert journal[mark:] == [("fsync", str(ws.jobs / "ready"))]
    assert _fs.seal_directory(ws.root / "x" / "y") == ws.root


def test_a_seal_clears_only_obligations_whose_mutation_finished_before_it_started(
    ws: FakeWorkspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _owner(ws)
    first, second, third = _directories(ws.control, "first", "second", "third")
    with owner.obliged([first], kind="owner"):
        pass
    with owner.obliged([second], kind="owner"):
        mark = len(journal)
        owner.seal_obligations()  # second's mutation has not finished: not cleared
        assert journal[mark:] == [("fsync", str(first))]
    # Through a commit point, with an obligation finishing while it runs.
    with _fs.Commit([ws.control], "fsync") as commit:
        fsync = os.fsync
        finishing: list[None] = []

        def during(descriptor: int) -> None:
            fsync(descriptor)
            if not finishing:
                finishing.append(None)
                with owner.obliged([third], kind="owner"):
                    pass

        monkeypatch.setattr(os, "fsync", during)
        owner.commit(commit)
        monkeypatch.setattr(os, "fsync", fsync)
    mark = len(journal)
    owner.seal_obligations()
    assert journal[mark:] == [("fsync", str(third))]  # second was sealed by the commit, third was not
    mark = len(journal)
    owner.seal_obligations()
    assert journal[mark:] == []


def test_obligations_of_both_kinds_mix(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    one, two = _job(ws, owner, "p/1"), _job(ws, owner, "p/2")
    (ws.control / "requests").mkdir()
    with owner:
        with _fs.Commit([ws.jobs, ws.control], "fsync") as commit:
            first = claim(ws, owner, one, commit=commit)
            second = claim(ws, owner, two, commit=commit)
            assert first is not None and second is not None
            assert not owner.latched and not owner.pinned(first)  # owner-gating obligations pin no job
            owner.commit(commit)
            first.unlink_requests([f"{first.job_id}.{uuid.uuid4()}.json"], commit=commit)  # exposure of first
            assert owner.pinned(first) and not owner.pinned(second) and owner.latched
            second.give_back()  # an unpinned handle leaves; its inline fsyncs seal its own directories
            with pytest.raises(WorkflowError, match="stays held"):
                first.give_back()
            mark = len(journal)
            owner.commit(commit)
            assert ("fsync", str(ws.control / "requests")) in journal[mark:]
        assert not owner.pinned(first)
        first.give_back()
        owner.unlatch()
    assert not owner.path.exists()


# -- the recovery-side seal (amendment 15) -----------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["fsync", "syncfs"])
def test_the_recovery_seal_comes_before_any_return_reconcile_or_removal(
    ws: FakeWorkspace, journal: Journal, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    monkeypatch.setattr(_fs, "protocol_mode", lambda path, setting: mode)
    dead = _owner(ws)
    ref = _job(ws, dead)
    held = claim(ws, dead, ref)
    assert held is not None
    scratch = dead.scratch("trash")  # a scratch recovery takes and reconciles
    dead.launch_dir(str(uuid.uuid4()), "0")
    (ws.control / "requests").mkdir()
    _kernel.attest_dead(ws, dead.owner_id, by="operator", evidence=[], operator="t", reason="t")
    recoverer = _owner(ws)
    mark = len(journal)
    report = _kernel.recover(ws, recoverer, dead.owner_id)
    assert [item.job_id for item in report.returned] == [ref.job_id]
    events = journal[mark:]
    first_rename = next(i for i, (op, _) in enumerate(events) if op == "rename")
    returned = next(i for i, (op, where) in enumerate(events) if op == "rename" and "/owned/" in where.split(" -> ")[0])
    taken = next(i for i, (op, where) in enumerate(events) if op == "rename" and ".trash." in where)
    removed = events.index(("unlink", str(dead.path / "owner.json")))
    if mode == "syncfs":
        seal = [events.index(("syncfs", str(ws.control / "requests")))]
    else:
        seal = [
            events.index(("fsync", str(ws.control / "requests"))),
            events.index(("fsync", str(ws.jobs / OWNED / dead.owner_id))),
            events.index(("fsync", str(held.path))),  # the held job directory (amendment 27)
            events.index(("fsync", str(scratch))),  # the dead owner's scratch
            events.index(("fsync", str(ws.control / "tmp"))),
        ]
        # A serial owner: no placement directory is sealed.
        assert ("fsync", str(ws.jobs / "ready")) not in events[: seal[-1]]
    assert seal == sorted(seal) and seal[-1] < first_rename  # before the return, the scratch and the record
    assert seal[-1] < returned and seal[-1] < taken and seal[-1] < removed


def test_recovery_seals_nothing_in_a_non_durable_workspace(tmp_path: Path, journal: Journal) -> None:
    root = tmp_path / "ws"
    workspace = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", False, 0.0)
    dead = _owner(workspace)
    _kernel.attest_dead(workspace, dead.owner_id, by="operator", evidence=[], operator="t", reason="t")
    mark = len(journal)
    _kernel.recover(workspace, _owner(workspace), dead.owner_id)
    assert [op for op, _ in journal[mark:] if op in ("fsync", "syncfs")] == []
