"""Batched publication of ``new_jobs`` batches (plan WORKFLOW_LOCAL_COMMANDS phase L4; amendments 13, 24, 25, 27).

A batch of at least ``_BATCH_MIN`` jobs, none moving inputs, under a ``syncfs`` barrier and the protocol predicate,
is published with one commit point per phase: P0 (the batch's payloads, the existing commit), P1 (every ``build/``
renamed to ``job/``), P2 (every ``job/`` renamed into ``jobs/ready/``), and only then yielded. Once the renames have
started, every job named ``job/`` is submitted, yielded or not, and an early end reports by id the jobs submitted and
not reported, named and not published, and unresolved. ``batch=1``, a job that moves inputs and fsync mode keep the
per-job path, pinned operation by operation (the pins were taken from HEAD 763ad94's ``scaffold.py``).
"""

import errno
import hashlib
import logging
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import Workspace, _fs, _kernel
from httk.workflow import scaffold as scaffold_module
from httk.workflow.scaffold import JobItem, new_jobs
from kernel._journal import Journal, record
from test_batched_creation import LARGE, moves_jobs, moves_setup, moving, ready_tags, tagged, tmp_entries
from test_input_moves import _unregister  # noqa: F401 - an autouse fixture
from test_job_creation import install, raw_package, ready
from test_sync_publication import durable_workspace
from v3_helpers import cli_owner

pytestmark = pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="names descriptors through /proc")

SOURCE = Path(__file__).resolve().parents[1] / "src"
_JOBS = 9


@pytest.fixture(autouse=True)
def _syncfs_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The protocol predicate holds (host-independent; ``syncfs`` is never really called), no batch time cap."""

    def resolve(path: Path, setting: object) -> tuple[str, str, str]:
        if setting == "fsync":
            return "fsync", "fsync (io.sync)", "ext4"
        return "syncfs", "syncfs (ext4, Linux 6.1)", "ext4"

    monkeypatch.setattr(_fs, "_resolve_sync", resolve)
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: None)
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    monkeypatch.setattr(scaffold_module, "_clock", lambda: 0.0)
    yield
    _fs.set_fault_injector(None)


def raw(tmp_path: Path, sync: str = "syncfs") -> Workspace:
    ws = durable_workspace(tmp_path / "ws", sync)
    install(ws, raw_package(tmp_path / "package"))
    return ws


def create(ws: Workspace, count: int, journal: Journal | None = None, **options: object) -> list[str]:
    """Create *count* tagged jobs (a journal records each yield); the tags yielded."""

    tags = []
    for job in new_jobs(ws, "tests.raw", iter(tagged(count)), **{"batch": LARGE, **options}):  # type: ignore[arg-type]
        if journal is not None:
            journal.append(("yield", str(job.tag)))
        tags.append(str(job.tag))
    return tags


def shape(journal: Journal, root: Path, *, since: int = 0) -> list[tuple[str, str]]:
    """The journal from *since* on with the workspace root, owner ids, UUIDs and fresh tokens replaced."""

    def normal(text: str) -> str:
        text = text.replace(str(root), "<ws>")
        text = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<uuid>", text)
        text = re.sub(r"[0-9a-f]{32}", "<owner>", text)
        return re.sub(r"(?<=[.~])[a-z2-7]{16}(?=$|[./~ ])", "<t>", text)

    return [(operation, normal(where)) for operation, where in journal[since:] if operation not in ("create", "write")]


def renames(journal: Journal, test: Callable[[str, str], bool]) -> list[int]:
    found = []
    for index, (operation, where) in enumerate(journal):
        if operation == "rename":
            source, _arrow, target = where.partition(" -> ")
            if test(source, target):
                found.append(index)
    return found


def named(source: str, target: str) -> bool:
    return source.endswith("/build") and target.endswith("/job")


def submitted(source: str, target: str) -> bool:
    return source.endswith("/job") and "/jobs/ready/" in target


# -- the call order (amendment 24) ------------------------------------------------------------------------------------


def test_p0_precedes_the_first_naming_p1_the_submits_and_p2_the_first_yield(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw(tmp_path)
    journal = record(monkeypatch, writes=True)
    assert create(ws, _JOBS, journal) == [f"j{index}" for index in range(_JOBS)]
    builds = [
        index for index, (operation, where) in enumerate(journal) if operation == "mkdir" and where.endswith("/build")
    ]
    syncs = journal.positions("syncfs")
    first_named, *_, last_named = renames(journal, named)
    first_submit, *_, last_submit = renames(journal, submitted)
    yields = journal.positions("yield")
    assert len(builds) == len(renames(journal, named)) == len(renames(journal, submitted)) == len(yields) == _JOBS
    # P0: a syncfs after the last job staged, before the first build/ is named job/.
    assert any(builds[-1] < sync < first_named for sync in syncs)
    # P1: after the last naming, before the first submit; P2: after the last submit, before the first yield.
    assert any(last_named < sync < first_submit for sync in syncs)
    assert any(last_submit < sync < yields[0] for sync in syncs)
    # Every naming precedes every submit, and every submit every yield.
    assert last_named < first_submit and last_submit < yields[0]
    # No per-job fsync from the first naming to the yields: the commit points are the syncfs calls.
    assert [entry for entry in journal[first_named : yields[0]] if entry[0] == "fsync"] == []
    # In syncfs mode the writing-descriptor fsyncs of staging (job.json, saved inputs) are skipped (L4.4): nothing
    # in a submit scratch is fsynced at all.
    assert [where for operation, where in journal if operation == "fsync" and ".submit." in where] == []
    # The scratches go after P2 (tmp/ an obligation the owner's close seals).
    scratches = [
        index for index, (operation, where) in enumerate(journal) if operation == "rmdir" and ".submit." in where
    ]
    p2 = max(sync for sync in syncs if sync < yields[0])
    assert len(scratches) == _JOBS and min(scratches) > p2
    # The owner was marked batched durably before the first batched mutation (amendment 27), and closed cleanly.
    marker = [index for index, (operation, where) in enumerate(journal) if where.endswith("/.batched")]
    assert marker and marker[0] < first_named
    assert [path for path in tmp_entries(ws) if ".submit." in path.name] == [] and len(ready(ws)) == _JOBS
    assert all(item.record is None for item in _kernel.list_owners(ws))


def test_a_batch_below_the_minimum_is_published_job_by_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = raw(tmp_path)
    journal = record(monkeypatch)
    create(ws, scaffold_module._BATCH_MIN - 1, journal)
    first_named = renames(journal, named)
    first_submit = renames(journal, submitted)
    # Interleaved: each job is named and submitted before the next one is named.
    assert all(a < b < c for a, b, c in zip(first_named, first_submit, first_named[1:], strict=False))
    assert not [where for operation, where in journal if where.endswith("/.batched")]


# -- the per-job path, pinned against HEAD ------------------------------------------------------------------------------

#: The per-job publication of one job (HEAD): build/ named job/ with the scratch fsynced, submitted with both
#: directories fsynced, the scratch removed.
_PER_JOB = [
    ("rename", "<ws>/.httk-workspace/tmp/<owner>.submit.<t>/build -> <ws>/.httk-workspace/tmp/<owner>.submit.<t>/job"),
    ("fsync", "<ws>/.httk-workspace/tmp/<owner>.submit.<t>"),
    ("rename", "<ws>/.httk-workspace/tmp/<owner>.submit.<t>/job -> <ws>/jobs/ready/j{n}--<uuid>~p500~<t>"),
    ("fsync", "<ws>/.httk-workspace/tmp/<owner>.submit.<t>"),
    ("fsync", "<ws>/jobs/ready"),
    ("rmdir", "<ws>/.httk-workspace/tmp/<owner>.submit.<t>"),
    ("yield", "j{n}"),
]


def per_job(count: int) -> list[tuple[str, str]]:
    return [(operation, where.format(n=n)) for n in range(count) for operation, where in _PER_JOB]


def published(journal: Journal, root: Path) -> list[tuple[str, str]]:
    """The normalized journal from the first naming of a payload on, without its fsyncs of the payload's files."""

    start, end = renames(journal, named)[0], (journal.positions("yield") or [len(journal) - 1])[-1]
    return [
        entry
        for entry in shape(journal, root, since=start)[: end + 1 - start]
        if not entry[1].startswith("<ws>/jobs/ready/j")
    ]


@pytest.mark.parametrize("sync", ["syncfs", "fsync"])
def test_batch_one_keeps_the_per_job_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sync: str) -> None:
    # Even with the minimum lowered to one job, batch=1 (the library default) is published job by job.
    monkeypatch.setattr(scaffold_module, "_BATCH_MIN", 1, raising=False)
    ws = raw(tmp_path, sync)
    journal = record(monkeypatch)
    create(ws, 3, journal, batch=1)
    # Each job is committed before the next item is staged: the stagings' own operations fall between the
    # publications, which are each exactly HEAD's.
    yields = journal.positions("yield")
    windows = [
        shape(journal, ws.root, since=start)[: end + 1 - start]
        for start, end in zip(renames(journal, named), yields, strict=True)
    ]
    assert [entry for window in windows for entry in window] == per_job(3)


def test_fsync_mode_keeps_the_per_job_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = raw(tmp_path, "fsync")
    journal = record(monkeypatch)
    create(ws, _JOBS, journal)
    assert published(journal, ws.root) == per_job(_JOBS)
    assert journal.positions("syncfs") == []


def test_moved_inputs_keep_the_per_job_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scaffold_module, "_BATCH_MIN", 1, raising=False)
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    journal = record(monkeypatch)
    jobs = list(moves_jobs(ws, [moving(source, "x")]))
    assert [job.tag for job in jobs] == ["x"]
    steps = [entry for entry in published(journal, ws.root) if entry[0] != "unlink"]
    expected = [entry for entry in per_job(1) if entry[0] != "yield"]
    expected = [(operation, where.replace("j0--", "x--")) for operation, where in expected]
    # The moved job's markers go after its submit (unlinks, not pinned here), then its scratch, as on HEAD.
    assert steps[:5] == expected[:5] and ("rmdir", expected[5][1]) in steps[5:]
    assert not [where for operation, where in journal if where.endswith("/.batched")]


# -- accounting (amendment 13) ------------------------------------------------------------------------------------------


def test_closing_after_the_first_yield_reports_the_rest_by_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    ws = raw(tmp_path)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(_JOBS)), batch=LARGE)
    first = next(jobs)
    with caplog.at_level(logging.WARNING, logger="httk.workflow.scaffold"):
        jobs.close()
    # Every job of the batch was published before the first yield: all are submitted, the rest named by id.
    assert first.tag == "j0" and ready_tags(ws) == sorted(f"j{index}" for index in range(_JOBS))
    (message,) = [record.getMessage() for record in caplog.records if "before reporting" in record.getMessage()]
    keys = {ref.job_key for ref in ready(ws)} - {first.job_key}
    assert "1 job(s) of this command were reported as submitted" in message
    assert f"{_JOBS - 1} more were submitted but not reported" in message
    assert all(key in message for key in keys) and first.job_key not in message
    assert "named complete" not in message and "not submitted" not in message
    assert [path for path in tmp_entries(ws) if ".submit." in path.name] == []
    assert all(item.record is None for item in _kernel.list_owners(ws))


def test_an_error_thrown_in_at_a_yield_notes_the_jobs_not_reported(tmp_path: Path) -> None:
    ws = raw(tmp_path)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(_JOBS)), batch=LARGE)
    next(jobs)
    next(jobs)
    with pytest.raises(OSError, match="broken pipe") as raised:
        jobs.throw(OSError(errno.EPIPE, "broken pipe"))
    (note,) = raised.value.__notes__
    assert note.startswith("2 job(s) of this command were reported as submitted; 7 more were submitted but not")
    assert len(ready(ws)) == _JOBS


def _failing_commit(point: int, moment: str = "before") -> None:
    # The point-th commit of a protocol collector (P1 is 1, P2 is 2) fails.
    seen = {"before": 0, "after": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "commit":
            seen[phase] += 1
            if phase == moment and seen[phase] == point:
                raise _fs.SyncFailed(errno.EIO, "cannot sync to disk: Input/output error")

    _fs.set_fault_injector(fault)


def test_a_failed_p1_leaves_the_named_jobs_to_the_owner_s_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw(tmp_path)
    finished: list[Path] = []
    finish = scaffold_module._finish_submit

    def finishing(owner: _kernel.Owner, scratch: Path) -> bool:
        finished.append(scratch)
        return finish(owner, scratch)

    monkeypatch.setitem(_kernel._RECONCILERS, "submit", finishing)
    _failing_commit(1)
    with pytest.raises(_fs.SyncFailed, match="Input/output error") as raised:
        create(ws, _JOBS)
    _fs.set_fault_injector(None)
    (note,) = raised.value.__notes__
    assert note.startswith("0 job(s) of this command were reported as submitted; 9 were named complete but not")
    # The reseal by directory succeeded: the owner's close published every named job/ through _finish_submit.
    assert len(finished) == _JOBS and ready_tags(ws) == sorted(f"j{index}" for index in range(_JOBS))
    assert [path for path in tmp_entries(ws) if ".submit." in path.name] == []
    assert all(item.record is None for item in _kernel.list_owners(ws))


def test_a_failed_p2_is_resealed_by_directory_and_the_jobs_yielded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw(tmp_path)
    journal = record(monkeypatch)
    _failing_commit(2)
    assert create(ws, _JOBS, journal) == [f"j{index}" for index in range(_JOBS)]
    last_submit = renames(journal, submitted)[-1]
    first_yield = journal.positions("yield")[0]
    resealed = {where for operation, where in journal[last_submit:first_yield] if operation == "fsync"}
    assert str(ws.jobs / "ready") in resealed and str(ws.control / "tmp") not in resealed
    assert all(item.record is None for item in _kernel.list_owners(ws))


def test_a_failed_p2_whose_reseal_fails_keeps_the_owner_and_reports_the_jobs_unresolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw(tmp_path)
    _failing_commit(2)
    monkeypatch.setattr(
        _kernel.Owner,
        "seal_obligations",
        lambda self: (_ for _ in ()).throw(_fs.SyncFailed(errno.EIO, "reseal failed")),
    )
    with pytest.raises(_fs.SyncFailed) as raised:
        create(ws, _JOBS)
    _fs.set_fault_injector(None)
    (note,) = raised.value.__notes__
    assert "9 more were submitted but not reported" in note
    assert "the durability of 9 of them is unresolved" in note and "kept for recovery" in note
    (owner,) = [item for item in _kernel.list_owners(ws) if item.record is not None]
    assert (ws.control / "owners" / owner.owner_id / ".batched").is_file()
    assert len(ready(ws)) == _JOBS


def test_a_refusal_before_the_renames_publishes_job_by_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A check that refuses the fifth job (after the batch check started, before any rename): the batch falls back to
    # the per-job path, which publishes the four before it and refuses it as without batching.
    ws = raw(tmp_path)
    journal = record(monkeypatch)
    read_header = _kernel._read_header
    calls = {"n": 0}

    def header(directory: Path, expect_key: str | None = None) -> _kernel.JobHeader:
        calls["n"] += 1
        if calls["n"] in (5, 10):  # the batch check's fifth, then the per-job submit's fifth (five later)
            raise _kernel.FormatError(f"{directory}/job.json is refused")
        return read_header(directory, expect_key)

    monkeypatch.setattr(_kernel, "_read_header", header)
    with pytest.raises(_kernel.FormatError, match="refused") as raised:
        create(ws, _JOBS)
    assert ready_tags(ws) == ["j0", "j1", "j2", "j3"]
    assert raised.value.__notes__[0].endswith("was not submitted, and the 4 job(s) staged after it were not submitted")
    # The batch check's refusal came before any rename: then the jobs were named and submitted one by one.
    first_named, first_submit = renames(journal, named), renames(journal, submitted)
    assert len(first_named) == 5 and len(first_submit) == 4
    assert all(a < b < c for a, b, c in zip(first_named, first_submit, first_named[1:], strict=False))


def test_a_placement_recreated_before_its_submit_is_resealed_as_the_new_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # H1: the check creates p/q; before the first submit rename a pruner removes it (empty), the move recreates it
    # (a new inode), then P2 fails: the reseal by directory fsyncs the directory the jobs are in, not the old one.
    ws = raw(tmp_path)
    placement = ws.jobs / "ready" / "p" / "q"
    seen: dict[str, object] = {"pruned": None, "commits": 0, "resealing": False}
    inodes: list[int] = []
    fsync = os.fsync

    def fsyncing(descriptor: int) -> None:
        if seen["resealing"]:
            inodes.append(os.fstat(descriptor).st_ino)
        fsync(descriptor)

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        renaming = op == "rename" and phase == "before" and dst is not None and dst.path.parent == placement
        if renaming and seen["pruned"] is None:
            seen["pruned"] = os.stat(placement).st_ino
            os.rmdir(placement)
            os.rmdir(placement.parent)
        if op == "commit" and phase == "before":
            seen["commits"] = int(str(seen["commits"])) + 1
            if seen["commits"] == 2:
                seen["resealing"] = True
                raise _fs.SyncFailed(errno.EIO, "cannot sync to disk: Input/output error")

    monkeypatch.setattr(os, "fsync", fsyncing)
    _fs.set_fault_injector(fault)
    assert create(ws, _JOBS, placement="p/q") == [f"j{index}" for index in range(_JOBS)]
    _fs.set_fault_injector(None)
    recreated = os.stat(placement).st_ino
    assert seen["pruned"] is not None and recreated != seen["pruned"]
    assert recreated in inodes and os.stat(placement.parent).st_ino in inodes
    assert all(item.record is None for item in _kernel.list_owners(ws))


@pytest.mark.parametrize("registration", ["works", "fails"])
def test_a_submit_that_raises_after_its_rename_still_registers_the_new_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, registration: str
) -> None:
    # H1b: p/q is pruned and recreated by the first submit's move, whose rename then raises after it happened (as
    # directory inspection or the collector's bookkeeping may): the landed directory is registered all the same, so
    # the reseal fsyncs the new inode; when that registration fails, the owner is kept latched for recovery.
    ws = raw(tmp_path)
    placement = ws.jobs / "ready" / "p" / "q"
    seen: dict[str, object] = {"pruned": None, "raised": False, "sealing": False}
    inodes: list[int] = []
    fsync = os.fsync

    def fsyncing(descriptor: int) -> None:
        if seen["sealing"]:
            inodes.append(os.fstat(descriptor).st_ino)
        fsync(descriptor)

    seal = _kernel.Owner.seal_obligations

    def sealing(self: _kernel.Owner) -> None:
        # Only the reseal's fsyncs count (the close's own submits fsync the placement too).
        seen["sealing"] = seen["raised"]
        try:
            seal(self)
        finally:
            seen["sealing"] = False

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op != "rename" or dst is None or dst.path.parent != placement:
            return
        if phase == "before" and seen["pruned"] is None:
            seen["pruned"] = os.stat(placement).st_ino
            os.rmdir(placement)
            os.rmdir(placement.parent)
        elif phase == "after" and not seen["raised"] and _fs.exists(dst):
            seen["raised"] = True
            raise OSError(errno.EIO, "bookkeeping failed after the rename")

    obliged = _kernel.Owner.obliged

    def obliging(self: _kernel.Owner, directories: Iterable[Path], **options: Any) -> Any:
        directories = list(directories)
        if registration == "fails" and seen["raised"] and directories[:1] == [placement]:
            raise OSError(errno.EMFILE, "cannot register")
        return obliged(self, directories, **options)

    monkeypatch.setattr(_kernel.Owner, "obliged", obliging)
    monkeypatch.setattr(os, "fsync", fsyncing)
    monkeypatch.setattr(_kernel.Owner, "seal_obligations", sealing)
    _fs.set_fault_injector(fault)
    with pytest.raises(OSError) as raised:
        create(ws, _JOBS, placement="p/q")
    _fs.set_fault_injector(None)
    recreated = os.stat(placement).st_ino
    assert seen["pruned"] is not None and recreated != seen["pruned"]
    owners = [item for item in _kernel.list_owners(ws) if item.record is not None]
    if registration == "works":
        assert "bookkeeping failed" in str(raised.value)
        assert recreated in inodes  # the reseal (not the close) fsynced the new directory
        assert owners == []  # resealed: the close published the rest
        assert len(ready(ws)) == _JOBS
    else:
        assert "cannot register" in str(raised.value)
        (owner,) = owners  # latched, close skipped: owner.json and the batched marker stay for recovery
        assert (ws.control / "owners" / owner.owner_id / ".batched").is_file()
        assert "is unresolved" in raised.value.__notes__[0]


def _take_scratch(ws: Workspace, tag: str = "j0") -> None:
    """As a recoverer takes a scratch: the submit scratch of the job tagged *tag* is renamed away."""

    for path in tmp_entries(ws):
        for name in ("build", "job"):
            definition = path / name / "job.json"
            if ".submit." in path.name and definition.is_file() and f'"tag":"{tag}"' in definition.read_text():
                path.rename(path.with_name("taken"))
                return
    raise AssertionError(f"no scratch of {tag}")


def _assert_left_to_recovery(ws: Workspace, journal: Journal) -> None:
    # The owner is kept (latched, close skipped) and nothing more was published or named job by job.
    (owner,) = [item for item in _kernel.list_owners(ws) if item.record is not None]
    assert (ws.control / "owners" / owner.owner_id / "owner.json").is_file()
    assert ready(ws) == [] and renames(journal, submitted) == []


def test_owner_lost_in_the_batch_check_stops_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # H2: a recoverer took a scratch before the check: OwnerLost is no refusal, so nothing falls back job by job.
    ws = raw(tmp_path)
    check = scaffold_module._check_batch

    def checking(*args: object, **kwargs: object) -> None:
        _take_scratch(ws)
        check(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(scaffold_module, "_check_batch", checking)
    journal = record(monkeypatch)
    with pytest.raises(_kernel.OwnerLost):
        create(ws, _JOBS)
    assert renames(journal, named) == []
    _assert_left_to_recovery(ws, journal)
    # Every other build/ is still there for recovery to discard; no job/ was made.
    assert sum((path / "build").is_dir() for path in tmp_entries(ws) if ".submit." in path.name) == _JOBS - 1


def test_owner_lost_after_p1_stops_publication_and_skips_the_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # H2: a recoverer took the first scratch after P1: the named job/ entries are left to recovery, never published
    # by this owner's close.
    ws = raw(tmp_path)
    journal = record(monkeypatch)

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "commit" and phase == "after":
            _fs.set_fault_injector(None)
            _take_scratch(ws)

    _fs.set_fault_injector(fault)
    with pytest.raises(_kernel.OwnerLost) as raised:
        create(ws, _JOBS)
    _fs.set_fault_injector(None)
    assert renames(journal, submitted) == []
    _assert_left_to_recovery(ws, journal)
    assert sum((path / "job").is_dir() for path in tmp_entries(ws) if ".submit." in path.name) == _JOBS - 1
    (note,) = raised.value.__notes__
    assert "were named complete but not published yet" in note and "left to the recovery" in note


def test_a_failed_p2_reseal_is_never_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # H3: the first reseal fails; a retry would succeed, but none is made: the owner latches at once, keeps
    # owner.json, and the jobs are reported unresolved.
    ws = raw(tmp_path)
    _failing_commit(2)
    calls: list[int] = []
    seal = _kernel.Owner.seal_obligations

    def sealing(self: _kernel.Owner) -> None:
        calls.append(1)
        if len(calls) == 1:
            raise _fs.SyncFailed(errno.EIO, "reseal failed")
        seal(self)

    monkeypatch.setattr(_kernel.Owner, "seal_obligations", sealing)
    with pytest.raises(_fs.SyncFailed, match="reseal failed") as raised:
        create(ws, _JOBS)
    _fs.set_fault_injector(None)
    assert calls == [1]
    (note,) = raised.value.__notes__
    assert "the durability of 9 of them is unresolved" in note
    (owner,) = [item for item in _kernel.list_owners(ws) if item.record is not None]
    assert (ws.control / "owners" / owner.owner_id / "owner.json").is_file()


# -- the crash matrix (amendment 24) ------------------------------------------------------------------------------------

#: Runs job creation in a separate process and exits without unwinding at one point: before or after P0 (the batch
#: commit), P1 or P2 (the protocol collector's first and second commit).
_CRASHING = """
import os, sys
from pathlib import Path
from httk.workflow import Workspace, _fs
from httk.workflow import scaffold

root, point, moment, count = sys.argv[1:]
_fs._resolve_sync = lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4")
_fs._syncfs = lambda descriptor: None
seen = {"before": 0, "after": 0}
commit_batch = scaffold._commit_batch

def committing(*args, **kwargs):
    if point == "P0" and moment == "before":
        os._exit(17)
    commit_batch(*args, **kwargs)
    if point == "P0" and moment == "after":
        os._exit(17)

def fault(op, phase, src, dst):
    if op == "commit":
        seen[phase] += 1
        if phase == moment and seen[phase] == {"P1": 1, "P2": 2}.get(point):
            os._exit(17)

scaffold._commit_batch = committing
_fs.set_fault_injector(fault)
items = [{"tag": f"j{index}"} for index in range(int(count))]
for job in scaffold.new_jobs(Workspace(Path(root)), "tests.raw", iter(items), batch=256):
    print(job.tag, flush=True)
os._exit(0)
"""


@pytest.mark.parametrize("moment", ["before", "after"])
@pytest.mark.parametrize("point", ["P0", "P1", "P2"])
def test_a_crash_at_a_commit_point_is_recovered_to_each_job_published_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str, moment: str
) -> None:
    ws = raw(tmp_path)
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    child = subprocess.run(
        [sys.executable, "-c", _CRASHING, str(ws.root), point, moment, str(_JOBS)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert child.returncode == 17, child.stderr
    assert child.stdout == ""  # nothing was yielded before P2 was durable
    (dead,) = [item.owner_id for item in _kernel.list_owners(ws) if item.record is not None]
    dead_dir = ws.control / "owners" / dead
    renamed = point != "P0"
    scratches = [path for path in tmp_entries(ws) if ".submit." in path.name]
    # Before P1's commit every build/ was named job/ already (the phase renames, then commits); before P0 none was.
    in_ready = len(ready(ws))
    assert in_ready == (_JOBS if point == "P2" else 0)
    assert sum((path / "job").is_dir() for path in scratches) == (_JOBS if point == "P1" else 0)
    assert sum((path / "build").is_dir() for path in scratches) == (_JOBS if point == "P0" else 0)
    assert (dead_dir / ".batched").is_file() == renamed
    # Recovery seals first (amendment 27), then publishes every job/ through the submit reconciler, exactly once.
    calls: list[str] = []
    seal, finish = _fs.seal_directories, scaffold_module._finish_submit

    def sealing(directories: list[Path], *args: object, **kwargs: object) -> None:
        calls.append("seal")
        if renamed:
            assert ws.jobs / "ready" in directories  # a batched owner: every placement directory too
        seal(directories, *args, **kwargs)  # type: ignore[arg-type]

    def finishing(owner: _kernel.Owner, scratch: Path) -> bool:
        if (scratch / "job").is_dir():
            calls.append("finish")  # one that publishes a staged job/
        return finish(owner, scratch)

    monkeypatch.setattr(_fs, "seal_directories", sealing)
    monkeypatch.setitem(_kernel._RECONCILERS, "submit", finishing)
    _kernel.attest_dead(ws, dead, by="operator", evidence=[], operator="t", reason="crash")
    with cli_owner(ws) as rescuer:
        _kernel.recover(ws, rescuer, dead)
    assert calls[0] == "seal"
    assert calls.count("finish") == (_JOBS if point == "P1" else 0)
    published = sorted(ref.job_key.split("--")[0] for ref in ready(ws))
    expected = [] if point == "P0" else sorted(f"j{index}" for index in range(_JOBS))
    assert published == expected  # each job once: no duplicate, none lost after its naming
    assert len({ref.job_key for ref in ready(ws)}) == len(published)
    assert [path for path in tmp_entries(ws) if ".submit." in path.name] == []
    assert not (dead_dir / "owner.json").exists()
    # A rerun with --duplicate-tag skip creates exactly the jobs not submitted.
    rerun = [job.tag for job in new_jobs(ws, "tests.raw", iter(tagged(_JOBS)), batch=LARGE, duplicate_tag="skip")]
    assert rerun == ([f"j{index}" for index in range(_JOBS)] if point == "P0" else [])
    assert ready_tags(ws) == sorted(f"j{index}" for index in range(_JOBS))


# -- written contents -----------------------------------------------------------------------------------------------

#: The digest of the 9 jobs ``_written_jobs`` creates, as HEAD 763ad94 (whose publication is job by job) wrote them:
#: every file of each payload, ``job.json`` raw except its ``id`` (a fresh UUID; nothing else in it varies).
_HEAD_DIGEST = "1a1c6d7fcf60909ab61d5ac993e81f1d8e2ab6b3c8f51cc990e250d433627cac"


def _written_jobs(root: Path) -> tuple[Workspace, list[object]]:
    """Create 9 jobs, each with its own input file and parameter, under the default and a nested placement."""

    ws = raw(root)
    inputs = root / "inputs"
    inputs.mkdir()
    items: list[JobItem] = []
    for index in range(_JOBS):
        (inputs / f"in{index}").write_text(f"structure {index}\n", encoding="utf-8")
        item: JobItem = {"tag": f"j{index}", "files": {"INPUT": inputs / f"in{index}"}, "parameters": {"index": index}}
        if index % 2:
            item["placement"] = "p/q"
        items.append(item)
    return ws, list(new_jobs(ws, "tests.raw", iter(items), batch=LARGE))


def _written_digest(jobs: list[object]) -> str:
    """The digest of the jobs' payloads (by tag), ``job.json``'s ``id`` replaced."""

    digest = hashlib.sha256()
    for job in sorted(jobs, key=lambda job: str(job.tag)):  # type: ignore[attr-defined]
        payload: Path = job.payload  # type: ignore[attr-defined]
        for item in sorted(payload.rglob("*")):
            if not item.is_file():
                continue
            data = item.read_bytes()
            if item.name == "job.json":
                data = data.replace(str(job.job_id).encode(), b"<id>")  # type: ignore[attr-defined]
            digest.update(f"{job.tag}/{item.relative_to(payload)}\0{len(data)}\0".encode())  # type: ignore[attr-defined]
            digest.update(data)
    return digest.hexdigest()


def test_batched_jobs_are_written_byte_for_byte_as_head_wrote_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Each job carries its own input (a stale input or structure across jobs would change the digest).
    batches: list[int] = []
    publish = scaffold_module._publish_batch

    def publishing(*args: object, **kwargs: object) -> None:
        batches.append(1)
        publish(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(scaffold_module, "_publish_batch", publishing)
    _ws, jobs = _written_jobs(tmp_path)
    assert batches == [1] and len(jobs) == _JOBS  # the batched publication wrote them
    for index, job in enumerate(sorted(jobs, key=lambda job: int(str(job.tag)[1:]))):  # type: ignore[attr-defined]
        assert (job.payload / "files" / "INPUT").read_text(encoding="utf-8") == f"structure {index}\n"  # type: ignore[attr-defined]
    assert _written_digest(jobs) == _HEAD_DIGEST
