"""The step primitives of plan WORKFLOW_LOCAL_COMMANDS phase L3a (§5 amendment 14), carved out of ``claim``,
``OwnedJob.write_state``, ``append_log``, ``release`` and ``discard``.

The serial methods are compositions of these with inline fsyncs; their sequences are pinned by
``test_protocol_pins`` and ``test_local_commands``. Here each primitive is driven in the batched mode a later phase
will use, one phase at a time with a commit point after each, and its guards are checked in that mode too.
"""

import errno
import json
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import _fs, _kernel
from httk.workflow._kernel import OWNED, JobRef, Owner, Release, claim, register_owner, submit
from httk.workflow._state import StateDoc, decode_state
from httk.workflow.errors import WorkflowError
from kernel._journal import Journal, record


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a durable workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@pytest.fixture
def ws(tmp_path: Path) -> FakeWorkspace:
    root = tmp_path / "ws"
    workspace = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", True, 0.0)
    (workspace.control / "requests").mkdir(parents=True)
    return workspace


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    yield
    _fs.set_fault_injector(None)


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> Journal:
    return record(monkeypatch)


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


def _between(journal: Journal, start: int, end: int, *operations: str) -> list[tuple[str, str]]:
    return [entry for entry in journal[start:end] if entry[0] in operations]


def test_a_batched_release_makes_each_phase_durable_only_at_its_commit_point(
    ws: FakeWorkspace, journal: Journal
) -> None:
    owner = _owner(ws)
    ref = _job(ws, owner)
    request_id, request = _request(ws, ref.job_id)
    owned_dir = ws.jobs / OWNED / owner.owner_id
    owned_dir.mkdir(parents=True)
    with owner:
        with _fs.Commit([ws.jobs, ws.control], "fsync", workers=1) as commit:
            owner.mark_batched()  # as a batch does before its first mutation (amendment 27)
            assert (owner.path / ".batched").is_file()
            marks = [len(journal)]
            # B: the claim.
            handle = claim(ws, owner, ref, commit=commit)
            assert handle is not None
            assert (ws.jobs / "ready" / "p" / "q").is_dir()  # pruning is deferred
            marks.append(len(journal))
            owner.commit(commit)
            marks.append(len(journal))
            # C1: the run-log lines and the state temporary.
            handle.append_log_line("claimed", {"from": "ready", "to": OWNED}, commit=commit)
            handle.append_log_line("request_applied", {"detail": {"request_id": request_id}}, commit=commit)
            doc = StateDoc.empty(handle.job_id).next_activation("start", "initial")
            release = Release("paused", 500, (request_id,))
            staged = handle.stage_state(doc.with_release(release), commit=commit)
            marks.append(len(journal))
            owner.commit(commit)
            marks.append(len(journal))
            # C2: the state replace.
            handle.replace_state(staged, commit=commit)
            assert owner.pinned(handle) and owner.latched
            marks.append(len(journal))
            owner.commit(commit)
            marks.append(len(journal))
            # D1: the request unlink.
            handle.unlink_requests([request.name], commit=commit)
            marks.append(len(journal))
            owner.commit(commit)
            assert not owner.pinned(handle)
            marks.append(len(journal))
            # D2: the release move.
            moved = handle.move_out("paused", PurePosixPath("p/q"), 500, commit=commit)
            marks.append(len(journal))
            owner.commit(commit)
            marks.append(len(journal))
        owner.unlatch()
    claimed = handle.path
    # Nothing is fsynced between a phase's first mutation and its commit point.
    for start, end in zip(marks[0::2], marks[1::2], strict=False):
        assert not _between(journal, start, end, "fsync", "syncfs")
    commits = [
        sorted(_between(journal, start, end, "fsync")) for start, end in zip(marks[1::2], marks[2::2], strict=False)
    ]
    assert commits[0] == sorted([("fsync", str(ws.jobs / "ready" / "p" / "q")), ("fsync", str(owned_dir))])
    assert commits[1] == sorted(
        [
            ("fsync", str(claimed / "logs" / "runlog.jsonl")),  # the log data, on its writing descriptor (once)
            ("fsync", str(staged.temporary.path)),  # the state temporary's writing descriptor
            ("fsync", str(claimed)),  # logs/ was created there
            ("fsync", str(claimed / "logs")),  # runlog.jsonl was created there
        ]
    )
    assert commits[2] == [("fsync", str(claimed))]
    assert commits[3] == [("fsync", str(ws.control / "requests"))]
    created = sorted(
        ("fsync", str(path))
        for path in (owned_dir, ws.jobs, ws.jobs / "paused", ws.jobs / "paused" / "p", moved.path.parent)
    )
    assert commits[4] == created  # owned/<O>, the destination and the parents of what the move created
    assert not request.exists() and moved.path.is_dir()
    recorded = decode_state((moved.path / "state.json").read_bytes())
    assert recorded.release_to == Release("paused", 500) and recorded.applied_requests == (request_id,)
    assert not owner.path.exists()


def test_a_batched_release_in_syncfs_mode_issues_one_syncfs_per_commit_point(
    ws: FakeWorkspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_fs, "_resolve_sync", lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4"))
    owner = _owner(ws)
    ref = _job(ws, owner)
    request_id, request = _request(ws, ref.job_id)
    entry = len(journal)
    with owner:
        with _fs.Commit([ws.jobs, ws.control], "auto") as commit:
            assert commit.mode == "syncfs"
            primed = len(journal)
            handle = claim(ws, owner, ref, commit=commit)
            assert handle is not None
            owner.commit(commit)
            handle.append_log_line("claimed", {}, commit=commit)
            staged = handle.stage_state(
                StateDoc.empty(handle.job_id).with_release(Release("paused", 500, (request_id,))), commit=commit
            )
            owner.commit(commit)
            handle.replace_state(staged, commit=commit)
            owner.commit(commit)
            handle.unlink_requests([request.name], commit=commit)
            owner.commit(commit)
            handle.move_out("paused", PurePosixPath("p/q"), 500, commit=commit)
            owner.commit(commit)
        owner.unlatch()
    assert _between(journal, entry, primed, "fsync", "syncfs") == [("syncfs", str(ws.jobs))]
    assert _between(journal, primed, len(journal), "syncfs") == [("syncfs", str(ws.jobs))] * 5
    # Only owner.json's own removal fsyncs afterwards; no step fsynced inline.
    synced = _between(journal, primed, len(journal), "fsync")
    assert all(path.startswith(str(ws.control / "owners")) for _, path in synced)


def test_stage_state_drops_retired_ids_and_refuses_another_job(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    retired, kept = str(uuid.uuid4()), str(uuid.uuid4())
    handle._retired.add(retired)
    doc = StateDoc.empty(handle.job_id).with_release(Release("paused", 500, (retired, kept)))
    with _fs.Commit([handle.path], "fsync") as commit:
        staged = handle.stage_state(doc, commit=commit)
        assert decode_state(Path(staged.temporary.path).read_bytes()).applied_requests == (kept,)
        with pytest.raises(ValueError, match="written to"):
            handle.stage_state(StateDoc.empty(str(uuid.uuid4())), commit=commit)
        handle.replace_state(staged, commit=commit)
        owner.commit(commit)
    assert handle.already_applied(kept) and not handle.already_applied(retired)
    assert not handle.already_applied(str(uuid.uuid4()))


def test_every_batched_step_checks_the_job_is_still_there(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    with _fs.Commit([handle.path, ws.jobs], "fsync") as commit:
        staged = handle.stage_state(StateDoc.empty(handle.job_id), commit=commit)
        away = ws.root / "away"
        handle.path.rename(away)  # a recoverer took it
        for step in (
            lambda: handle.stage_state(StateDoc.empty(handle.job_id), commit=commit),
            lambda: handle.replace_state(staged, commit=commit),
            lambda: handle.append_log_line("claimed", {}, commit=commit),
            lambda: handle.move_out("ready", PurePosixPath("p/q"), 500, commit=commit),
            lambda: handle.move_to_trash(commit=commit),
        ):
            with pytest.raises(_kernel.OwnerLost):
                step()


def test_a_batched_move_out_checks_the_placement(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    (ws.jobs / "paused").mkdir()
    (ws.root / "elsewhere").mkdir()
    (ws.jobs / "paused" / "p").symlink_to(ws.root / "elsewhere")
    with _fs.Commit([ws.jobs], "fsync") as commit, pytest.raises(_fs.UnsafePath):
        handle.move_out("paused", PurePosixPath("p/q"), 500, commit=commit)
    assert handle.path.is_dir() and not list((ws.root / "elsewhere").iterdir())


@pytest.mark.parametrize("planted", ["logs symlink", "logs file", "runlog directory", "runlog fifo"])
def test_a_batched_log_step_refuses_planted_entries_which_the_serial_one_replaces(
    ws: FakeWorkspace, planted: str
) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    logs = handle.path / "logs"
    if planted == "logs symlink":
        logs.symlink_to(ws.root)
    elif planted == "logs file":
        logs.write_text("")
    else:
        logs.mkdir()
        if planted == "runlog directory":
            (logs / "runlog.jsonl").mkdir()
        else:
            os.mkfifo(logs / "runlog.jsonl")
    with _fs.Commit([handle.path], "fsync") as commit, pytest.raises(_kernel.StepRefused):
        handle.append_log_line("claimed", {}, commit=commit)
    handle.append_log("claimed")  # the serial step removes the planted entry and recreates it
    assert (logs / "runlog.jsonl").is_file() and not (logs / "runlog.jsonl").is_symlink()


def test_a_serial_claim_prunes_and_a_batched_one_leaves_it(ws: FakeWorkspace) -> None:
    owner = _owner(ws)
    serial, batched = _job(ws, owner, "a/b"), _job(ws, owner, "c/d")
    assert claim(ws, owner, serial) is not None
    assert not (ws.jobs / "ready" / "a").exists()
    with _fs.Commit([ws.jobs], "fsync") as commit:
        assert claim(ws, owner, batched, commit=commit) is not None
        owner.commit(commit)
    assert (ws.jobs / "ready" / "c" / "d").is_dir()


def test_a_batched_discard_moves_to_the_trash_then_unlinks_its_requests(ws: FakeWorkspace, journal: Journal) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    _request_id, request = _request(ws, handle.job_id)
    with _fs.Commit([ws.jobs, ws.control], "fsync", workers=1) as commit:
        trashed = handle.move_to_trash(commit=commit)
        assert owner.latched and not owner.pinned(handle)  # owner-gating obligations of a handle
        mark = len(journal)
        owner.commit(commit)  # D′
        sealed = sorted(_between(journal, mark, len(journal), "fsync"))
        assert sealed == sorted([("fsync", str(handle.path.parent)), ("fsync", str(Path(trashed.path).parent))])
        _fs.remove_trashed(trashed)  # the tree unlinks only after D′
        handle.retire_discarded()
        handle.unlink_requests([request.name], commit=commit)
        mark = len(journal)
        owner.commit(commit)  # D″
        assert _between(journal, mark, len(journal), "fsync") == [("fsync", str(ws.control / "requests"))]
    owner.unlatch()
    owner.close()
    assert not owner.path.exists() and not request.exists()


def test_the_serial_methods_are_the_compositions(ws: FakeWorkspace, journal: Journal) -> None:
    # The serial release: write_state (stage, then replace), unlink_requests, move_out; each step's own fsync in
    # place, exactly as before (the pins compare whole command paths).
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    request_id, request = _request(ws, handle.job_id)
    mark = len(journal)
    moved = handle.release(StateDoc.empty(handle.job_id), Release("paused", 500, (request_id,)))
    events = [entry for entry in journal[mark:] if entry[0] != "rename" or "state.json" not in entry[1]]
    ops = [op for op, _ in events]
    # The temporary's descriptor, replace, the job directory; unlink, requests/; three placement directories created
    # (each one's parent), the move, owned/<O> and the destination.
    assert ops == [
        "fsync",
        "replace",
        "fsync",
        "unlink",
        "fsync",
        "fsync",
        "fsync",
        "fsync",
        "rename",
        "fsync",
        "fsync",
    ]
    assert events[2] == ("fsync", str(handle.path))
    assert events[3:5] == [("unlink", str(request)), ("fsync", str(ws.control / "requests"))]
    assert events[-1] == ("fsync", str(moved.path.parent))
    owner.close()


def test_a_failed_seal_leaves_the_handle_pinned(
    ws: FakeWorkspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    with _fs.Commit([handle.path], "fsync") as commit:
        handle.replace_state(handle.stage_state(StateDoc.empty(handle.job_id), commit=commit), commit=commit)
        fsync = os.fsync
        monkeypatch.setattr(os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError(errno.EIO, "dead disk")))
        with pytest.raises(OSError):
            owner.commit(commit)
        monkeypatch.setattr(os, "fsync", fsync)
        assert owner.pinned(handle)
        with pytest.raises(WorkflowError, match="stays held"):
            handle.give_back()
        owner.commit(commit)
    assert not owner.pinned(handle)
    handle.give_back()
    owner.unlatch()
    owner.close()


def test_a_serial_step_seals_the_obligations_of_its_directory(ws: FakeWorkspace) -> None:
    # The C2 failure row (amendments 11, 18): the serial release after an unsealed batched state replace. Its inline
    # fsync of the job directory seals that obligation, so the move out is allowed.
    owner = _owner(ws)
    handle = claim(ws, owner, _job(ws, owner))
    assert handle is not None
    doc = StateDoc.empty(handle.job_id)
    with _fs.Commit([handle.path], "fsync") as commit:
        handle.replace_state(handle.stage_state(doc, commit=commit), commit=commit)
    assert owner.pinned(handle)
    handle.release(doc, Release("paused", 500))
    assert not owner.pinned(handle)
    owner.unlatch()
    owner.close()
    assert not owner.path.exists()
