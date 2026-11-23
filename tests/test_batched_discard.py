"""Batched Discard chunks, and chunks that mix Drop and Discard, in ``syncfs`` mode (plan WORKFLOW_LOCAL_COMMANDS phase
L3c, §5 amendment 14; amendment 9's merged phase order).

A local ``delete`` serves each contiguous run of at least eight eligible jobs as one batch: post (C0, A), claim (B),
run-log lines (C1; a Discard job writes no state), the Drop jobs' state replaces, request unlinks and release moves
(C2, D1, D2), then the Discard jobs' trash moves (D′), and only after that seal their tree unlinks and request unlinks
(D″). An exchange-origin delete is evicted after the claim and served serially, as is every job of a dependency
component; delete's waiting-parent check holds at admission. The failure table, interrupts and the crash matrix are in
``test_batched_discard_failures``.
"""

import dataclasses
import os
import re
import shutil
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

import test_batched_release
import v3_helpers as v3
from httk.workflow import Workspace, _kernel, removal
from httk.workflow._state import Release, StateDoc
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command
from kernel._journal import Journal, record
from test_batched_release import Batches, _jobs, _normalizer, _requests_left, _where
from test_cli_job_request import _context, _job, _new_workspace
from test_local_commands import _at, _held, _identity, _parented  # noqa: F401  (_identity: an autouse fixture)

#: The fixtures of the L3b tests, shared.
syncfs = test_batched_release.syncfs
batches = test_batched_release.batches

#: The CLI's removal, before any test wraps it.
_REMOVE_JOBS = job_cli._remove_jobs


def _terminal(tmp_path: Path, workspace: Workspace, prefix: str, count: int) -> list[_kernel.JobRef]:
    return [_at(workspace, ref, "cancelled") for ref in _jobs(tmp_path, workspace, prefix, count)]


def _delete(workspace: Workspace, refs: list[_kernel.JobRef]) -> list[removal.RequestOutcome]:
    report = removal.remove_jobs(workspace, refs)
    assert all(outcome.request is not None for outcome in report.outcomes)
    return [outcome.request for outcome in report.outcomes if outcome.request is not None]


def _exchanged(tmp_path: Path, workspace: Workspace, tag: str, exchange_name: str) -> _kernel.JobRef:
    """A cancelled job of exchange origin, indexed under *exchange_name*."""

    ref = _job(tmp_path, workspace, tag)
    with v3.cli_owner(workspace) as owner:
        claimed = _kernel.claim_exchange_name(
            workspace, owner, exchange_name, ref.job_id, ref.placement or PurePosixPath(), nonce="n1"
        )
        assert claimed is _kernel.ExchangeClaim.WON
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial")
        doc = dataclasses.replace(doc, origin="exchange", exchange_name=exchange_name)
        return owned.release(doc, Release("cancelled", 500))


def _scratch(workspace: Workspace) -> list[str]:
    tmp = workspace.control / "tmp"
    return sorted(os.listdir(tmp)) if tmp.is_dir() else []


# -- the opt-in --------------------------------------------------------------------------------------------------------


def test_a_chunk_of_deletes_is_one_batch(tmp_path: Path, syncfs: None, batches: Batches) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "d", 10)
    outcomes = _delete(workspace, refs)
    assert [(outcome.status, outcome.explanation, outcome.state) for outcome in outcomes] == [
        ("applied", "job deleted", None)
    ] * 10
    assert batches.runs == [[ref.job_id for ref in refs]] and batches.serial == []
    assert all(_where(workspace, ref.job_id) is None for ref in refs)
    assert _requests_left(workspace) == []
    # The owner is gone with its trash scratch and its batched marker.
    assert not list((workspace.control / "owners").iterdir()) and _scratch(workspace) == []


@pytest.mark.parametrize("setting", ["fsync", "below eight"])
def test_deletes_stay_serial_in_fsync_mode_and_below_eight(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch, setting: str
) -> None:
    trashed: list[object] = []
    move_to_trash = _kernel.OwnedJob.move_to_trash

    def recording(self: _kernel.OwnedJob, **kwargs: Any) -> object:
        trashed.append(kwargs.get("commit"))
        return move_to_trash(self, **kwargs)

    monkeypatch.setattr(_kernel.OwnedJob, "move_to_trash", recording)
    workspace, _name = _new_workspace(tmp_path)
    if setting == "fsync":
        workspace.configure_settings({"io.sync": "fsync"})
    refs = _terminal(tmp_path, workspace, "s", 7 if setting == "below eight" else 9)
    assert [outcome.status for outcome in _delete(workspace, refs)] == ["applied"] * len(refs)
    assert batches.runs == [] and batches.serial == [ref.job_id for ref in refs]
    assert trashed == [None] * len(refs)  # the serial discard, fsynced inline


def test_an_exchange_origin_delete_is_evicted_and_served_serially(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "x", 8)
    exchange_name = "11111111-2222-4333-8444-555555555555"
    exchanged = _exchanged(tmp_path, workspace, "xch", exchange_name)
    assert _kernel.exchange_index(workspace, exchange_name) is not None
    evicted: list[str] = []
    serially = removal._Batch._serially

    def recording(self: removal._Batch, member: removal._Member) -> None:
        evicted.append(member.ref.job_id)
        serially(self, member)

    batched_trash: list[str] = []
    move_to_trash = _kernel.OwnedJob.move_to_trash

    def trashing(self: _kernel.OwnedJob, **kwargs: Any) -> object:
        if kwargs.get("commit") is not None:
            batched_trash.append(self.job_id)
        return move_to_trash(self, **kwargs)

    monkeypatch.setattr(removal._Batch, "_serially", recording)
    monkeypatch.setattr(_kernel.OwnedJob, "move_to_trash", trashing)
    outcomes = _delete(workspace, [*refs[:4], exchanged, *refs[4:]])
    assert [outcome.status for outcome in outcomes] == ["applied"] * 9
    assert len(batches.runs) == 1 and len(batches.runs[0]) == 9
    # Claimed with the batch, then evicted after B: the serial discard dropped its exchange index first.
    assert evicted == [exchanged.job_id] and exchanged.job_id not in batched_trash
    assert sorted(batched_trash) == sorted(ref.job_id for ref in refs)
    assert _kernel.exchange_index(workspace, exchange_name) is None
    assert _where(workspace, exchanged.job_id) is None and _requests_left(workspace) == []


def test_the_waiting_parent_check_holds_at_admission(tmp_path: Path, syncfs: None, batches: Batches) -> None:
    # The parent is no job of this command, so its child is batch-eligible; admission evaluates delete's
    # waiting-parent check exactly as the serial apply does: the delete is dropped, the child stays.
    workspace, _name = _new_workspace(tmp_path)
    parent = _job(tmp_path, workspace, "parent")
    child = _at(workspace, _parented(tmp_path, workspace, parent, "child", "project/child"), "failed")
    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, parent)
        assert owned is not None
        join = {"children": [{"job_id": child.job_id, "job_key": child.job_key, "placement_hint": "project/child"}]}
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial").updated(join=join)
        owned.release(doc, Release("waiting", 500))
    refs = _terminal(tmp_path, workspace, "w", 8)
    outcomes = _delete(workspace, [*refs[:3], child, *refs[3:]])
    assert batches.runs == [[ref.job_id for ref in [*refs[:3], child, *refs[3:]]]] and batches.serial == []
    assert outcomes[3].status == "refused" and "is waiting on it" in outcomes[3].explanation
    assert _where(workspace, child.job_id) == "failed/p500"
    assert [outcome.status for outcome in outcomes[:3] + outcomes[4:]] == ["applied"] * 8
    assert _requests_left(workspace) == []


# -- the merged phase order (amendment 9) and the call order (amendment 24) --------------------------------------------


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch, syncfs: None) -> Journal:
    return record(monkeypatch, writes=True)


def _phases(journal: Journal, count: int) -> list[int]:
    positions = journal.positions("syncfs")
    assert len(positions) == count, [journal[at] for at in positions]
    return positions


def test_a_delete_chunk_trashes_at_d_prime_and_unlinks_only_after_it(
    tmp_path: Path, journal: Journal, batches: Batches
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "o", 9)
    journal.clear()
    assert [outcome.status for outcome in _delete(workspace, refs)] == ["applied"] * 9
    # The prime, then C0, A, B, C1, D′ and D″: no C2, D1 or D2 without a Drop job.
    _prime, _c0, a, b, c1, d1_prime, d2_prime = _phases(journal, 7)
    requests, tmp = f"{workspace.control / 'requests'}/", f"{workspace.control / 'tmp'}/"
    owned = f"{workspace.jobs / 'owned'}/"
    trash_moves = [
        at
        for at, (op, path) in enumerate(journal)
        if op == "rename" and path.split(" -> ")[0].startswith(owned) and path.split(" -> ")[1].startswith(tmp)
    ]
    assert len(trash_moves) == 9 and all(c1 < at < d1_prime for at in trash_moves)
    trashed = {journal[at][1].split(" -> ")[1] for at in trash_moves}

    def inside(path: str) -> bool:
        return any(path == tree or path.startswith(tree + "/") for tree in trashed)

    tree_unlinks = [at for at, (op, path) in enumerate(journal) if op in ("unlink", "rmdir") and inside(path)]
    assert tree_unlinks and all(d1_prime < at < d2_prime for at in tree_unlinks)
    assert len([at for at, (op, path) in enumerate(journal) if op == "rmdir" and path in trashed]) == 9
    request_unlinks = [at for at, (op, path) in enumerate(journal) if op == "unlink" and path.startswith(requests)]
    assert len(request_unlinks) == 9 and all(max(tree_unlinks) < at < d2_prime for at in request_unlinks)
    # Nothing of a job is touched between A and its claim but the claims; no fsync from C0 through D″, except the
    # creation of the trash scratch (durable: it may receive the only copy of a moved job).
    assert [op for op, _path in journal[a + 1 : b] if op != "mkdir"] == ["rename"] * 9  # mkdir: owned/<owner-id>/
    fsyncs = [path for op, path in journal[_c0:d2_prime] if op == "fsync"]
    assert fsyncs == [str(workspace.control / "tmp")]


def test_a_mixed_chunk_runs_the_drop_phases_before_the_trash(
    tmp_path: Path, journal: Journal, batches: Batches
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ready = _jobs(tmp_path, workspace, "r", 4)  # delete of a ready job is a Drop
    cancelled = _terminal(tmp_path, workspace, "c", 5)
    refs = [*ready[:2], *cancelled, *ready[2:]]
    journal.clear()
    outcomes = _delete(workspace, refs)
    assert len(batches.runs) == 1
    statuses = {ref.job_id: outcome.status for ref, outcome in zip(refs, outcomes, strict=True)}
    assert [statuses[ref.job_id] for ref in ready] == ["refused"] * 4
    assert [statuses[ref.job_id] for ref in cancelled] == ["applied"] * 5
    _prime, _c0, _a, b, c1, c2, d1, d2, d1_prime, d2_prime = _phases(journal, 10)
    requests = f"{workspace.control / 'requests'}/"
    owned = f"{workspace.jobs / 'owned'}/"

    def unlinked(job_id: str) -> list[int]:
        return [
            at for at, (op, path) in enumerate(journal) if op == "unlink" and path.startswith(f"{requests}{job_id}.")
        ]

    # The Drop jobs' requests at D1; a Discard job's request is touched only after D′.
    assert all(len(unlinked(ref.job_id)) == 1 and c2 < unlinked(ref.job_id)[0] < d1 for ref in ready)
    assert all(len(unlinked(ref.job_id)) == 1 and d1_prime < unlinked(ref.job_id)[0] < d2_prime for ref in cancelled)
    # The Drop jobs' state replaces at C2 and release moves at D2; the trash moves after D2, before D′.
    replaced = [at for at, (op, path) in enumerate(journal) if op == "replace" and path.endswith("/state.json")]
    assert len(replaced) == 4 and all(c1 < at < c2 for at in replaced)
    moves = [at for at, (op, path) in enumerate(journal) if op == "rename" and path.startswith(owned)]
    released = [at for at in moves if "/ready/" in journal[at][1].split(" -> ")[1]]
    scratch = f"{workspace.control / 'tmp'}/"
    trash = [at for at in moves if journal[at][1].split(" -> ")[1].startswith(scratch)]
    assert len(released) == 4 and all(d1 < at < d2 for at in released)
    assert len(trash) == 5 and all(d2 < at < d1_prime for at in trash)
    # C1 wrote every job's run-log lines, but a state temporary only for the Drop jobs.
    state_temporaries = [at for at, (op, path) in enumerate(journal) if op == "create" and ".state.json." in path]
    assert len(state_temporaries) == 4 and all(b < at < c1 for at in state_temporaries)
    for ref in ready:
        assert _where(workspace, ref.job_id) == "ready/p500"
    assert _requests_left(workspace) == []


def test_d_double_prime_unlinks_every_request_of_a_deleted_job(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch
) -> None:
    # As OwnedJob.discard: every request file of the deleted job, also one posted while the batch held it.
    workspace, _name = _new_workspace(tmp_path)
    refs = _terminal(tmp_path, workspace, "l", 8)
    trash = removal._Batch._trash
    late: list[Path] = []

    def posting_meanwhile(self: removal._Batch, members: list[removal._Member]) -> None:
        late.append(
            removal._requests.post(
                workspace, action="pause", job_id=refs[2].job_id, placement="project/l2", operator="t", reason="late"
            )
        )
        trash(self, members)

    monkeypatch.setattr(removal._Batch, "_trash", posting_meanwhile)
    assert [outcome.status for outcome in _delete(workspace, refs)] == ["applied"] * 8
    assert late and not late[0].exists() and _requests_left(workspace) == []


# -- parity with the serial path ---------------------------------------------------------------------------------------


def _scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any, *, batched: bool) -> dict[str, Any]:
    """A mixed delete chunk through ``job delete --yes``: cancelled and failed jobs (Discard), ready ones (Drop), one
    held by another owner (queued), an exchange-origin one (evicted, serial), a parent with its child (a component,
    serial), an unknown one, a duplicate and a planted ``logs``; served with or without batching."""

    monkeypatch.setattr(removal, "_BATCH_MIN", 8 if batched else 10**6)
    workspace, name = _new_workspace(tmp_path)
    jobs: dict[str, _kernel.JobRef] = {}
    for label in ("c1", "c2", "c3", "r1", "f1", "planted", "held", "unknown", "c4", "r2"):
        jobs[label] = _job(tmp_path, workspace, label)
    jobs["xch"] = _exchanged(tmp_path, workspace, "xch", "11111111-2222-4333-8444-555555555555")
    jobs["parent"] = _at(workspace, _job(tmp_path, workspace, "parent"), "cancelled")
    jobs["child"] = _parented(tmp_path, workspace, jobs["parent"], "child", "project/child")
    for label in ("s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"):
        jobs[label] = _job(tmp_path, workspace, label)
    for label, state in {"f1": "failed", "child": "cancelled", "planted": "cancelled", "held": "cancelled"}.items():
        jobs[label] = _at(workspace, jobs[label], state)
    for label in ("c1", "c2", "c3", "c4", "unknown", *(f"s{number}" for number in range(1, 9))):
        jobs[label] = _at(workspace, jobs[label], "cancelled")
    (jobs["planted"].path / "logs").write_text("planted")
    labels = {ref.job_id: label for label, ref in jobs.items()}
    order = [*jobs, "c1"]  # c1 named twice
    remove_jobs = _REMOVE_JOBS

    def resolved_then_gone(*args: Any, **kwargs: Any) -> object:
        shutil.rmtree(v3.find(workspace, jobs["unknown"].job_id).path)  # resolved, then gone before its request
        return remove_jobs(*args, **kwargs)

    monkeypatch.setattr(job_cli, "_remove_jobs", resolved_then_gone)
    capsys.readouterr()
    with _held(workspace, jobs["held"]):
        status = command(
            ["job", "delete", "--yes", "--workspace", name, *(jobs[label].job_id for label in order)],
            _context(tmp_path),
        )
    normalize = _normalizer(labels)
    observed: dict[str, Any] = {
        "status": status,
        "printed": [normalize(line) for line in capsys.readouterr().out.splitlines()],
        "requests": [normalize(name) for name in _requests_left(workspace)],
        "where": {label: _where(workspace, ref.job_id) for label, ref in jobs.items()},
        "exchange": _kernel.exchange_index(workspace, "11111111-2222-4333-8444-555555555555"),
        "scratch": _scratch(workspace),
        "owners": sorted(os.listdir(workspace.control / "owners")),
    }
    return observed


def test_job_delete_has_the_serial_outcomes_and_prints_what_the_serial_code_prints(
    tmp_path: Path, syncfs: None, monkeypatch: pytest.MonkeyPatch, capsys: Any, batches: Batches
) -> None:
    serial = _scenario(tmp_path / "serial", monkeypatch, capsys, batched=False)
    assert batches.runs == []
    batched = _scenario(tmp_path / "batched", monkeypatch, capsys, batched=True)
    # c1..c3, r1, f1, planted (evicted after B), held (queued at the claim), then c4, r2 and xch (evicted after B);
    # the unknown one is not posted; then s1..s8 after the parent and its child.
    assert [len(run) for run in batches.runs] == [10, 8]
    assert batched == serial
    printed = "\n".join(serial["printed"])
    assert re.search(r"r1--\S*\tready\trefused\tonly terminal or paused jobs are deleted, not ready", printed)
    assert re.search(r"held--\S*\towned\tqueued\tanother owner holds the job", printed)
    assert re.search(r"unknown--\S*\tcancelled\tuncertain\tthe job was not found", printed)
    assert serial["status"] == 1 and serial["exchange"] is None
    assert serial["where"]["held"] == "cancelled/p500" and serial["where"]["planted"] is None
    assert serial["where"]["r1"] == "ready/p500" and serial["where"]["child"] is None
    assert len(serial["requests"]) == 1 and serial["requests"][0].startswith("held.")  # the queued one
    assert serial["scratch"] == [] and serial["owners"] == []
