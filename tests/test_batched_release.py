"""Batched Release and Drop chunks in ``syncfs`` mode (plan WORKFLOW_LOCAL_COMMANDS phase L3b, §5 amendment 14).

A local ``job request`` of a Release or Drop action serves each contiguous run of at least eight eligible jobs as one
batch: post (C0, A), claim (B), run-log lines and state temporaries (C1), state replaces (C2), request unlinks (D1) and
release moves (D2), each phase made durable by one commit point. Everything else stays on the serial path, in input
order: fsync mode, short runs, Discard, every other action and every ineligible job. Here: the opt-in and the
eligibility, outcome parity with the serial path over a mixed chunk, and the recorded call order per phase. The failure
table, interrupts and the crash matrix are in ``test_batched_release_failures``.
"""

import hashlib
import json
import os
import re
import shutil
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as v3
from httk.workflow import Workspace, _fs, _kernel, _requests, removal
from httk.workflow._state import read_state_unowned
from httk.workflow.workflow_cli import command
from kernel._journal import Journal, record
from test_cli_job_request import _context, _job, _new_workspace
from test_local_commands import _at, _held, _identity, _parented, _pinned  # noqa: F401  (_identity: an autouse fixture)

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@pytest.fixture
def syncfs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The protocol predicate holds on the test's filesystem (host-independent); ``syncfs`` is not really called."""

    monkeypatch.setattr(_fs, "_resolve_sync", lambda path, setting: ("syncfs", "syncfs (ext4, Linux 6.1)", "ext4"))
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: None)
    monkeypatch.setattr(_fs, "_syncfs_works", {})


@dataclass
class Batches:
    """The runs served as batches (their job ids, in order) and the jobs served serially, in serving order."""

    runs: list[list[str]]
    serial: list[str]
    order: list[str]


@pytest.fixture
def batches(monkeypatch: pytest.MonkeyPatch) -> Batches:
    seen = Batches([], [], [])
    init, serve_one = removal._Batch.__init__, removal._serve_one

    def initializing(self: removal._Batch, *args: object, **kwargs: object) -> None:
        init(self, *args, **kwargs)  # type: ignore[arg-type]
        run = [member.ref.job_id for member in self.members]
        seen.runs.append(run)
        seen.order.extend(f"batch:{job_id}" for job_id in run)

    def serving(workspace: object, owner: object, ref: _kernel.JobRef, *args: object, **kwargs: object) -> object:
        seen.serial.append(ref.job_id)
        seen.order.append(f"serial:{ref.job_id}")
        return serve_one(workspace, owner, ref, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal._Batch, "__init__", initializing)
    monkeypatch.setattr(removal, "_serve_one", serving)
    return seen


def _jobs(tmp_path: Path, workspace: Workspace, prefix: str, count: int) -> list[_kernel.JobRef]:
    return [_job(tmp_path, workspace, f"{prefix}{number}") for number in range(count)]


def _cancel(
    workspace: Workspace, refs: Sequence[_kernel.JobRef], action: str = "cancel"
) -> list[removal.RequestOutcome]:
    with v3.cli_owner(workspace) as owner:
        return removal.request_jobs(workspace, owner, refs, action, "batch")


def _requests_left(workspace: Workspace) -> list[str]:
    directory = workspace.control / "requests"
    return sorted(name for name in os.listdir(directory) if not name.startswith(".")) if directory.is_dir() else []


def _where(workspace: Workspace, job_id: str) -> str | None:
    ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
    return None if ref is None else f"{ref.state}/p{ref.priority}"


# -- the opt-in --------------------------------------------------------------------------------------------------------


def test_a_chunk_of_eligible_jobs_is_one_batch(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "b", 10)
    outcomes = _cancel(workspace, refs)
    assert [outcome.status for outcome in outcomes] == ["applied"] * 10
    assert [outcome.explanation for outcome in outcomes] == ["cancel applied"] * 10
    assert batches.runs == [[ref.job_id for ref in refs]] and batches.serial == []
    assert all(_where(workspace, ref.job_id) == "cancelled/p500" for ref in refs)
    assert _requests_left(workspace) == []
    # The owner is gone, its batched marker with it; the emptied placements were pruned.
    assert not list((workspace.control / "owners").iterdir())
    assert not any((workspace.jobs / "ready").iterdir())


@pytest.mark.parametrize("setting", ["fsync", "below eight", "not durable", "prime unsupported", "delete"])
def test_everything_else_stays_serial(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch, setting: str
) -> None:
    obliged: list[object] = []
    original = _kernel.Owner.obliged

    def recording(self: _kernel.Owner, *args: object, **kwargs: object) -> object:
        obliged.append(args)
        return original(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel.Owner, "obliged", recording)
    if setting == "not durable":
        workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    else:
        workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "s", 7 if setting == "below eight" else 9)
    action = "cancel"
    if setting == "fsync":
        workspace.configure_settings({"io.sync": "fsync"})
    elif setting == "prime unsupported":
        # The prime finds syncfs not supported there: that filesystem is fsync mode, and the chunk goes serial.
        monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: (_ for _ in ()).throw(OSError(38, "no syncfs")))
    elif setting == "delete":
        refs = [_at(workspace, ref, "cancelled") for ref in refs]
        action = "delete"
    outcomes = _cancel(workspace, refs, action)
    assert [outcome.status for outcome in outcomes] == ["applied"] * len(refs)
    assert batches.runs == [] and batches.serial == [ref.job_id for ref in refs]
    assert obliged == []  # the serial path registers no obligation


def test_a_dependency_component_is_served_serially_in_input_order(
    tmp_path: Path, syncfs: None, batches: Batches
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first = _jobs(tmp_path, workspace, "f", 8)
    parent = _job(tmp_path, workspace, "parent")
    child = _parented(tmp_path, workspace, parent, "child", "project/child")
    outsider = _job(tmp_path, workspace, "outsider")
    loner = _parented(tmp_path, workspace, outsider, "loner", "project/loner")  # its parent is no job of this command
    second = _jobs(tmp_path, workspace, "g", 7)
    refs = [*first, parent, child, loner, *second]
    outcomes = _cancel(workspace, refs)
    assert [outcome.status for outcome in outcomes] == ["applied"] * len(refs)
    # The parent and its child (one component) break the run; the loner's edge does not count, so it and the seven
    # after it make a second run.
    assert batches.runs == [[ref.job_id for ref in first], [loner.job_id, *(ref.job_id for ref in second)]]
    assert batches.order == [
        *(f"batch:{ref.job_id}" for ref in first),
        f"serial:{parent.job_id}",
        f"serial:{child.job_id}",
        f"batch:{loner.job_id}",
        *(f"batch:{ref.job_id}" for ref in second),
    ]


def test_a_component_counts_across_chunks(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(removal, "_CHUNK_JOBS", 9)
    workspace, _name = _new_workspace(tmp_path)
    first = _jobs(tmp_path, workspace, "f", 8)
    parent = _job(tmp_path, workspace, "parent")
    second = _jobs(tmp_path, workspace, "g", 8)
    child = _parented(tmp_path, workspace, parent, "child", "project/child")
    refs = [*first, parent, *second, child]  # the parent in the first chunk, its child in the second
    assert [outcome.status for outcome in _cancel(workspace, refs)] == ["applied"] * len(refs)
    assert batches.runs == [[ref.job_id for ref in first], [ref.job_id for ref in second]]
    assert batches.serial == [parent.job_id, child.job_id]


def test_a_backlog_or_a_short_run_breaks_the_batch(tmp_path: Path, syncfs: None, batches: Batches) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first = _jobs(tmp_path, workspace, "f", 8)
    backlog = _job(tmp_path, workspace, "backlog")
    _requests.post(
        workspace,
        action="set_priority",
        job_id=backlog.job_id,
        placement=backlog.placement or "",
        operator="t",
        reason="older",
        priority=700,
    )
    short = _jobs(tmp_path, workspace, "s", 3)
    refs = [*first, backlog, *short]
    outcomes = _cancel(workspace, refs)
    assert [outcome.status for outcome in outcomes] == ["applied"] * len(refs)
    assert batches.runs == [[ref.job_id for ref in first]]
    assert batches.serial == [backlog.job_id, backlog.job_id, *(ref.job_id for ref in short)]
    assert _where(workspace, backlog.job_id) == "cancelled/p700"  # its older request served first, as serially


def test_post_claim_checks_evict_to_the_serial_code_while_held(tmp_path: Path, syncfs: None, batches: Batches) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "e", 10)
    (refs[2].path / "logs").write_text("planted")  # amendment 26: served serially, which replaces it
    reconciling = refs[5]
    (reconciling.path / "state.json").write_bytes(b"")  # unreadable state.json: today's code decides
    outcomes = _cancel(workspace, refs)
    assert batches.runs == [[ref.job_id for ref in refs]]
    assert batches.serial == []  # evicted after the claim, never located or claimed again
    assert [outcome.status for outcome in outcomes] == [*["applied"] * 5, "uncertain", *["applied"] * 4]
    planted = _kernel.locate(workspace, refs[2].job_id, placement_hint=None, exhaustive=True)
    assert planted is not None and (planted.path / "logs").is_dir()


# -- parity with the serial path ---------------------------------------------------------------------------------------


def _scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, batched: bool) -> dict[str, Any]:
    """A mixed chunk: ready, cancelled (Drop), held by another owner, moving mid-command, unknown, duplicated, with a
    backlog, a parent with its child, and a planted ``logs``; served with or without batching."""

    if not batched:
        monkeypatch.setattr(removal, "_BATCH_MIN", 10**6)
    workspace, _name = _new_workspace(tmp_path)
    jobs: dict[str, _kernel.JobRef] = {}
    for label in ("r1", "r2", "r3", "planted", "r4", "r5", "c1", "c2", "held", "moving", "unknown", "backlog"):
        jobs[label] = _job(tmp_path, workspace, label)
    jobs["parent"] = _job(tmp_path, workspace, "parent")
    jobs["child"] = _parented(tmp_path, workspace, jobs["parent"], "child", "project/child")
    for label in ("s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"):
        jobs[label] = _job(tmp_path, workspace, label)
    for label in ("c1", "c2"):
        jobs[label] = _at(workspace, jobs[label], "cancelled")
    (jobs["planted"].path / "logs").write_text("planted")
    _requests.post(
        workspace,
        action="set_priority",
        job_id=jobs["backlog"].job_id,
        placement=jobs["backlog"].placement or "",
        operator="t",
        reason="older",
        priority=650,
    )
    unknown = jobs["unknown"]
    shutil.rmtree(unknown.path)
    labels = {ref.job_id: label for label, ref in jobs.items()}
    order = [*jobs, "r1"]  # r1 named twice: its second request goes in a second chunk
    snapshot = removal._snapshot
    moved: list[bool] = []

    def moving_after_the_post(*args: object, **kwargs: object) -> object:
        if not moved:
            moved.append(True)  # a manager moves it after the post, before any claim
            _at(workspace, v3.find(workspace, jobs["moving"].job_id), "ready", 400)
        return snapshot(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "_snapshot", moving_after_the_post)
    with _held(workspace, jobs["held"]):
        outcomes = _cancel(workspace, [jobs[label] for label in order])
    normalize = _normalizer(labels)
    observed: dict[str, Any] = {
        "outcomes": [
            (label, outcome.status, normalize(outcome.explanation), outcome.state)
            for label, outcome in zip(order, outcomes, strict=True)
        ],
        "requests": [normalize(name) for name in _requests_left(workspace)],
    }
    for label, ref in jobs.items():
        found = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
        if found is None:
            observed[label] = None
            continue
        doc, _damage = read_state_unowned(found.path / "state.json")
        runlog = found.path / "logs" / "runlog.jsonl"
        lines = [json.loads(line) for line in runlog.read_text().splitlines()] if runlog.is_file() else []
        observed[label] = {
            "where": f"{found.state}/p{found.priority}",
            "runlog": [
                normalize(json.dumps({k: v for k, v in line.items() if k not in ("at", "owner_id")})) for line in lines
            ],
            "history": None if doc is None else [entry.get("event") for entry in doc.history_tail],
            "applied": None if doc is None else len(doc.applied_requests),
            "release_to": None
            if doc is None or doc.release_to is None
            else (doc.release_to.state, doc.release_to.priority),
            "files": sorted(name for name in os.listdir(found.path) if name.startswith(".state")),
        }
    return observed


def _normalizer(labels: dict[str, str]) -> Callable[[str], str]:
    def normalize(text: str) -> str:
        return _UUID.sub(lambda match: labels.get(match.group(), "R"), text)

    return normalize


def test_a_mixed_chunk_has_the_serial_outcomes(
    tmp_path: Path, syncfs: None, monkeypatch: pytest.MonkeyPatch, batches: Batches
) -> None:
    serial = _scenario(tmp_path / "serial", monkeypatch, batched=False)
    assert batches.runs == []
    batches.serial.clear()
    monkeypatch.setattr(removal, "_BATCH_MIN", 8)
    batched = _scenario(tmp_path / "batched", monkeypatch, batched=True)
    # r1..r5 with the planted one (evicted after the claim), c1, c2, held and moving; then s1..s8.
    assert [len(run) for run in batches.runs] == [10, 8]
    assert batched == serial
    outcomes = {label: (status, explanation, state) for label, status, explanation, state in serial["outcomes"]}
    assert outcomes["r1"] == ("refused", "the job is already cancelled", "cancelled")  # its second request
    assert outcomes["planted"] == ("applied", "cancel applied", "cancelled")
    assert outcomes["c1"] == ("refused", "the job is already cancelled", "cancelled")
    assert outcomes["held"][0] == "queued" and outcomes["unknown"] == ("uncertain", "the job was not found", None)
    assert outcomes["moving"][0] == "applied" and serial["moving"]["where"] == "cancelled/p400"
    assert serial["backlog"]["where"] == "cancelled/p650"
    assert outcomes["parent"][0] == outcomes["child"][0] == "applied"


def test_job_request_cancel_prints_what_the_serial_code_prints(
    tmp_path: Path, syncfs: None, monkeypatch: pytest.MonkeyPatch, capsys, batches: Batches
) -> None:
    printed: list[list[str]] = []
    for batched in (False, True):
        monkeypatch.setattr(removal, "_BATCH_MIN", 8 if batched else 10**6)
        workspace, name = _new_workspace(tmp_path / str(batched))
        refs = _jobs(tmp_path / str(batched), workspace, "p", 9)
        refs[4] = _at(workspace, refs[4], "cancelled")
        labels = {ref.job_id: f"j{number}" for number, ref in enumerate(refs)}
        status = command(
            ["job", "request", "cancel", "--workspace", name, "--reason", "r", *(ref.job_id for ref in refs)],
            _context(tmp_path),
        )
        normalize = _normalizer(labels)
        printed.append([str(status), *(normalize(line) for line in capsys.readouterr().out.splitlines())])
    assert printed[0] == printed[1]
    assert len(batches.runs) == 1 and len(batches.runs[0]) == 9


# -- the call order per phase (amendment 24) ---------------------------------------------------------------------------


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch, syncfs: None) -> Journal:
    return record(monkeypatch)


def test_each_phase_is_durable_at_its_one_syncfs(tmp_path: Path, journal: Journal, batches: Batches) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "o", 9)
    (refs[3].path / "logs").write_text("planted")  # evicted after B: its serial finish comes after commit B
    journal.clear()
    assert [outcome.status for outcome in _cancel(workspace, refs)] == ["applied"] * 9
    assert batches.runs == [[ref.job_id for ref in refs]]
    syncfs = journal.positions("syncfs")
    # The prime, then C0, A, B, C1, C2, D1, D2: one syncfs per commit point on the workspace's one filesystem.
    assert len(syncfs) == 8 and {journal[at][1] for at in syncfs} == {str(workspace.control / "requests")}
    prime, c0, a, b, c1, c2, d1, d2 = syncfs
    requests, owners = workspace.control / "requests", workspace.control / "owners"
    owned = workspace.jobs / "owned"

    def phase(start: int, end: int) -> list[tuple[str, str]]:
        return journal[start + 1 : end]

    # Before the prime, only the owner's registration; between the prime and C0, only its batched marker.
    assert all(path.startswith(str(owners)) for _op, path in journal[:prime])
    assert [op for op, _path in phase(prime, c0)] == ["fsync", "replace", "fsync"]
    assert all(path.startswith(str(owners)) for _op, path in phase(prime, c0))
    # A: the request renames.
    assert [op for op, _path in phase(c0, a)] == ["replace"] * 9
    assert all(path.split(" -> ")[1].startswith(f"{requests}/") for _op, path in phase(c0, a))
    # B: the claims.
    assert [op for op, _path in phase(a, b)] == ["rename"] * 9
    assert all(path.split(" -> ")[1].startswith(f"{owned}/") for _op, path in phase(a, b))
    # After B, before C1: only the evicted job's serial finish (fsynced inline, the pinned order); C1 itself writes
    # appends and temporaries, which the journal does not record.
    evicted = phase(b, c1)
    assert [op for op, _path in evicted if op != "fsync"] == ["unlink", "replace", "unlink", "rename"]
    assert refs[3].job_id in evicted[0][1] and evicted[0][1].endswith("/logs")  # the planted logs, replaced
    # C2: the state replaces; D1: the request unlinks; D2: the release moves. No fsync from C0 to D2.
    assert [op for op, _path in phase(c1, c2)] == ["replace"] * 8
    assert all(path.endswith("/state.json") for _op, path in phase(c1, c2))
    assert [op for op, _path in phase(c2, d1)] == ["unlink"] * 8
    assert all(path.startswith(f"{requests}/") for _op, path in phase(c2, d1))
    assert [op for op, _path in phase(d1, d2)] == ["rename"] * 8
    assert all(path.split(" -> ")[1].startswith(f"{workspace.jobs}/cancelled/") for _op, path in phase(d1, d2))
    assert not [entry for entry in journal[c0:b] + journal[c1:d2] if entry[0] == "fsync"]
    # After D2: the emptied placements pruned, the owner's records removed; nothing else.
    tail = journal[d2 + 1 :]
    assert {op for op, _path in tail} <= {"rmdir", "unlink", "fsync"}
    assert all(op != "fsync" or path.startswith(str(owners)) for op, path in tail)


_TOKEN = re.compile(r"(?<![a-z2-7])[a-z2-7]{16}(?![a-z2-7])")
_OWNER = re.compile(r"[0-9a-f]{32}")


def _normalized_journal(journal: Journal, root: Path) -> list[str]:
    """The journal with the workspace root dropped and each UUID, owner id and token labelled by first appearance."""

    labels: dict[str, str] = {}

    def label(prefix: str) -> Callable[[re.Match[str]], str]:
        return lambda match: labels.setdefault(match.group(), f"{prefix}{len(labels)}")

    entries = []
    for op, path in journal:
        text = _UUID.sub(label("U"), path.replace(str(root), ""))
        entries.append(f"{op} {_TOKEN.sub('T', _OWNER.sub(label('O'), text))}")
    return entries


def _journal_pin(journal: Journal, root: Path) -> dict[str, object]:
    entries = _normalized_journal(journal, root)
    return {"entries": len(entries), "sha256": hashlib.sha256("\n".join(entries).encode()).hexdigest()}


#: The normalized journals of HEAD 41d1239 (before L3b), recorded by running these two tests against its sources
#: (``PYTHONPATH`` set to its ``src``) with ``HTTK_TEST_DUMP_PINS``: nine cancels under a forced ``io.sync=fsync``, and
#: nine cancels in syncfs mode with no qualifying run (a backlog in the middle leaves two runs of four).
_HEAD_JOURNALS: dict[str, dict[str, object]] = {
    "forced-fsync": {"entries": 211, "sha256": "d3b891e7ec259d0251e1c5e8ebe6beb5ffd02624d11229071cc9224787430d2b"},
    "no-qualifying-run": {"entries": 228, "sha256": "f808095cf15ff2f8e3075b0c7b4ba7a7fa13811107857d81d81bd777ededf1c6"},
}


def test_forced_fsync_keeps_the_head_sequence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, syncfs: None) -> None:
    # io.sync=fsync: the chunk goes serial, exactly as at HEAD (every rename, unlink, fsync, in order), no syncfs.
    journal = record(monkeypatch)
    workspace, _name = _new_workspace(tmp_path)
    workspace.configure_settings({"io.sync": "fsync"})
    refs = _jobs(tmp_path, workspace, "q", 9)
    journal.clear()
    assert [outcome.status for outcome in _cancel(workspace, refs)] == ["applied"] * 9
    assert journal.positions("syncfs") == []
    _pinned("l3b-forced-fsync", _journal_pin(journal, workspace.root), _HEAD_JOURNALS["forced-fsync"])


def test_a_chunk_without_a_qualifying_run_keeps_the_head_sequence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, syncfs: None
) -> None:
    # Eight candidates and more, but no run of eight: posted and served exactly as at HEAD (E5).
    journal = record(monkeypatch)
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "n", 9)
    _requests.post(
        workspace,
        action="set_priority",
        job_id=refs[4].job_id,
        placement=refs[4].placement or "",
        operator="t",
        reason="older",
        priority=700,
        request_id="00000000-0000-4000-8000-000000000001",
        created_at="2026-01-01T00:00:00+00:00",
    )
    journal.clear()
    assert [outcome.status for outcome in _cancel(workspace, refs)] == ["applied"] * 9
    assert journal.positions("syncfs") == []
    _pinned("l3b-no-qualifying-run", _journal_pin(journal, workspace.root), _HEAD_JOURNALS["no-qualifying-run"])


def test_a_chunk_without_a_qualifying_run_never_batches_its_posts(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch
) -> None:
    served: list[object] = []
    monkeypatch.setattr(removal._Batching, "serve", lambda *args, **kwargs: served.append(args))
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "k", 12)
    for ref in (refs[3], refs[7]):  # runs of three, three and four
        _requests.post(
            workspace,
            action="set_priority",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="t",
            reason="older",
            priority=700,
        )
    assert [outcome.status for outcome in _cancel(workspace, refs)] == ["applied"] * 12
    assert served == [] and batches.runs == []


def test_the_candidates_after_preparation_must_still_make_a_run(
    tmp_path: Path, syncfs: None, batches: Batches, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The pre-post look admits eight jobs; one vanishes before it is prepared: seven candidates, no collector, no
    # batched post (Opus F6).
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "v", 8)
    considers, entered = removal._Batching.considers, []
    admitted: list[object] = []

    def considering(self: removal._Batching, chunk: Sequence[tuple[str, object]]) -> object:
        existing = considers(self, chunk)
        admitted.append(existing)
        shutil.rmtree(refs[3].path)  # gone before its preparation
        return existing

    monkeypatch.setattr(removal._Batching, "considers", considering)
    monkeypatch.setattr(removal._Batching, "_enter", lambda *args: entered.append(args))
    outcomes = _cancel(workspace, refs)
    assert admitted and admitted[0] is not None and entered == [] and batches.runs == []
    assert [outcome.status for outcome in outcomes] == ["applied"] * 3 + ["uncertain"] + ["applied"] * 4
    assert outcomes[3].explanation == "the job was not found"


def test_data_is_durable_before_what_publishes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, syncfs: None
) -> None:
    # The C0 edge (each request's data before its rename) and the C1 edge (the run-log lines and the state temporary
    # before the state replace), from the recorded writes, each on its device.
    journal = record(monkeypatch, writes=True)
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "w", 9)
    journal.clear()
    assert [outcome.status for outcome in _cancel(workspace, refs)] == ["applied"] * 9
    prime, c0, a, b, c1, c2, d1, d2 = journal.positions("syncfs")
    requests = f"{workspace.control / 'requests'}/"

    def at(operation: str, test: Callable[[str], bool]) -> list[int]:
        return [index for index, (op, path) in enumerate(journal) if op == operation and test(path)]

    request_data = at("create", lambda path: path.startswith(requests)) + at(
        "write", lambda path: path.startswith(requests)
    )
    assert len(request_data) == 18 and all(prime < index < c0 for index in request_data)
    published = at("replace", lambda path: path.split(" -> ")[1].startswith(requests))
    assert len(published) == 9 and all(c0 < index < a for index in published)
    log_lines = at("write", lambda path: path.endswith("/logs/runlog.jsonl"))
    state_data = at("create", lambda path: ".state.json." in path) + at("write", lambda path: ".state.json." in path)
    logs_created = at("mkdir", lambda path: path.endswith("/logs"))
    assert len(log_lines) == 27 and len(state_data) == 18 and len(logs_created) == 9
    assert all(b < index < c1 for index in log_lines + state_data + logs_created)
    replaced = at("replace", lambda path: path.endswith("/state.json"))
    assert len(replaced) == 9 and all(c1 < index < c2 for index in replaced)
    device = os.lstat(workspace.root).st_dev
    assert set(journal.devices.values()) == {device}
    assert d1 < d2


def test_the_marker_is_durable_before_the_first_batched_mutation(
    tmp_path: Path, journal: Journal, batches: Batches
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = _jobs(tmp_path, workspace, "m", 8)
    journal.clear()
    _cancel(workspace, refs)
    assert len(batches.runs) == 1
    (marker,) = [at for at, (op, path) in enumerate(journal) if op == "replace" and path.endswith("/.batched")]
    owner_dir = str(Path(journal[marker][1].split(" -> ")[1]).parent)
    sealed = journal.index(("fsync", owner_dir), marker)
    first_post = next(at for at, (op, path) in enumerate(journal) if op == "replace" and "/requests/" in path)
    assert marker < sealed < first_post
