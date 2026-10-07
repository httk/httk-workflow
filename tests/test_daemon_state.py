"""Layout, validation, quotas, durability and two-instance interleavings of the lock-free daemon ledger."""

import errno
import json
import os
import shutil
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import _daemon_state as state_module
from httk.workflow import _txn
from httk.workflow._daemon_protocol import Request, Response, encode_response, request_digest
from httk.workflow._daemon_state import (
    CapacityError,
    ConflictError,
    Decision,
    Ledger,
    LedgerError,
    refusal_response,
)

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"
_REAL_LINK = os.link
_REAL_RENAME = os.rename


class _Crash(BaseException):
    """An injected process death at one protocol step."""


def _request(
    number: int,
    operation: str = "health",
    *,
    profile: str | None = None,
    handle: str | None = None,
) -> Request:
    return Request(
        f"{number:032x}",
        WORKSPACE_ID,
        operation,
        profile=profile,
        handle=handle,
        enrollment_id=ENROLLMENT_ID,
        configuration_digest="0" * 64 if operation == "start_manager" else None,
    )


def _start(number: int) -> Request:
    return _request(number, "start_manager", profile="cpu")


def _response(request: Request, outcome: str, **fields: str) -> Response:
    return Response(
        request.request_id,
        request.workspace_id,
        request.enrollment_id,
        request_digest(request),
        outcome,
        **fields,
    )


@pytest.fixture
def state(tmp_path: Path) -> Path:
    directory = tmp_path / "state"
    directory.mkdir(mode=0o700)
    with Ledger(directory, WORKSPACE_ID, ENROLLMENT_ID, initialize=True):
        pass
    return directory


def _open(state: Path, *, initialize: bool = False, max_records: int = 4096, max_submissions: int = 128) -> Ledger:
    return Ledger(
        state,
        WORKSPACE_ID,
        ENROLLMENT_ID,
        initialize=initialize,
        max_records=max_records,
        max_submissions=max_submissions,
    )


@pytest.fixture
def hook() -> Iterator[list[Callable[[str], None]]]:
    """Install a list of step callbacks as the ``_txn`` hook."""

    callbacks: list[Callable[[str], None]] = []

    def run(step: str) -> None:
        for callback in callbacks:
            callback(step)

    _txn._HOOK = run
    try:
        yield callbacks
    finally:
        _txn._HOOK = None


def _slots(state: Path) -> dict[str, bytes]:
    directory = state / "ledger" / "slots"
    return {path.name: path.read_bytes() for path in directory.iterdir() if not path.name.startswith(".")}


# ---------------------------------------------------------------------------
# Layout and refusals
# ---------------------------------------------------------------------------


def test_initialize_creates_the_directory_layout_exclusively_and_instances_share_it(state: Path) -> None:
    root = state / "ledger"
    assert sorted(path.name for path in root.iterdir()) == [
        "format",
        "handles",
        "observations",
        "prep",
        "req",
        "slots",
    ]
    assert (root / "format").read_bytes() == f"httk-workspace-daemon-ledger 2 {WORKSPACE_ID} {ENROLLMENT_ID}\n".encode()
    assert root.stat().st_mode & 0o777 == 0o700
    assert not [path for path in state.iterdir() if path.name.startswith("birth.")]
    with pytest.raises(FileExistsError):
        _open(state, initialize=True)
    # No lock: any number of instances open the same ledger, each with its own nonce.
    with _open(state) as first, _open(state) as second:
        assert first.nonce != second.nonce and len(first.nonce) == 32
        first.admit(_request(1))
        assert second.lookup_request(f"{1:032x}") is not None
    assert not (state / "daemon.lock").exists() and not (state / "ledger.sqlite3").exists()


@pytest.mark.parametrize("legacy", ["ledger.sqlite3", "daemon.lock"])
@pytest.mark.parametrize("initialize", [False, True])
def test_an_sqlite_ledger_or_its_lock_is_refused_with_the_way_forward(
    tmp_path: Path, legacy: str, initialize: bool
) -> None:
    directory = tmp_path / "state"
    directory.mkdir()
    if not initialize:
        with _open(directory, initialize=True):
            pass
    (directory / legacy).write_bytes(b"SQLite format 3\x00")
    with pytest.raises(LedgerError, match=r"SQLite daemon ledger.*no migration.*daemon init.*httk system reset"):
        _open(directory, initialize=initialize)


def test_missing_wrong_identity_unsupported_and_incomplete_ledgers_fail_closed(tmp_path: Path, state: Path) -> None:
    missing = tmp_path / "missing"
    missing.mkdir()
    with pytest.raises(LedgerError, match="missing"):
        _open(missing)
    with pytest.raises(FileNotFoundError):
        _open(tmp_path / "absent")
    with pytest.raises(LedgerError, match="identity mismatch"):
        Ledger(state, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", ENROLLMENT_ID)
    with pytest.raises(LedgerError, match="identity mismatch"):
        Ledger(state, WORKSPACE_ID, "f" * 32)
    format_file = state / "ledger" / "format"
    original = format_file.read_bytes()
    format_file.write_bytes(original.replace(b" 2 ", b" 1 "))  # the previous (three-file anchor) format
    with pytest.raises(LedgerError, match="unsupported daemon ledger format"):
        _open(state)
    format_file.write_bytes(original)
    (state / "ledger" / "slots").rmdir()
    with pytest.raises(LedgerError, match="slots is missing"):
        _open(state)
    (state / "ledger" / "slots").symlink_to(tmp_path)
    with pytest.raises(LedgerError, match="slots is missing or not a directory"):
        _open(state)
    linked = tmp_path / "linked"
    linked.mkdir()
    (linked / "ledger").symlink_to(state / "ledger")
    with pytest.raises(LedgerError, match="not a directory"):
        _open(linked)


def test_invalid_paths_identities_and_bounds_are_refused(state: Path) -> None:
    for max_records, max_submissions in ((0, 1), (100_001, 1), (8, 0), (8, 9)):
        with pytest.raises(ValueError):
            _open(state, max_records=max_records, max_submissions=max_submissions)
    with pytest.raises(ValueError, match="absolute"):
        Ledger(Path("relative"), WORKSPACE_ID, ENROLLMENT_ID)
    with pytest.raises(ValueError, match="workspace_id"):
        Ledger(state, WORKSPACE_ID.upper(), ENROLLMENT_ID)
    with pytest.raises(ValueError, match="enrollment_id"):
        Ledger(state, WORKSPACE_ID, "F" * 32)
    ledger = _open(state)
    ledger.close()
    ledger.close()
    with pytest.raises(ValueError, match="closed"):
        ledger.lookup_request(f"{1:032x}")


# ---------------------------------------------------------------------------
# Admission, replay, conflicts and per-file validation
# ---------------------------------------------------------------------------


def test_admission_writes_the_anchor_and_replays_by_canonical_bytes(state: Path) -> None:
    with _open(state) as ledger:
        health = ledger.admit(_request(1))
        assert (health.state, health.handle, health.owner) == ("received", None, ledger.nonce)
        anchor = state / "ledger" / "req" / f"{1:032x}"
        assert sorted(path.name for path in anchor.iterdir()) == ["envelope"]
        start = ledger.admit(_start(2))
        assert start.handle is not None and start.decision is None
        assert (state / "ledger" / "handles" / start.handle).read_bytes() == f"{2:032x}\n".encode()
        assert ledger.admit(_start(2)) == start
        with pytest.raises(ConflictError):
            ledger.admit(_request(2))
        with pytest.raises(ValueError, match="wrong workspace"):
            ledger.admit(replace(_request(3), workspace_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
        with pytest.raises(ValueError, match="wrong enrollment"):
            ledger.admit(replace(_request(3), enrollment_id="f" * 32))
        assert ledger.lookup_request(f"{3:032x}") is None
        assert ledger.verify() == 2
        assert sorted(_slots(state)) == ["record.0", "record.1", "start.0"]
        assert not list((state / "ledger" / "prep").iterdir())


def test_a_crash_before_the_handle_entry_is_repaired_on_replay(state: Path, hook: list[Callable[[str], None]]) -> None:
    def crash(step: str) -> None:
        if step == "ledger.anchored":
            raise _Crash

    hook.append(crash)
    with _open(state) as ledger, pytest.raises(_Crash):
        ledger.admit(_start(1))
    hook.clear()
    assert not list((state / "ledger" / "handles").iterdir())
    with _open(state) as other:
        assert other.managers() == []
        entry = other.admit(_start(1))
        assert entry.handle is not None
        assert [row["handle"] for row in other.managers()] == [entry.handle]


def _stored_start(state: Path) -> tuple[Path, Request, str]:
    with _open(state) as ledger:
        request = _start(1)
        entry = ledger.admit(request)
        assert entry.handle is not None
        ledger.decide(request.request_id)
        ledger.record_scheduler(request.request_id, "42", "cluster-1")
        ledger.finish(request.request_id, _response(request, "submitted", handle=entry.handle))
    return state / "ledger" / "req" / request.request_id, request, entry.handle


def _symlinked_response(anchor: Path, _request: Request, _handle: str) -> None:
    (anchor / "response").unlink()
    (anchor / "response").symlink_to(anchor / "envelope")


def _edit_envelope(anchor: Path, **changes: object) -> None:
    document = json.loads((anchor / "envelope").read_bytes())
    document.update(changes)
    (anchor / "envelope").write_bytes(json.dumps(document, sort_keys=True, separators=(",", ":")).encode())


_MUTATIONS: dict[str, Callable[[Path, Request, str], object]] = {
    "noncanonical envelope": lambda anchor, _r, _h: (anchor / "envelope").write_bytes(
        (anchor / "envelope").read_bytes() + b" "
    ),
    "envelope of another id": lambda anchor, _r, _h: (anchor / "envelope").write_bytes(
        (anchor.parent / f"{2:032x}" / "envelope").read_bytes()
    ),
    "missing envelope": lambda anchor, _r, _h: (anchor / "envelope").unlink(),
    "uppercase owner": lambda anchor, _r, _h: _edit_envelope(anchor, owner="A" * 32),
    "short handle": lambda anchor, _r, _h: _edit_envelope(anchor, handle="a" * 31),
    "missing handle": lambda anchor, _r, _h: _edit_envelope(anchor, handle=None),
    "extra envelope field": lambda anchor, _r, _h: _edit_envelope(anchor, extra=1),
    "other envelope version": lambda anchor, _r, _h: _edit_envelope(anchor, format_version=2),
    "oversized envelope": lambda anchor, _r, _h: (anchor / "envelope").write_bytes(b" " * (17 * 1024)),
    "bad decision": lambda anchor, _r, _h: (anchor / "decision").write_bytes(b"submit 01 " + b"a" * 32 + b"\n"),
    "unknown decision": lambda anchor, _r, _h: (anchor / "decision").write_bytes(b"maybe x " + b"a" * 32 + b"\n"),
    "missing decision": lambda anchor, _r, _h: (anchor / "decision").unlink(),
    "refuse decision under a submitted response": lambda anchor, _r, _h: (anchor / "decision").write_bytes(
        b"refuse stale_configuration " + b"a" * 32 + b"\n"
    ),
    "bad scheduler": lambda anchor, _r, _h: (anchor / "scheduler").write_bytes(b"042 cluster-1\n"),
    "submitted without scheduler": lambda anchor, _r, _h: (anchor / "scheduler").unlink(),
    "signed response": lambda anchor, request, handle: (anchor / "response").write_bytes(
        encode_response(replace(_response(request, "submitted", handle=handle), operator_key="k", signature="s"))
    ),
    "foreign response handle": lambda anchor, request, _h: (anchor / "response").write_bytes(
        encode_response(_response(request, "submitted", handle="b" * 32))
    ),
    "refused despite submit": lambda anchor, request, handle: (anchor / "response").write_bytes(
        encode_response(_response(request, "refused", handle=handle, reason="x"))
    ),
    "symlinked response": _symlinked_response,
    "oversized response": lambda anchor, _r, _h: (anchor / "response").write_bytes(b" " * (17 * 1024)),
}


@pytest.mark.parametrize("mutation", sorted(_MUTATIONS))
def test_every_stored_file_is_validated_canonically_on_read(state: Path, mutation: str) -> None:
    anchor, request, handle = _stored_start(state)
    with _open(state) as ledger:
        ledger.admit(_request(2))
        _MUTATIONS[mutation](anchor, request, handle)
        with pytest.raises(LedgerError):
            ledger.lookup_request(request.request_id)
        with pytest.raises(LedgerError):
            ledger.verify()


def test_an_anchor_that_is_a_file_and_unexpected_entries_are_refused(state: Path) -> None:
    with _open(state) as ledger:
        ledger.admit(_request(1))
        (state / "ledger" / "req" / f"{1:032x}" / "extra").write_bytes(b"x")
        with pytest.raises(LedgerError, match="unexpected files"):
            ledger.verify()
        (state / "ledger" / "req" / f"{1:032x}" / "extra").unlink()
        (state / "ledger" / "req" / f"{2:032x}").write_bytes(b"x")
        with pytest.raises(LedgerError, match="not a directory"):
            ledger.lookup_request(f"{2:032x}")
        (state / "ledger" / "req" / f"{2:032x}").unlink()
        (state / "ledger" / "slots" / "other").write_bytes(b"x")
        with pytest.raises(LedgerError, match="slots"):
            ledger.verify()


def test_responses_must_belong_to_the_request_and_follow_its_decision(state: Path) -> None:
    with _open(state) as ledger:
        health = _request(1)
        ledger.admit(health)
        with pytest.raises(ValueError, match="operation mismatch"):
            ledger.finish(health.request_id, _response(health, "busy", reason="capacity"))
        with pytest.raises(ValueError, match="identity"):
            ledger.finish(health.request_id, _response(_request(9), "ready"))
        with pytest.raises(ValueError, match="unsigned"):
            ledger.finish(health.request_id, replace(_response(health, "ready"), signature="s", operator_key="k"))
        stored = ledger.finish(health.request_id, _response(health, "ready"))
        assert stored.state == "done"
        # The first response stays: a second one (another instance's) is answered with the stored one.
        assert ledger.finish(health.request_id, _response(health, "refused", reason="late")).response == stored.response

        start = _start(2)
        handle = ledger.admit(start).handle
        assert handle is not None
        with pytest.raises(ValueError, match="requires a recorded decision"):
            ledger.finish(start.request_id, _response(start, "submitted", handle=handle))
        with pytest.raises(ValueError, match="manager start"):
            ledger.decide(health.request_id)
        entry, won = ledger.decide(start.request_id)
        assert won and entry.state == "submitting"
        with pytest.raises(ValueError, match="scheduler identity"):
            ledger.finish(start.request_id, _response(start, "submitted", handle=handle))
        with pytest.raises(ValueError, match="cannot be refused"):
            ledger.finish(start.request_id, refusal_response(start, handle, "late"))
        with pytest.raises(ValueError, match="job identifier"):
            ledger.record_scheduler(start.request_id, "0", "cluster-1")
        with _open(state) as other, pytest.raises(ValueError, match="submit winner"):
            other.record_scheduler(start.request_id, "42", "cluster-1")
        ledger.record_scheduler(start.request_id, "42", "cluster-1")
        ledger.record_scheduler(start.request_id, "42", "cluster-1")  # a repeat of the same identity is harmless
        (state / "ledger" / "req" / start.request_id / "scheduler").unlink()
        (state / "ledger" / "req" / start.request_id / "scheduler").write_bytes(b"43 cluster-1\n")
        with pytest.raises(LedgerError, match="different scheduler identity"):
            ledger.record_scheduler(start.request_id, "42", "cluster-1")


# ---------------------------------------------------------------------------
# Quotas and slots
# ---------------------------------------------------------------------------


def test_record_and_submission_quotas_are_cumulative_slots(state: Path) -> None:
    with _open(state, max_records=3, max_submissions=1) as ledger:
        ledger.admit(_start(1))
        with pytest.raises(CapacityError, match="submission"):
            ledger.admit(_start(2))
        # The record slot claimed for the refused start was released again.
        assert sorted(_slots(state)) == ["record.0", "start.0"]
        ledger.admit(_request(3))
        ledger.admit(_request(4))
        with pytest.raises(CapacityError, match="record"):
            ledger.admit(_request(5))
        assert ledger.lookup_request(f"{5:032x}") is None
        # Replays need no slot.
        assert ledger.admit(_request(4)).request.request_id == f"{4:032x}"
        for name, content in _slots(state).items():
            assert content.endswith(f" {ledger.nonce}\n".encode()), name


def test_the_slot_scan_starts_at_the_listing_hint_wraps_and_is_busy_only_after_every_slot(
    state: Path, hook: list[Callable[[str], None]]
) -> None:
    slots = state / "ledger" / "slots"
    for k in (1, 3):
        (slots / f"record.{k}").write_bytes(f"{9:032x} {'f' * 32}\n".encode())
    attempts: list[str] = []
    hook.append(attempts.append)
    with _open(state, max_records=4, max_submissions=1) as ledger:
        ledger.admit(_request(1))  # hint 2: record.2
        ledger.admit(_request(2))  # hint 3: record.3 is taken, wrap to record.0
        assert _slots(state)["record.2"].startswith(f"{1:032x} ".encode())
        assert _slots(state)["record.0"].startswith(f"{2:032x} ".encode())
        attempts.clear()
        with pytest.raises(CapacityError, match="record"):
            ledger.admit(_request(3))
        # Every slot was looked up by name; none was free, so no exclusive creation was even attempted.
        assert "ledger.slot" not in attempts


def test_a_slot_taken_between_lookup_and_creation_is_skipped(state: Path, hook: list[Callable[[str], None]]) -> None:
    slots = state / "ledger" / "slots"

    def take(step: str) -> None:
        if step == "ledger.slot" and not (slots / "record.0").exists():
            (slots / "record.0").write_bytes(f"{9:032x} {'f' * 32}\n".encode())

    hook.append(take)
    with _open(state, max_records=2, max_submissions=1) as ledger:
        ledger.admit(_request(1))
    assert _slots(state)["record.1"].startswith(f"{1:032x} ".encode())


# ---------------------------------------------------------------------------
# Durability order
# ---------------------------------------------------------------------------


@pytest.fixture
def events(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Record fsyncs (by path), renames and links in order."""

    recorded: list[tuple[str, str]] = []
    real_fsync = os.fsync

    def fsync(descriptor: int) -> None:
        recorded.append(("fsync", os.readlink(f"/proc/self/fd/{descriptor}")))
        real_fsync(descriptor)

    def rename(src: object, dst: object, **options: object) -> None:
        _REAL_RENAME(src, dst, **options)  # type: ignore[arg-type]
        recorded.append(("rename", str(dst)))

    def link(src: object, dst: object, **options: object) -> None:
        _REAL_LINK(src, dst, **options)  # type: ignore[arg-type]
        recorded.append(("link", str(dst)))

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "rename", rename)
    monkeypatch.setattr(os, "link", link)
    return recorded


def _index(events: list[tuple[str, str]], kind: str, path: Path | str) -> int:
    return events.index((kind, str(path)))


def test_files_are_synchronized_before_their_directories_and_the_anchor_before_the_decision(
    state: Path, events: list[tuple[str, str]]
) -> None:
    root = state / "ledger"
    with _open(state) as ledger:
        request = _start(1)
        entry = ledger.admit(request)
        assert entry.handle is not None
        anchor = root / "req" / request.request_id
        renamed = _index(events, "rename", anchor)
        prepared = [path for kind, path in events[:renamed] if kind == "fsync" and "/prep/" in path]
        # The one envelope file is fsynced, then the prepared directory, all before the rename.
        assert Path(prepared[0]).name == "envelope"
        assert Path(prepared[1]).parent == root / "prep"
        req_synced = _index(events, "fsync", root / "req")
        assert req_synced > renamed
        handle_linked = _index(events, "link", root / "handles" / entry.handle)
        assert handle_linked > req_synced
        assert _index(events, "fsync", root / "handles") > handle_linked

        events.clear()
        ledger.decide(request.request_id)
        linked = _index(events, "link", anchor / "decision")
        assert events[linked - 1][0] == "fsync" and "decision" in Path(events[linked - 1][1]).name
        assert events[linked + 1] == ("fsync", str(anchor))

        events.clear()
        ledger.record_scheduler(request.request_id, "42", "cluster-1")
        ledger.finish(request.request_id, _response(request, "submitted", handle=entry.handle))
        assert _index(events, "link", anchor / "scheduler") < _index(events, "link", anchor / "response")
        assert events[-1] == ("fsync", str(anchor))


# ---------------------------------------------------------------------------
# Two-instance interleavings
# ---------------------------------------------------------------------------


def test_two_instances_racing_to_admit_one_request_leave_one_anchor(
    state: Path, hook: list[Callable[[str], None]]
) -> None:
    first, second = _open(state), _open(state)
    request = _start(1)
    raced: list[None] = []

    def other_admits_first(step: str) -> None:
        if step == "ledger.prepared" and not raced:
            raced.append(None)
            second.admit(request)

    hook.append(other_admits_first)
    entry = first.admit(request)
    hook.clear()
    assert entry.owner == second.nonce
    assert entry == second.lookup_request(request.request_id)
    # The loser removed only its own slots and its prepared anchor; the winner's slots stay.
    assert set(_slots(state).values()) == {f"{request.request_id} {second.nonce}\n".encode()}
    assert sorted(_slots(state)) == ["record.1", "start.1"]
    assert not list((state / "ledger" / "prep").iterdir())
    assert [row["handle"] for row in first.managers()] == [entry.handle]


def test_a_racing_instance_with_different_owner_and_handle_is_adopted_but_different_content_conflicts(
    state: Path, hook: list[Callable[[str], None]]
) -> None:
    first, second = _open(state), _open(state)
    winner = second.admit(_start(1))
    assert winner.handle is not None
    # Equal request bytes, different owner nonce and (never linked) minted handle: one anchor, no conflict.
    adopted = first.admit(_start(1))
    assert adopted == winner and adopted.owner == second.nonce != first.nonce
    assert sorted(path.name for path in (state / "ledger" / "handles").iterdir()) == [winner.handle]
    assert sorted(path.name for path in (state / "ledger" / "req").iterdir()) == [f"{1:032x}"]
    conflicting = replace(_start(1), profile="gpu")
    with pytest.raises(ConflictError):
        first.admit(conflicting)

    # The same outcome when the other instance installs its anchor while this one is preparing.
    raced: list[None] = []

    def other_admits_first(step: str) -> None:
        if step == "ledger.prepared" and not raced:
            raced.append(None)
            second.admit(_start(3))

    hook.append(other_admits_first)
    assert first.admit(_start(3)).owner == second.nonce
    hook.clear()


def test_a_retransmitted_anchor_rename_still_belongs_to_its_instance(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def retransmitted(src: object, dst: object, **options: object) -> None:
        _REAL_RENAME(src, dst, **options)  # type: ignore[arg-type]
        if "/req/" in str(dst):
            raise OSError(errno.ENOTEMPTY, "retransmitted rename")

    monkeypatch.setattr(_txn.os, "rename", retransmitted)
    with _open(state) as ledger:
        entry = ledger.admit(_start(1))
        assert entry.owner == ledger.nonce
        assert len(_slots(state)) == 2  # the winner kept its slots despite the error


def test_a_retransmitted_decision_link_still_wins_and_the_other_instance_loses(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = _open(state), _open(state)
    request = _start(1)
    first.admit(request)

    def retransmitted(src: object, dst: object, **options: object) -> None:
        _REAL_LINK(src, dst, **options)  # type: ignore[arg-type]
        if str(dst).endswith("/decision"):
            raise FileExistsError(errno.EEXIST, "retransmitted link")

    monkeypatch.setattr(_txn.os, "link", retransmitted)
    entry, won = first.decide(request.request_id)
    assert won and entry.decision is not None and entry.decision.nonce == first.nonce
    monkeypatch.setattr(_txn.os, "link", _REAL_LINK)
    later, lost = second.decide(request.request_id, reason="stale_configuration")
    assert not lost and later.decision == entry.decision


def test_two_instances_deciding_at_once_have_exactly_one_winner(state: Path, hook: list[Callable[[str], None]]) -> None:
    first, second = _open(state), _open(state)
    request = _start(1)
    first.admit(request)
    outcomes: list[bool] = []
    raced: list[None] = []

    def other_decides(step: str) -> None:
        if step == "link_new.written" and not raced:
            raced.append(None)
            _entry, won = second.decide(request.request_id)
            outcomes.append(won)

    hook.append(other_decides)
    entry, won = first.decide(request.request_id)
    hook.clear()
    assert outcomes == [True] and not won
    assert entry.decision is not None and entry.decision.nonce == second.nonce


def test_recovery_marks_an_old_unanswered_submission_uncertain_and_a_late_winner_keeps_its_scheduler(
    state: Path,
) -> None:
    winner, recoverer = _open(state), _open(state)
    request = _start(1)
    handle = winner.admit(request).handle
    assert handle is not None
    entry, won = winner.decide(request.request_id)
    assert won and entry.decision is not None and entry.decision.time_ns is not None
    decided = entry.decision.time_ns
    # Not yet older than the longest a submission can take: nothing happens.
    assert recoverer.recover(older_than=95.0, now_ns=decided + 95 * 10**9) == []
    (settled,) = recoverer.recover(older_than=95.0, now_ns=decided + 96 * 10**9)
    assert settled.response is not None
    assert (settled.response.outcome, settled.response.reason, settled.state) == (
        "uncertain",
        "submission_unconfirmed",
        "uncertain",
    )
    # The late winner still records the job, and leaves the response alone.
    late = winner.record_scheduler(request.request_id, "42", "cluster-1")
    stored = winner.finish(request.request_id, _response(request, "submitted", handle=handle))
    assert stored.response == settled.response
    assert (late.job_id, late.cluster, late.state) == ("42", "cluster-1", "submitted")
    assert recoverer.lookup_manager(handle) == stored
    assert recoverer.managers() == [
        {"handle": handle, "profile": "cpu", "request_id": request.request_id, "state": "submitted", "job_id": "42"}
    ]


def test_a_refusal_decided_by_a_crashed_instance_is_linked_deterministically_by_another(
    state: Path, hook: list[Callable[[str], None]]
) -> None:
    crashed, other = _open(state), _open(state)
    request = _start(1)
    handle = crashed.admit(request).handle

    def crash(step: str) -> None:
        if step == "ledger.decided":
            raise _Crash

    hook.append(crash)
    with pytest.raises(_Crash):
        crashed.decide(request.request_id, reason="stale_configuration")
    hook.clear()
    pending = other.lookup_request(request.request_id)
    assert pending is not None and pending.response is None and pending.state == "refused"
    (settled,) = other.recover(older_than=1e9)
    assert settled.response == refusal_response(request, handle, "stale_configuration")
    # Any instance computes the identical refusal, so linking it again is harmless.
    assert crashed.finish(request.request_id, refusal_response(request, handle, "stale_configuration")) == settled


def test_managers_list_handles_cross_checked_and_skip_orphans(state: Path) -> None:
    with _open(state) as ledger:
        received = ledger.admit(_start(1))
        decided = ledger.admit(_start(2))
        ledger.decide(decided.request.request_id, reason="invalid_configuration")
        ledger.admit(_request(3))
        handles = state / "ledger" / "handles"
        (handles / ("c" * 32)).write_bytes(f"{3:032x}\n".encode())  # names a request without that handle
        (handles / ("d" * 32)).write_bytes(f"{7:032x}\n".encode())  # names no request
        (handles / ".dddd.link-1234").write_bytes(b"crash leftover")
        rows = ledger.managers()
        assert [(row["request_id"], row["state"], row["job_id"]) for row in rows] == [
            (f"{1:032x}", "received", None),
            (f"{2:032x}", "refused", None),
        ]
        assert rows[0]["handle"] == received.handle and rows[0]["profile"] == "cpu"
        assert ledger.lookup_manager("c" * 32) is None and ledger.lookup_manager("e" * 32) is None
        (handles / "junk").write_bytes(b"x")
        with pytest.raises(LedgerError, match="handles"):
            ledger.managers()


def test_crash_leftovers_are_swept_after_an_hour(state: Path, hook: list[Callable[[str], None]]) -> None:
    def crash(step: str) -> None:
        if step == "ledger.prepared":
            raise _Crash

    hook.append(crash)
    with _open(state) as crashed, pytest.raises(_Crash):
        crashed.admit(_request(1))
    hook.clear()
    (prepared,) = (state / "ledger" / "prep").iterdir()
    leftover = state / "ledger" / "slots" / ".record.5.link-abcd"
    leftover.write_bytes(b"x")
    with _open(state) as ledger:
        ledger.recover(older_than=95.0)
        assert prepared.exists() and leftover.exists()
        later = _open(state)
        later.recover(older_than=95.0, now_ns=prepared.stat().st_mtime_ns + 3601 * 10**9)
        assert not prepared.exists() and not leftover.exists()
        # An admission failing in-process releases its own slots (only a process death leaks one).
        assert _slots(state) == {}
        assert ledger.verify() == 0


def test_observations_are_per_handle_and_the_last_writer_wins(state: Path) -> None:
    with _open(state) as first, _open(state) as second:
        assert first.observation("a" * 32) is None
        first.record_observation("a" * 32, b'{"x":1}')
        second.record_observation("a" * 32, b'{"x":2}')
        assert first.observation("a" * 32) == b'{"x":2}'
        (state / "ledger" / "observations" / f"{'b' * 32}.json").mkdir()
        assert first.observation("b" * 32) is None
        with pytest.raises(OSError):
            first.record_observation("b" * 32, b"{}")
        with pytest.raises(ValueError, match="handle"):
            first.record_observation("../x", b"{}")
        assert not [path for path in (state / "ledger" / "observations").iterdir() if path.name.startswith(".")]


def test_decisions_are_canonical() -> None:
    nonce = "a" * 32
    assert Decision("submit", nonce, time_ns=5).encode() == f"submit 5 {nonce}\n".encode()
    assert Decision("refuse", nonce, reason="request_expired").encode() == f"refuse request_expired {nonce}\n".encode()
    cases: tuple[tuple[str, dict[str, Any]], ...] = (
        ("submit", {}),
        ("submit", {"time_ns": -1}),
        ("submit", {"time_ns": 1, "reason": "x"}),
        ("refuse", {"reason": "Bad"}),
        ("refuse", {"reason": "x", "time_ns": 1}),
        ("maybe", {}),
    )
    for kind, options in cases:
        with pytest.raises(ValueError):
            Decision(kind, nonce, **options)
    with pytest.raises(ValueError):
        Decision("submit", "A" * 32, time_ns=1)


def test_a_copied_ledger_tree_is_still_valid(state: Path, tmp_path: Path) -> None:
    """The ledger holds no host-local state: a copy (or another filesystem) reads the same."""

    _stored_start(state)
    copy = tmp_path / "copy"
    shutil.copytree(state, copy)
    with _open(copy) as ledger:
        assert ledger.verify() == 1
        assert ledger.managers()[0]["state"] == "submitted"


# ---------------------------------------------------------------------------
# Review fixes: the fenced leftover sweep, swept temporaries, and slots after an install
# ---------------------------------------------------------------------------


def test_the_sweep_fences_a_slow_owners_prepared_anchor_and_the_owner_loses_cleanly(
    state: Path, hook: list[Callable[[str], None]]
) -> None:
    owner, sweeper = _open(state), _open(state)
    request = _start(1)
    prep = state / "ledger" / "prep"
    swept: list[None] = []

    def sweep_while_stalled(step: str) -> None:
        if step == "ledger.prepared" and not swept:
            swept.append(None)
            (prepared,) = prep.iterdir()
            # The owner stalled for over an hour (by the sweeper's clock) between preparing and installing.
            sweeper.recover(older_than=95.0, now_ns=prepared.stat().st_mtime_ns + 3601 * 10**9)
            assert not prepared.exists()

    hook.append(sweep_while_stalled)
    entry = owner.admit(request)
    hook.clear()
    assert swept and entry.owner == owner.nonce and entry.handle is not None
    anchor = state / "ledger" / "req" / request.request_id
    assert sorted(path.name for path in anchor.iterdir()) == ["envelope"]
    assert list(prep.iterdir()) == []
    # The lost round released its own slots; the settled admission holds one record and one start slot.
    assert len(_slots(state)) == 2
    assert set(_slots(state).values()) == {f"{request.request_id} {owner.nonce}\n".encode()}
    assert owner.verify() == 1 and sweeper.verify() == 1


def test_a_prepared_anchor_fenced_while_it_is_written_is_prepared_again(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner, sweeper = _open(state), _open(state)
    real = state_module.fsync_directory
    swept: list[None] = []

    def sweep_before_sync(path: Path) -> None:
        if path.parent.name == "prep" and not swept:
            swept.append(None)
            sweeper.recover(older_than=95.0, now_ns=path.stat().st_mtime_ns + 3601 * 10**9)
        real(path)

    monkeypatch.setattr(state_module, "fsync_directory", sweep_before_sync)
    entry = owner.admit(_request(1))
    assert swept and entry.owner == owner.nonce
    assert list((state / "ledger" / "prep").iterdir()) == []
    assert owner.verify() == 1


def test_a_link_whose_temporary_was_swept_is_retried_once(state: Path, hook: list[Callable[[str], None]]) -> None:
    slots = state / "ledger" / "slots"
    swept: list[Path] = []

    def sweep_temporary(step: str) -> None:
        if step == "link_new.written" and not swept:
            (temporary,) = slots.glob(".record.*")
            temporary.unlink()  # the leftover sweep took the stalled link's temporary
            swept.append(temporary)

    hook.append(sweep_temporary)
    with _open(state) as ledger:
        entry = ledger.admit(_request(1))
    assert swept and entry.owner == ledger.nonce
    assert _slots(state) == {"record.0": f"{1:032x} {ledger.nonce}\n".encode()}
    assert not list(slots.glob(".*"))


def test_an_error_after_the_anchor_is_installed_keeps_its_slots(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = state_module.fsync_directory
    request = _start(1)

    def fail_after_install(path: Path) -> None:
        if path == state / "ledger" / "req":
            raise OSError(errno.EIO, "injected directory sync failure")
        real(path)

    monkeypatch.setattr(state_module, "fsync_directory", fail_after_install)
    with _open(state) as ledger, pytest.raises(OSError, match="injected"):
        ledger.admit(request)
    monkeypatch.undo()
    assert sorted(_slots(state)) == ["record.0", "start.0"]
    with _open(state) as other:
        entry = other.admit(request)  # the replay completes the admission (its handle entry)
        assert entry.owner == ledger.nonce and [row["handle"] for row in other.managers()] == [entry.handle]


def test_a_retransmitted_prepare_mkdir_takes_another_name(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = os.mkdir
    retransmitted: list[str] = []

    def mkdir(path: object, mode: int = 0o777, **options: object) -> None:
        real(path, mode, **options)  # type: ignore[arg-type]
        if "/prep/" in str(path) and not retransmitted:
            retransmitted.append(str(path))
            raise FileExistsError(errno.EEXIST, "retransmitted mkdir")

    monkeypatch.setattr(state_module.os, "mkdir", mkdir)
    with _open(state) as ledger:
        entry = ledger.admit(_request(1))
    assert retransmitted and entry.owner == ledger.nonce
    # The first name is never shared with the second preparation; it is a leftover for the sweep.
    assert [path.name for path in (state / "ledger" / "prep").iterdir()] == [Path(retransmitted[0]).name]


def test_an_anchor_another_instance_installed_is_made_durable_before_this_one_executes_it(
    state: Path, events: list[tuple[str, str]]
) -> None:
    first, second = _open(state), _open(state)
    request = _start(1)
    first.admit(request)
    events.clear()
    entry = second.admit(request)  # the existing-anchor branch: second may now win the decision
    assert entry.owner == first.nonce
    assert ("fsync", str(state / "ledger" / "req")) in events
    second.decide(request.request_id)
    assert events.index(("fsync", str(state / "ledger" / "req"))) < events.index(
        ("link", str(state / "ledger" / "req" / request.request_id / "decision"))
    )
    # An answered entry needs no further durability work to be replayed.
    health = _request(2)
    first.admit(health)
    first.finish(health.request_id, _response(health, "ready"))
    events.clear()
    second.admit(health)
    assert ("fsync", str(state / "ledger" / "req")) not in events
