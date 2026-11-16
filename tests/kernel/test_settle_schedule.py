"""The settle schedule of :func:`httk.workflow._kernel.locate_many` (rule I6) on a fake clock.

The clock advances through :func:`httk.workflow._kernel._pause` and, when a test says so, by a fixed cost per
listing. The deadline runs from the end of the first round. A round is one call of ``_kernel._scan``; the final
one lists owned, unowned, owned.
"""

import os
import uuid
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any

import pytest

from httk.workflow import Workspace, _fs, _kernel
from httk.workflow._kernel import OWNED, format_job_name

DEADLINE = 5.0
PLACEMENT = PurePosixPath("a")
OWNER = uuid.uuid4().hex


class _Spy:
    """A fake clock plus a record of every round and listing; a name in *hidden* is unseen before its time."""

    def __init__(self) -> None:
        self.now = 0.0
        self.pauses: list[float] = []
        self.on_pause: Callable[[], None] = lambda: None
        self.listing_cost = 0.0
        self.log: list[tuple[float, Path]] = []
        self.rounds: list[tuple[float, bool]] = []
        self.ends: list[float] = []
        self.hidden: dict[str, float] = {}
        self.after: Callable[[Path], None] = lambda directory: None

    def pause(self, seconds: float) -> None:
        assert seconds > 0
        self.pauses.append(seconds)
        self.now += seconds
        self.on_pause()


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, deadline: float = DEADLINE) -> tuple[Workspace, _Spy]:
    ws = Workspace.initialize(tmp_path / "ws", durable=False, policy={"visibility_deadline_seconds": deadline})
    (ws.jobs / OWNED / OWNER).mkdir(parents=True)
    spy = _Spy()
    real_list, real_scan = _kernel.list_names, _kernel._scan

    def list_names(directory: Path) -> list[str]:
        names = real_list(directory)
        spy.log.append((spy.now, directory))
        names = [name for name in names if spy.now >= spy.hidden.get(name, spy.now)]
        spy.now += spy.listing_cost
        spy.after(directory)
        return names

    def scan(*args: Any, final: bool = False) -> dict[str, _kernel.JobRef]:
        spy.rounds.append((spy.now, final))
        try:
            return real_scan(*args, final=final)
        finally:
            spy.ends.append(spy.now)

    monkeypatch.setattr(_kernel, "time", SimpleNamespace(monotonic=lambda: spy.now))
    monkeypatch.setattr(_kernel, "_pause", spy.pause)
    monkeypatch.setattr(_kernel, "list_names", list_names)
    monkeypatch.setattr(_kernel, "_scan", scan)
    return ws, spy


def _unowned_path(ws: Workspace, key: str, state: str = "ready") -> Path:
    path = ws.jobs / state / PLACEMENT / format_job_name(key, 500, _fs.fresh_token())
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _unowned(ws: Workspace, key: str) -> Path:
    path = _unowned_path(ws, key)
    path.mkdir()
    return path


def _owned_name(key: str) -> str:
    return format_job_name(key, 500, _fs.fresh_token(), "ready")


def _missing(ws: Workspace, poll: bool) -> dict[str, _kernel.JobRef]:
    return _kernel.locate_many(ws, [str(uuid.uuid4())], placements=[PLACEMENT], settle=True, poll=poll)


def test_unpolled_miss_scans_twice_the_second_after_the_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    assert _missing(ws, poll=False) == {}
    assert spy.rounds == [(0.0, False), (DEADLINE, True)]
    assert spy.pauses == [DEADLINE]
    final = [directory for when, directory in spy.log if when >= DEADLINE]
    # owned, unowned, owned: the final round lists owned/ on both sides of the unowned states.
    owned_root = ws.jobs / OWNED
    assert final[:2] == [owned_root, owned_root / OWNER] and final[-2:] == [owned_root, owned_root / OWNER]
    assert {ws.jobs / state / PLACEMENT for state in _kernel.UNOWNED_STATES} == set(final[2:-2])


def test_polled_miss_backs_off_and_ends_after_the_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    assert _missing(ws, poll=True) == {}
    # Rounds at 0, .1, .3, .7, 1.5, 2.5, 3.5, 4.5 and 5: at most nine, only the last one final.
    assert len(spy.rounds) <= 9
    assert spy.rounds[-1][0] >= DEADLINE and all(when < DEADLINE for when, _ in spy.rounds[:-1])
    assert [final for _, final in spy.rounds] == [False] * (len(spy.rounds) - 1) + [True]
    assert spy.pauses[:4] == pytest.approx([0.1, 0.2, 0.4, 0.8])
    assert max(spy.pauses) <= 1.0
    assert sum(spy.pauses) == pytest.approx(DEADLINE)


def test_polled_wait_ends_when_the_job_is_published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    job_id = str(uuid.uuid4())

    def publish() -> None:
        if len(spy.pauses) == 2:
            _unowned(ws, job_id)

    spy.on_pause = publish
    found = _kernel.locate_many(ws, [job_id], placements=[PLACEMENT], settle=True)
    assert found[job_id].state == "ready"
    assert [when for when, _ in spy.rounds] == pytest.approx([0.0, 0.1, 0.3])
    assert spy.now < DEADLINE


@pytest.mark.parametrize("poll", [False, True])
def test_zero_deadline_scans_once_without_a_pause(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, poll: bool) -> None:
    ws, spy = _setup(tmp_path, monkeypatch, deadline=0.0)
    assert _missing(ws, poll=poll) == {}
    assert spy.rounds == [(0.0, True)] and spy.pauses == []


@pytest.mark.parametrize("poll", [False, True])
def test_the_deadline_runs_from_the_end_of_a_slow_first_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, poll: bool
) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    spy.listing_cost = 0.4
    assert _missing(ws, poll=poll) == {}
    deadline = spy.ends[0] + DEADLINE
    *early, (last, final) = spy.rounds
    # Every round but the last started before the deadline; the last, the only final one, at or after it.
    assert final and last >= deadline
    assert all(when < deadline and not was_final for when, was_final in early)
    if not poll:
        assert len(spy.rounds) == 2


def test_a_polled_round_overrunning_the_deadline_is_followed_by_one_final_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    # Nine listings a round at 0.4 s: rounds start at 0, 3.7 and 7.5; the deadline is 3.6 + 5 = 8.6.
    spy.listing_cost = 0.4
    assert _missing(ws, poll=True) == {}
    deadline = spy.ends[0] + DEADLINE
    assert [final for _, final in spy.rounds] == [False, False, False, True]
    # The third round started before the deadline and ended past it; the final round follows it without a pause.
    assert spy.rounds[2][0] < deadline < spy.ends[2] == spy.rounds[3][0]
    assert spy.pauses == pytest.approx([0.1, 0.2])  # _Spy.pause refuses a non-positive pause


@pytest.mark.parametrize("poll", [False, True])
def test_a_release_during_the_first_scan_that_shows_late_is_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, poll: bool
) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    job_id = str(uuid.uuid4())
    released = ws.jobs / OWNED / OWNER / _owned_name(job_id)
    released.mkdir()
    released_to = _unowned_path(ws, job_id, "succeeded")

    def after(directory: Path) -> None:
        if directory == released_to.parent and not released_to.exists():
            # The destination's listing takes 3 s; the job is released after it and shows there only a deadline
            # later (at 8 s), so a final round anchored at the call's start (5 s) would miss it in both places.
            spy.now += 3.0
            os.rename(released, released_to)
            spy.hidden[released_to.name] = spy.now + DEADLINE

    spy.after = after
    found = _kernel.locate_many(ws, [job_id], placements=[PLACEMENT], settle=True, poll=poll)
    assert found[job_id].path == released_to and found[job_id].state == "succeeded"


@pytest.mark.parametrize("poll", [False, True])
def test_final_round_sees_a_claim_and_a_release_during_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, poll: bool
) -> None:
    ws, spy = _setup(tmp_path, monkeypatch)
    claimed_id, released_id = str(uuid.uuid4()), str(uuid.uuid4())
    claimed_key = f"tagged--{claimed_id}"
    claimed = _unowned(ws, claimed_key)
    owned_dir = ws.jobs / OWNED / OWNER
    released = owned_dir / _owned_name(released_id)
    released.mkdir()
    released_to = _unowned_path(ws, released_id, "succeeded")
    rested_id = str(uuid.uuid4())
    rested = owned_dir / _owned_name(rested_id)
    rested.mkdir()
    rested_to = _unowned_path(ws, rested_id)
    spy.hidden = {claimed.name: DEADLINE, released.name: DEADLINE, rested.name: DEADLINE}
    moves: list[str] = []

    def after(directory: Path) -> None:
        if spy.now < DEADLINE:
            return
        if directory == owned_dir and "claim" not in moves:
            # Claimed right after the first owned listing: no unowned listing of the round sees it any more.
            os.rename(claimed, owned_dir / _owned_name(claimed_key))
            # Released after the first owned listing saw it: it rests in its unowned state before that is listed.
            os.rename(rested, rested_to)
            moves.append("claim")
        elif directory == released_to.parent and "release" not in moves:
            # Released right after its unowned listing: no unowned listing of the round sees it.
            os.rename(released, released_to)
            moves.append("release")

    spy.after = after
    wanted = [claimed_id, released_id, rested_id]
    found = _kernel.locate_many(ws, wanted, placements=[PLACEMENT], settle=True, poll=poll)
    assert moves == ["claim", "release"]
    assert found.keys() == set(wanted)
    assert found[claimed_id].state == OWNED and found[released_id].state == OWNED
    # The later observation wins within the final round.
    assert found[rested_id].state == "ready" and found[rested_id].path == rested_to


def test_a_final_round_stops_listing_once_everything_is_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws, spy = _setup(tmp_path, monkeypatch, deadline=0.0)
    job_id = str(uuid.uuid4())
    (ws.jobs / OWNED / OWNER / _owned_name(job_id)).mkdir()
    found = _kernel.locate_many(ws, [job_id], placements=[PLACEMENT], settle=True)
    assert found[job_id].state == OWNED
    # Found by the first owned pass: no unowned state and no second owned pass is listed.
    assert [directory for _, directory in spy.log] == [ws.jobs / OWNED, ws.jobs / OWNED / OWNER]


def test_a_final_round_that_finds_everything_unowned_skips_the_second_owned_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws, spy = _setup(tmp_path, monkeypatch, deadline=0.0)
    job_id = str(uuid.uuid4())
    assert _kernel.UNOWNED_STATES[-1] == "away"
    _unowned_path(ws, job_id, "away").mkdir()
    found = _kernel.locate_many(ws, [job_id], placements=[PLACEMENT], settle=True)
    assert found[job_id].state == "away"
    listed = [directory for _, directory in spy.log]
    # Found in the last unowned state: owned/ is listed before the unowned states only.
    assert listed[-1] == ws.jobs / "away" / PLACEMENT and listed.count(ws.jobs / OWNED) == 1
