"""A push holds one page ahead (plan WORKFLOW_TRANSFER_SESSION §7.11, T6b).

A push from a local SRC to a remote DST without ``--keep`` holds page k+1 in a producer thread while page k is
staged, adopted and released. The producer owns the selection, prints nothing, and hands each page over with what it
completed, an exception alongside, never instead. After a stop no new page starts and the page in flight is cut at
its next claim or sub-batch (§7.8(b), amended); its never-sent holds are taken back after a refusal of a whole segment
and stay held after an unknown outcome, a failure of SRC or an interrupt. ``--keep`` holds no page ahead.
"""

import _thread
import argparse
import contextlib
import errno
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import MethodType
from typing import Any

import pytest

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _kernel, _moving, _pool
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from test_job_transfer import jobs, outcomes, outgoing, registered, run, selected
from test_moving import assert_in, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_paged_holds import paged
from test_session_push import patched_httk, scripted
from test_staged_settle import _owners

#: The producer thread's name.
PRODUCER = "httk-transfer-hold"
#: How long a test waits for the other thread before it gives up (a failure, never the normal path).
PATIENCE = 10.0

Ssh = tuple[Path, Workspace, str, Workspace, str]


@pytest.fixture
def push(tmp_path: Path, remote: Remote) -> Ssh:
    """A project with the stand-in ssh cluster (its ``httk`` patched as for a session push), a local SRC and a DST
    there."""

    project = tmp_path / "project"
    initialize_project(project, name="hold-ahead")
    fake_remote(project)
    patched_httk(tmp_path, remote)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


def in_producer() -> bool:
    return threading.current_thread().name == PRODUCER


def producers() -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == PRODUCER]


def nth_producer_hold(monkeypatch: pytest.MonkeyPatch, nth: int, action: Callable[[], object]) -> None:
    """Run *action* in the producer before its *nth* hold (``_eject_claimed``) is made."""

    real = transfer_cli._eject_claimed
    count: list[int] = []
    lock = threading.Lock()

    def eject(*arguments: Any, **options: Any) -> Any:
        if in_producer():
            with lock:
                count.append(1)
                number = len(count)
            if number == nth:
                action()
        return real(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_eject_claimed", eject)


class Joining(threading.Event):
    """Set once the main thread waits for the producer (it set the stop flag first), from inside the wait, where
    interrupts are absorbed or deferred; :attr:`polled` is set each time the wait looked at the running operations
    (once it polled with holds running: clear it, then wait for it)."""

    def __init__(self) -> None:
        super().__init__()
        self.polled = threading.Event()


def joining(monkeypatch: pytest.MonkeyPatch) -> Joining:
    """The main thread's wait for the producer, observed (see :class:`Joining`)."""

    event = Joining()
    real = _pool.wait_quietly

    def wait_quietly(condition: Any, done: Callable[[], bool], running: Callable[[], int], **options: Any) -> Any:
        def done_once_waiting() -> bool:
            event.set()
            return done()

        def running_polled() -> int:
            count = running()
            event.polled.set()
            return count

        return real(condition, done_once_waiting, running_polled, **options)

    monkeypatch.setattr(_pool, "wait_quietly", wait_quietly)
    return event


@contextlib.contextmanager
def default_sigint() -> Iterator[None]:
    """``SIGINT`` raises ``KeyboardInterrupt`` in the main thread meanwhile (as in a command)."""

    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


def aheads(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """The producers (:class:`_Ahead`) made from now on."""

    made: list[Any] = []
    real = transfer_cli._Ahead.__init__

    def init(self: Any, selection: Any) -> None:
        real(self, selection)
        made.append(self)

    monkeypatch.setattr(transfer_cli._Ahead, "__init__", init)
    return made


def drive_interrupted(monkeypatch: pytest.MonkeyPatch, after: threading.Event | None = None) -> None:
    """Make the first page's drive raise ``KeyboardInterrupt`` (once *after* is set), as a Ctrl-C there."""

    def drive(*arguments: Any, **options: Any) -> Any:
        if after is not None:
            assert after.wait(PATIENCE)
        raise KeyboardInterrupt

    monkeypatch.setattr(transfer_cli, "_drive", drive)


def held_ids(workspace: Workspace) -> list[str]:
    return sorted(member[-36:] for hold in _moving.held(workspace) for member in hold.members)


def ids_of(plan: list[tuple[dict[str, Any], str, int]]) -> list[str]:
    return sorted(str(mapping["id"]) for mapping, _, _ in plan)


def selection_of(workspace: Workspace, cwd: Path, **options: Any) -> Any:
    """A selection of *workspace* as ``job transfer`` makes it (``jobs``, ``state``, ``step``, ``placement``,
    ``tree``), its snapshot taken."""

    namespace = argparse.Namespace(
        **{"jobs": [], "state": [], "step": None, "placement": None, "tree": False, **options}
    )
    selection = transfer_cli._Selection(
        workspace, cwd, namespace, locator="elsewhere:x", destination_id=None, workflows=False
    )
    selection.snapshot()
    return selection


def page_held(monkeypatch: pytest.MonkeyPatch, nth: int) -> threading.Event:
    """An event set once the *nth* page was held (its ``next_page`` returned)."""

    event = threading.Event()
    real = transfer_cli._Selection.next_page
    count: list[int] = []

    def next_page(selection: Any, **options: Any) -> Any:
        page = real(selection, **options)
        count.append(1)
        if len(count) == nth:
            event.set()
        return page

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    return event


def first_stage_after(monkeypatch: pytest.MonkeyPatch, event: threading.Event) -> None:
    """Make the first stage request wait until *event* is set."""

    real = transfer_cli._session_stage
    count: list[int] = []

    def stage(*arguments: Any, **options: Any) -> Any:
        count.append(1)
        if len(count) == 1:
            assert event.wait(PATIENCE)
        return real(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_session_stage", stage)


@contextlib.contextmanager
def writers() -> Iterator[list[str]]:
    """Record the names of the threads that write to standard error (as captured during the test)."""

    names: list[str] = []
    stream = sys.stderr

    class Recording:
        def write(self, text: str) -> int:
            names.append(threading.current_thread().name)
            return stream.write(text)

        def __getattr__(self, name: str) -> Any:
            return getattr(stream, name)

    sys.stderr = Recording()
    try:
        yield names
    finally:
        sys.stderr = stream


# -- the overlap ----------------------------------------------------------------------------------------------------


def test_the_next_page_is_held_while_the_current_one_is_adopted(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 1)
    events: list[str] = []
    threads: list[str] = []
    second = threading.Event()
    real_next = transfer_cli._Selection.next_page

    def next_page(selection: Any, **options: Any) -> Any:
        number = len(threads) + 1
        threads.append(threading.current_thread().name)
        events.append(f"hold {number} starts")
        if number == 2:
            second.set()
        return real_next(selection, **options)

    real_adopt = transfer_cli._adopt_staged
    adoptions: list[int] = []

    def adopt_staged(*arguments: Any, **options: Any) -> Any:
        adoptions.append(1)
        number = len(adoptions)
        if number == 1:
            second.wait(PATIENCE)  # not a deadlock without the overlap: the hold would only start too late
        try:
            return real_adopt(*arguments, **options)
        finally:
            events.append(f"adoption {number} ends")

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    monkeypatch.setattr(transfer_cli, "_adopt_staged", adopt_staged)
    plan = jobs(near, 3)
    # Without -q: the progress lines and every other line are the main thread's.
    with writers() as names:
        assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 3
    assert events.index("hold 2 starts") < events.index("adoption 1 ends")
    assert set(threads) == {PRODUCER}
    assert names and PRODUCER not in names  # the producer prints nothing
    assert outgoing(near) == [] and producers() == []
    assert_in(far, plan)


def test_keep_holds_no_page_ahead(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 1)
    events: list[str] = []
    threads: list[str] = []
    real_next = transfer_cli._Selection.next_page

    def next_page(selection: Any, **options: Any) -> Any:
        threads.append(threading.current_thread().name)
        events.append(f"hold {len(threads)} starts")
        return real_next(selection, **options)

    real_drive = transfer_cli._drive
    drives: list[int] = []

    def drive(*arguments: Any, **options: Any) -> Any:
        drives.append(1)
        number = len(drives)
        try:
            return real_drive(*arguments, **options)
        finally:
            events.append(f"page {number} driven")

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    monkeypatch.setattr(transfer_cli, "_drive", drive)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", "--keep", *selected(plan), near_name, f"cluster:{far_name}") == 0
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 3 and all(o["kept"] for o in transfers)
    assert threads == ["MainThread"] * 3  # each page held here, after the one before was driven
    assert events == [
        "hold 1 starts",
        "page 1 driven",
        "hold 2 starts",
        "page 2 driven",
        "hold 3 starts",
        "page 3 driven",
    ]
    assert_in(far, plan)


def test_a_step_note_of_a_page_held_ahead_is_printed_by_the_main_thread(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, _far, far_name = push
    paged(monkeypatch, 1)
    plan = jobs(near, 2)
    with writers() as names:
        argv = ["--json", "-q", *selected(plan), "--step", "other", near_name, f"cluster:{far_name}"]
        assert run(project, *argv) == 0
    captured = capsys.readouterr()
    assert [line for line in captured.err.splitlines() if "left out" in line] == [
        f"{find(near, mapping['id']).job_key} is at step start, not --step other: left out"  # type: ignore[union-attr]
        for mapping, _, _ in plan
    ]
    assert names and PRODUCER not in names


# -- failures of the producer ---------------------------------------------------------------------------------------


def test_a_producer_failure_after_its_holds_hands_them_over_with_it(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    far.set_policy({"visibility_deadline_seconds": 0})  # each page adopted before the next is handed over
    paged(monkeypatch, 2)
    real_close = _kernel.Owner.close
    closes: list[int] = []

    def close(owner: _kernel.Owner) -> None:
        if in_producer():
            closes.append(1)
            if len(closes) == 2:  # the second page's owner, after both its holds were made
                raise OSError(errno.EIO, "the owner could not be closed")
        real_close(owner)

    monkeypatch.setattr(_kernel.Owner, "close", close)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 2
    # The holds the failing page made are reported with it, held.
    for outcome, (mapping, _, _) in zip(transfers[2:], plan[2:], strict=True):
        assert [member[-36:] for member in outcome["members"]] == [str(mapping["id"])]
        assert "stopped ([Errno 5] the owner could not be closed)" in outcome["message"]
        assert f"stays held on {near.root}" in outcome["message"]
    (error,) = document["errors"]
    assert "stopped: [Errno 5] the owner could not be closed" in error
    assert "0 selected job(s) were not attempted" in error and f"--resume {near.root} cluster:{far_name}" in error
    assert sorted(outgoing(near)) == sorted(outcome["transfer_id"] for outcome in transfers[2:])
    assert producers() == []
    monkeypatch.setattr(_kernel.Owner, "close", real_close)
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    assert outgoing(near) == []
    assert_in(far, plan)


def test_a_producer_error_is_raised_at_the_hand_over_and_stops_the_pair(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    far.set_policy({"visibility_deadline_seconds": 0})  # each page adopted before the next is handed over
    paged(monkeypatch, 1)
    real_owner = transfer_cli._owner
    owners: list[int] = []

    def owner(*arguments: Any, **options: Any) -> Any:
        if in_producer():
            owners.append(1)
            if len(owners) == 2:  # the second page: nothing of it held
                raise OSError(errno.ENOSPC, "no owner could be registered")
        return real_owner(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_owner", owner)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"]
    (error,) = document["errors"]
    assert "stopped: [Errno 28] no owner could be registered" in error
    assert "2 selected job(s) were not attempted" in error
    assert outgoing(near) == [] and producers() == []
    assert_in(far, plan[:1])
    assert_in(near, plan[1:])


def test_an_unexpected_producer_exception_is_raised_in_the_main_thread(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 1)
    real_owner = transfer_cli._owner
    owners: list[int] = []

    def owner(*arguments: Any, **options: Any) -> Any:
        if in_producer():
            owners.append(1)
            if len(owners) == 2:
                raise LookupError("a defect")
        return real_owner(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_owner", owner)
    plan = jobs(near, 3)
    with pytest.raises(LookupError, match="a defect"):
        run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    assert producers() == [] and _owners(near) == [] and _owners(far) == []
    assert_in(near, plan[1:])


# -- stops ----------------------------------------------------------------------------------------------------------


def _held_ahead(
    monkeypatch: pytest.MonkeyPatch, near: Workspace, how: str, count: int
) -> list[tuple[dict[str, Any], str, int]]:
    """*count* jobs pushed in pages of two (``pooled``: of four, held over two workers); the second page is either
    held whole before the first is staged (``whole``), or cut by the stop: its first claim (``cut``) or sub-batch
    (``pooled``) is made only once the main thread waits for the producer, so nothing more of it is held."""

    if how == "pooled":
        near.configure_settings({"io.workers": 2})
        paged(monkeypatch, 4)
    else:
        paged(monkeypatch, 2)
    plan = jobs(near, count)
    if how == "whole":
        first_stage_after(monkeypatch, page_held(monkeypatch, 2))
        return plan
    joined = joining(monkeypatch)
    reached = threading.Event()
    first = 5 if how == "pooled" else 3  # the first hold of the second page
    lock = threading.Lock()
    made: list[int] = []
    real = transfer_cli._eject_claimed

    def eject(*arguments: Any, **options: Any) -> Any:
        if threading.current_thread() is not threading.main_thread():  # the producer's (or its pool's)
            with lock:
                made.append(1)
                number = len(made)
            if number >= first:
                reached.set()
                assert joined.wait(PATIENCE)
        return real(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_eject_claimed", eject)
    first_stage_after(monkeypatch, reached)  # the stop comes once the second page is being held
    return plan


@pytest.mark.parametrize("how", ["whole", "cut", "pooled"])
def test_a_refusal_of_the_whole_destination_takes_back_the_page_held_ahead(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], how: str
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    plan = _held_ahead(monkeypatch, near, how, 8 if how == "pooled" else 5)
    argv = ["--state", "succeeded"] if how == "pooled" else selected(plan)
    assert run(project, "--json", "-q", *argv, near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    first = 4 if how == "pooled" else 2
    ahead = {"whole": 2, "cut": 1, "pooled": 2}[how]
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * (first + ahead)
    assert all("a scripted refusal" in outcome["message"] for outcome in transfers)
    assert all(f"the jobs are back in {near.root}" in outcome["message"] for outcome in transfers)
    (error,) = document["errors"]
    left = len(plan) - first - ahead
    assert f"stopped: cluster:{far_name}: a scripted refusal" in error
    assert f"; {left} selected job(s) were not attempted" in error
    assert outgoing(near) == [] and producers() == [] and _owners(near) == []
    assert_in(near, plan)


@pytest.mark.parametrize("how", ["whole", "cut", "pooled"])
def test_an_unknown_outcome_leaves_the_page_held_ahead_held(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], how: str
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"garble": True}})
    plan = _held_ahead(monkeypatch, near, how, 8 if how == "pooled" else 5)
    argv = ["--state", "succeeded"] if how == "pooled" else selected(plan)
    assert run(project, "--json", "-q", *argv, near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    first = 4 if how == "pooled" else 2
    ahead = {"whole": 2, "cut": 1, "pooled": 2}[how]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * (first + ahead)
    for outcome in transfers[first:]:
        # Held but never attempted: today's wording for the holds of a stopped pair.
        assert outcome["message"].startswith(f"not delivered, since the transfer to cluster:{far_name} stopped (")
        assert outcome["message"].endswith(f"it stays held on {near.root}")
    (error,) = document["errors"]
    left = len(plan) - first - ahead
    assert f"; {left} selected job(s) were not attempted" in error
    assert sorted(outgoing(near)) == sorted(outcome["transfer_id"] for outcome in transfers)
    assert producers() == [] and _owners(near) == []
    monkeypatch.delenv("HTTK_TEST_SCRIPT")
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * (first + ahead)
    assert outgoing(near) == []
    assert sum(find(far, mapping["id"]) is not None for mapping, _, _ in plan) == first + ahead
    assert sum(find(near, mapping["id"]) is not None for mapping, _, _ in plan) == left


def test_sigint_during_a_hold_ahead_waits_for_it_and_leaves_a_consistent_state(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 2)
    holding = threading.Event()
    joined = joining(monkeypatch)

    def interrupt_during_the_hold() -> None:
        holding.set()
        assert joined.wait(PATIENCE)  # the main thread was interrupted and waits for this hold
        joined.polled.clear()
        _thread.interrupt_main()  # a second Ctrl-C while it waits: recorded
        assert joined.polled.wait(PATIENCE)  # it looked again while this hold runs (and said so)

    nth_producer_hold(monkeypatch, 3, interrupt_during_the_hold)  # the second page's first hold
    real_drive = transfer_cli._drive

    def drive(*arguments: Any, **options: Any) -> Any:
        assert holding.wait(PATIENCE)
        raise KeyboardInterrupt  # Ctrl-C while the first page is driven and the second held

    monkeypatch.setattr(transfer_cli, "_drive", drive)
    plan = jobs(near, 4)
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        with pytest.raises(KeyboardInterrupt):
            run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    finally:
        signal.signal(signal.SIGINT, previous)
    assert "waiting for 1 running operations to finish" in capsys.readouterr().err
    # The hold in flight finished, nothing more of its page was held, and no owner is left registered.
    assert producers() == [] and _owners(near) == [] and _owners(far) == []
    held = _moving.held(near)
    assert sorted(member[-36:] for hold in held for member in hold.members) == sorted(
        str(mapping["id"]) for mapping, _, _ in plan[:3]
    )
    assert all(Path(hold.path).exists() for hold in held)
    assert find(near, plan[3][0]["id"]).state == "succeeded"  # type: ignore[union-attr]
    monkeypatch.setattr(transfer_cli, "_drive", real_drive)
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 3
    assert outgoing(near) == []
    assert_in(far, plan[:3])


# -- the pair's stop reaches the producer at once (G1) --------------------------------------------------------------


def test_the_pair_s_stop_cuts_the_page_held_ahead_while_its_page_is_still_driven(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    paged(monkeypatch, 4)
    plan = jobs(near, 8)
    made = aheads(monkeypatch)
    reached, seen = threading.Event(), threading.Event()
    lock = threading.Lock()
    claims: list[int] = []
    real_eject = transfer_cli._eject_claimed

    def eject(*arguments: Any, **options: Any) -> Any:
        if in_producer():
            with lock:
                claims.append(1)
                number = len(claims)
            if number == 5:  # the second page's first claim waits for the stop itself, not for the join
                reached.set()
                deadline = time.monotonic() + PATIENCE
                while not made[0].cut.stop and time.monotonic() < deadline:
                    time.sleep(0.01)
                if made[0].cut.stop:
                    seen.set()
        return real_eject(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_eject_claimed", eject)
    first_stage_after(monkeypatch, reached)
    real_take_back = transfer_cli._take_back
    flagged: list[bool] = []

    def take_back(*arguments: Any, **options: Any) -> Any:
        # The refused page is taken back only once the page held ahead is cut (the pair stopped first).
        flagged.append(made[0].cut.stop)
        return real_take_back(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_take_back", take_back)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    assert flagged[0] is True and seen.is_set()
    transfers = outcomes(capsys)["transfers"]
    # Nothing was claimed after the stop but the claim in flight; it is taken back with the refused page.
    assert len(claims) == 5
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * 5
    assert outgoing(near) == [] and producers() == []
    assert_in(near, plan)


# -- interrupts at the start and the stop (G2, G5) ------------------------------------------------------------------


def test_an_interrupt_between_the_producer_s_start_and_its_record_still_joins_it(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 2)
    real_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        real_start(thread)
        if thread.name == PRODUCER:
            _thread.interrupt_main()  # Ctrl-C right after the thread started, before it is recorded as started

    monkeypatch.setattr(threading.Thread, "start", start)
    joined = joining(monkeypatch)
    real_decide = transfer_cli._Selection._decide

    def decide(*arguments: Any, **options: Any) -> Any:
        if in_producer():
            joined.wait(PATIENCE)  # the producer still decides its first page until the main thread waits for it
        return real_decide(*arguments, **options)

    monkeypatch.setattr(transfer_cli._Selection, "_decide", decide)
    plan = jobs(near, 4)
    with default_sigint(), pytest.raises(KeyboardInterrupt):
        run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    # Joined: no producer runs on after the command, and it was cut while it decided: no owner, nothing held.
    assert producers() == [] and _owners(near) == [] and _owners(far) == []
    assert held_ids(near) == []
    assert_in(near, plan)


@pytest.mark.parametrize("where", ["signal-entering-the-stop", "raised-entering-the-stop", "before-the-stop"])
def test_an_interrupt_entering_the_stop_never_leaves_the_producer_running(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], where: str
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 2)
    made = aheads(monkeypatch)
    holding, entered = threading.Event(), threading.Event()
    joined = joining(monkeypatch)

    def hold_until_the_stop() -> None:
        holding.set()
        # In flight until the main thread waits for it (or, when it never will, entered the stop).
        (joined if where == "signal-entering-the-stop" else entered).wait(PATIENCE)

    nth_producer_hold(monkeypatch, 3, hold_until_the_stop)  # the second page's first hold
    drive_interrupted(monkeypatch, holding)  # the first Ctrl-C, while the first page is driven
    flagged: list[bool] = []  # whether the stop flag was set when the stop was entered
    if where == "before-the-stop":
        real_stop_ahead = transfer_cli._stop_ahead

        def stop_ahead(*arguments: Any, **options: Any) -> Any:
            flagged.append(made[0].cut.stop)
            entered.set()
            _thread.interrupt_main()  # a second Ctrl-C before the producer is waited for: it is not
            threading.Event().wait(PATIENCE)  # the interrupt lands here
            return real_stop_ahead(*arguments, **options)

        monkeypatch.setattr(transfer_cli, "_stop_ahead", stop_ahead)
    else:
        real_stop = transfer_cli._Ahead.stop

        def stop(ahead: Any, **options: Any) -> Any:
            flagged.append(ahead.cut.stop)
            entered.set()
            if where == "raised-entering-the-stop":
                raise KeyboardInterrupt  # a second Ctrl-C at the stop's entry: the producer is not waited for
            _thread.interrupt_main()  # recorded: the stop and its wait run to their end
            return real_stop(ahead, **options)

        monkeypatch.setattr(transfer_cli._Ahead, "stop", stop)
    plan = jobs(near, 4)
    try:
        with default_sigint(), pytest.raises(KeyboardInterrupt):
            run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
        if where == "signal-entering-the-stop":
            assert producers() == []  # waited for
        # Otherwise not waited for, but stopped: the flag was set first, so it ends by itself.
        for thread in producers():
            thread.join(PATIENCE)
        assert producers() == [] and made[0].cut.stop
        assert flagged == [True]  # set first, before anything that an interrupt can cut short
    finally:
        for ahead in made:
            ahead.cut.stop = True  # never leave a producer waiting for its slot behind a failed test
    assert _owners(near) == [] and _owners(far) == []
    # The first page held (never driven) and the second page's hold in flight stay held; nothing more was held.
    assert held_ids(near) == ids_of(plan[:3])
    assert find(near, plan[3][0]["id"]).state == "succeeded"  # type: ignore[union-attr]


def test_an_interrupt_while_the_producer_is_joined_after_a_refusal_keeps_its_holds(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    paged(monkeypatch, 2)
    joined, reached = joining(monkeypatch), threading.Event()

    def interrupt_while_joined() -> None:
        reached.set()
        assert joined.wait(PATIENCE)
        joined.polled.clear()
        _thread.interrupt_main()  # Ctrl-C while the refusal's stop waits for this hold
        assert joined.polled.wait(PATIENCE)  # recorded while this hold still runs

    nth_producer_hold(monkeypatch, 3, interrupt_while_joined)
    first_stage_after(monkeypatch, reached)
    plan = jobs(near, 5)
    with default_sigint(), pytest.raises(KeyboardInterrupt):
        run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    # The refused page was taken back; the page held ahead is not, since an interrupt was seen: it stays held.
    assert held_ids(near) == ids_of(plan[2:3])
    assert producers() == [] and _owners(near) == []
    assert_in(near, plan[:2] + plan[3:])


# -- pages taken and not driven (G3) --------------------------------------------------------------------------------


def unsent_reported(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The transfer ids :meth:`_Transfer.unsent` reports."""

    seen: list[str] = []
    real = transfer_cli._Transfer.unsent

    def unsent(run_: Any, pair: Any, holds: list[_moving.Hold], **options: Any) -> None:
        seen.extend(hold.transfer_id for hold in holds)
        real(run_, pair, holds, **options)

    monkeypatch.setattr(transfer_cli._Transfer, "unsent", unsent)
    return seen


def test_an_interrupt_right_after_a_page_is_taken_reports_its_holds(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, _far, far_name = push
    paged(monkeypatch, 2)
    reported = unsent_reported(monkeypatch)
    real_take = transfer_cli._Ahead.take

    def take(ahead: Any) -> Any:
        page = real_take(ahead)
        if page is not None:
            raise KeyboardInterrupt  # right after the page left the slot, before anything of it is handed over
        return page

    monkeypatch.setattr(transfer_cli._Ahead, "take", take)
    plan = jobs(near, 4)
    with pytest.raises(KeyboardInterrupt):
        run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    assert producers() == [] and _owners(near) == []
    # The page taken (and the next one, if it was held meanwhile) is reported held, never lost.
    assert sorted(reported) == sorted(outgoing(near)) and len(reported) >= 2
    assert set(ids_of(plan[:2])) <= set(held_ids(near))


def test_a_failing_hand_over_reports_the_page_s_holds_held(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    far.set_policy({"visibility_deadline_seconds": 0})  # each page adopted before the next is handed over
    paged(monkeypatch, 2)
    real_extend = transfer_cli._Spool.extend_errors
    calls: list[int] = []

    def extend_errors(spool: Any, errors: Any) -> None:
        calls.append(1)
        if len(calls) == 2:  # the second page's hand-over
            raise BrokenPipeError(errno.EPIPE, "stderr went away")
        real_extend(spool, errors)

    monkeypatch.setattr(transfer_cli._Spool, "extend_errors", extend_errors)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 2
    assert all(f"stays held on {near.root}" in outcome["message"] for outcome in transfers[2:])
    assert sorted(outgoing(near)) == sorted(outcome["transfer_id"] for outcome in transfers[2:])
    assert held_ids(near) == ids_of(plan[2:])
    (error,) = document["errors"]
    assert "stopped: [Errno 32] stderr went away" in error


# -- what a page held ahead hands over (G4, G6, G7) -----------------------------------------------------------------


def test_a_pooled_failure_hands_over_every_hold_completed_beside_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = registered(tmp_path, "a")
    workspace.configure_settings({"io.workers": 2})
    plan = jobs(workspace, 2)
    selection = selection_of(workspace, tmp_path, state=["succeeded"])
    first = selection._candidate(0)[0].job_key  # item 0 of the pooled sub-batch
    barrier = threading.Barrier(2, timeout=PATIENCE)
    real_eject = transfer_cli._eject_claimed

    def eject(workspace_: Any, owner: Any, root: Any, *arguments: Any, **options: Any) -> Any:
        barrier.wait()  # both items run at once
        if root.job_key == first:
            raise LookupError("a defect")  # item 0 fails ...
        return real_eject(workspace_, owner, root, *arguments, **options)  # ... while item 1 completes its hold

    monkeypatch.setattr(transfer_cli, "_eject_claimed", eject)
    page = selection.next_page(holds=None, errors=None, scanned=None, members=None, cut=transfer_cli._Cut())
    assert isinstance(page.exception, LookupError)
    assert [hold.members[0][-36:] for hold in page.holds] == held_ids(workspace)
    assert len(page.holds) == 1 and page.holds[0].members[0] != first
    assert {str(mapping["id"]) for mapping, _, _ in plan} >= set(held_ids(workspace))


@pytest.mark.parametrize("how", ["failed-claim", "cut-then-close-failed"])
def test_a_failed_page_leaves_its_unattempted_candidates_for_the_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, how: str
) -> None:
    workspace, _name = registered(tmp_path, "a")
    plan = jobs(workspace, 3)
    selection = selection_of(workspace, tmp_path, jobs=[str(mapping["id"]) for mapping, _, _ in plan])
    cut = transfer_cli._Cut()
    real_eject = transfer_cli._eject_claimed
    ejected: list[int] = []

    def eject(*arguments: Any, **options: Any) -> Any:
        ejected.append(1)
        if how == "failed-claim" and len(ejected) == 2:
            raise LookupError("a defect")  # the second attempt fails; the third is never attempted
        hold = real_eject(*arguments, **options)
        if how == "cut-then-close-failed":
            cut.stop = True  # cut after the first hold; then the owner cannot be closed
        return hold

    def close(owner: Any) -> None:
        raise OSError(errno.EIO, "the owner could not be closed")

    monkeypatch.setattr(transfer_cli, "_eject_claimed", eject)
    if how == "cut-then-close-failed":
        monkeypatch.setattr(_kernel.Owner, "close", close)
    page = selection.next_page(holds=3, errors=3, scanned=3, members=None, cut=cut)
    assert page.exception is not None and len(page.holds) == 1
    assert selection.remaining() == (1 if how == "failed-claim" else 2)


def test_a_stop_while_a_page_is_decided_registers_no_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = registered(tmp_path, "a")
    plan = jobs(workspace, 3)
    selection = selection_of(workspace, tmp_path, jobs=[str(mapping["id"]) for mapping, _, _ in plan])
    cut = transfer_cli._Cut()
    cut.stop = True
    registered_owners: list[int] = []
    real_owner = transfer_cli._owner

    def owner(*arguments: Any, **options: Any) -> Any:
        registered_owners.append(1)
        return real_owner(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_owner", owner)
    page = selection.next_page(holds=3, errors=3, scanned=3, members=None, cut=cut)
    assert page.holds == [] and page.exception is None and registered_owners == []
    assert selection.remaining() == 3 and held_ids(workspace) == []


# -- SRC failure first, the fallback, local to local (G8) -----------------------------------------------------------


def test_a_failure_of_src_is_classified_before_a_refusal_s_take_back(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    paged(monkeypatch, 2)
    real_close = _kernel.Owner.close
    closes: list[int] = []

    def close(owner: _kernel.Owner) -> None:
        if in_producer():
            closes.append(1)
            if len(closes) == 2:  # the page held ahead: both its holds made, then SRC fails
                raise OSError(errno.EIO, "the owner could not be closed")
        real_close(owner)

    monkeypatch.setattr(_kernel.Owner, "close", close)
    first_stage_after(monkeypatch, page_held(monkeypatch, 2))  # the refusal comes once that page failed
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * 2 + ["failed"] * 2
    assert all(f"stays held on {near.root}" in outcome["message"] for outcome in transfers[2:])
    assert held_ids(near) == ids_of(plan[2:])  # never taken back
    assert any("holding a page on" in error and "[Errno 5]" in error for error in document["errors"])


def test_without_a_thread_each_page_is_held_by_the_main_thread(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 1)
    real_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        if thread.name == PRODUCER:
            raise RuntimeError("can't start new thread")
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    threads = next_page_threads(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 3
    assert threads == ["MainThread"] * 3
    assert_in(far, plan)


def next_page_threads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The names of the threads that hold the pages from now on."""

    threads: list[str] = []
    real = transfer_cli._Selection.next_page

    def next_page(selection: Any, **options: Any) -> Any:
        threads.append(threading.current_thread().name)
        return real(selection, **options)

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    return threads


def test_local_to_local_holds_no_page_ahead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    paged(monkeypatch, 1)
    threads = next_page_threads(monkeypatch)
    plan = jobs(source, 3)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 3
    assert threads == ["MainThread"] * 3
    assert_in(target, plan)


# -- take-backs after the join, interrupts and a failing stderr (H2, H3, H5) ----------------------------------------


def test_ctrl_c_during_the_take_back_of_the_page_held_ahead_stops_it_promptly(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    near.configure_settings({"io.workers": 2})
    paged(monkeypatch, 8)
    jobs(near, 16)
    first_stage_after(monkeypatch, page_held(monkeypatch, 2))  # the second page held whole before the refusal
    take_backs: list[int] = []
    real_take_back = transfer_cli._take_back

    def take_back(*arguments: Any, **options: Any) -> Any:
        take_backs.append(1)
        return real_take_back(*arguments, **options)

    recorded = threading.Event()
    real_on_sigint = _pool._Pool.on_sigint

    def on_sigint(pool: Any, signum: int, frame: Any) -> None:
        real_on_sigint(pool, signum, frame)
        recorded.set()

    adopted: list[int] = []
    lock = threading.Lock()
    real_adopt = _moving.adopt

    def adopt(*arguments: Any, **options: Any) -> Any:
        if len(take_backs) == 2:  # the page held ahead
            with lock:
                adopted.append(1)
                first = len(adopted) == 1
            if first:
                _thread.interrupt_main()  # Ctrl-C during its take-back
            # Each item running finishes only once the take-back's pool saw it: no further item starts after.
            assert recorded.wait(PATIENCE)
        return real_adopt(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_take_back", take_back)
    monkeypatch.setattr(_pool._Pool, "on_sigint", on_sigint)
    monkeypatch.setattr(_moving, "adopt", adopt)
    with default_sigint(), pytest.raises(KeyboardInterrupt):
        run(project, "--json", "-q", "--state", "succeeded", near_name, f"cluster:{far_name}")
    # Only the items already running finished (two workers); the rest of the page stays held for --resume.
    assert len(take_backs) == 2 and 1 <= len(adopted) <= 2
    assert len(held_ids(near)) == 8 - len(adopted)
    assert producers() == [] and _owners(near) == []


def test_ctrl_c_let_pass_by_a_custom_handler_during_the_join_still_prevents_the_take_back(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    paged(monkeypatch, 2)
    joined, reached = joining(monkeypatch), threading.Event()

    def interrupt_while_joined() -> None:
        reached.set()
        assert joined.wait(PATIENCE)
        joined.polled.clear()
        _thread.interrupt_main()  # Ctrl-C while the refusal's stop waits for this hold
        assert joined.polled.wait(PATIENCE)

    nth_producer_hold(monkeypatch, 3, interrupt_while_joined)
    first_stage_after(monkeypatch, reached)
    plan = jobs(near, 5)
    seen: list[int] = []
    previous = signal.signal(signal.SIGINT, lambda _signum, _frame: seen.append(1))  # lets the interrupt pass
    try:
        code = run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    finally:
        signal.signal(signal.SIGINT, previous)
    assert code == 1 and seen == [1]
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * 2 + ["failed"]
    assert transfers[2]["message"].endswith(f"it stays held on {near.root}")
    assert held_ids(near) == ids_of(plan[2:3])  # seen, so never taken back
    assert producers() == [] and _owners(near) == []


def test_a_stderr_that_keeps_failing_costs_no_report_of_the_holds(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    far.set_policy({"visibility_deadline_seconds": 0})  # each page adopted before the next is handed over
    paged(monkeypatch, 2)
    third = page_held(monkeypatch, 3)
    real_extend = transfer_cli._Spool.extend_errors
    calls: list[int] = []

    def extend_errors(spool: Any, errors: Any) -> None:
        calls.append(1)
        if len(calls) >= 2:  # from the second page's hand-over on, every time
            assert third.wait(PATIENCE)  # the third page is held ahead by then: its report is replayed
            raise BrokenPipeError(errno.EPIPE, "stderr went away")
        real_extend(spool, errors)

    monkeypatch.setattr(transfer_cli._Spool, "extend_errors", extend_errors)
    plan = jobs(near, 6)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 4
    assert all(f"stays held on {near.root}" in outcome["message"] for outcome in transfers[2:])
    assert held_ids(near) == ids_of(plan[2:])
    assert len(calls) >= 3  # the replay of the third page's report failed too, contained


def test_a_failure_right_after_the_producer_started_still_waits_for_it(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, _far, far_name = push
    paged(monkeypatch, 2)
    real_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        real_start(thread)
        if thread.name == PRODUCER:
            raise SystemExit(17)  # after the start, before it is recorded as started

    monkeypatch.setattr(threading.Thread, "start", start)
    joined = joining(monkeypatch)
    real_decide = transfer_cli._Selection._decide

    def decide(*arguments: Any, **options: Any) -> Any:
        if in_producer():
            joined.wait(PATIENCE)  # still deciding until the main thread waits for it
        return real_decide(*arguments, **options)

    monkeypatch.setattr(transfer_cli._Selection, "_decide", decide)
    plan = jobs(near, 4)
    with pytest.raises(SystemExit) as caught:
        run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    assert caught.value.code == 17  # never replaced
    assert producers() == [] and _owners(near) == []  # waited for: it had started
    assert held_ids(near) == []
    assert_in(near, plan)


# -- a failure of SRC withdraws a refusal's take-back; a leaving exception survives; a completed run (I1-I3) -------


def test_a_failed_take_back_of_the_refused_page_keeps_the_page_held_ahead_held(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    paged(monkeypatch, 2)
    first_stage_after(monkeypatch, page_held(monkeypatch, 2))  # the second page held whole before the refusal
    take_backs: list[int] = []
    real_take_back = transfer_cli._take_back

    def take_back(*arguments: Any, **options: Any) -> Any:
        take_backs.append(1)
        if len(take_backs) == 1:
            raise OSError(errno.EIO, "SRC failed while the refused page was taken back")
        return real_take_back(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_take_back", take_back)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert take_backs == [1]  # the page held ahead is never taken back after that failure
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 4
    assert all(outcome["message"].endswith(f"it stays held on {near.root}") for outcome in transfers[2:])
    assert held_ids(near) == ids_of(plan)


def _interrupted_here(landed: list[int]) -> None:
    """Send a Ctrl-C and spin (each turn lets a pending handler run) until it lands here, recording that it did."""

    deadline = time.monotonic() + PATIENCE
    try:
        _thread.interrupt_main()
        while time.monotonic() < deadline:
            pass
    except KeyboardInterrupt:
        landed.append(1)
        raise


def test_a_ctrl_c_while_reporting_never_replaces_the_exception_leaving_the_run(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, _far, far_name = push
    paged(monkeypatch, 2)
    second = page_held(monkeypatch, 2)

    def drive(*arguments: Any, **options: Any) -> Any:
        assert second.wait(PATIENCE)
        raise SystemExit(17)  # leaving the run while the second page is held ahead

    landed: list[int] = []
    real_unsent = transfer_cli._Transfer.unsent

    def unsent(*arguments: Any, **options: Any) -> None:
        _interrupted_here(landed)  # Ctrl-C while the page held ahead is reported, after the join
        real_unsent(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_drive", drive)
    monkeypatch.setattr(transfer_cli._Transfer, "unsent", unsent)
    plan = jobs(near, 4)
    with default_sigint(), pytest.raises(SystemExit) as caught:
        run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    assert caught.value.code == 17 and landed == [1]
    assert producers() == [] and _owners(near) == []
    assert held_ids(near) == ids_of(plan)  # nothing taken back


def test_a_ctrl_c_let_pass_during_the_final_join_leaves_a_completed_run_completed(
    push: Ssh, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = push
    paged(monkeypatch, 2)
    real_stop = transfer_cli._Ahead.stop

    def stop(ahead: Any, **options: Any) -> Any:
        _thread.interrupt_main()  # Ctrl-C as the producer of a fully driven run is joined
        recorder = signal.getsignal(signal.SIGINT)
        assert isinstance(recorder, MethodType) and isinstance(recorder.__self__, _pool.DeferredInterrupts)
        deferred = recorder.__self__
        deadline = time.monotonic() + PATIENCE
        while not deferred.signals:  # recorded by the stop's deferral
            assert time.monotonic() < deadline
        return real_stop(ahead, **options)

    monkeypatch.setattr(transfer_cli._Ahead, "stop", stop)
    plan = jobs(near, 4)
    seen: list[int] = []
    previous = signal.signal(signal.SIGINT, lambda _signum, _frame: seen.append(1))  # lets the interrupt pass
    try:
        code = run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}")
    finally:
        signal.signal(signal.SIGINT, previous)
    assert seen == [1] and code == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 4
    assert outgoing(near) == []
    assert_in(far, plan)
