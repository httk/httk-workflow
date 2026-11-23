"""Tests of :func:`httk.workflow._pool.run_ordered`: order, progressive emission, failures, admission, interrupts."""

import _thread
import contextvars
import random
import signal
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from httk.workflow import _pool
from httk.workflow._pool import run_ordered

#: How long a test waits for something that should happen at once before calling it a failure.
PATIENCE = 10.0


class Boom(Exception):
    pass


@pytest.fixture
def pools(monkeypatch: pytest.MonkeyPatch) -> list[_pool._Pool[Any, Any]]:
    """Every pool created in the test, recorded as it installs its handler (before any item runs)."""

    created: list[_pool._Pool[Any, Any]] = []
    install = _pool._Pool.install

    def recording(pool: _pool._Pool[Any, Any]) -> object:
        created.append(pool)
        return install(pool)

    monkeypatch.setattr(_pool._Pool, "install", recording)
    return created


def _poll(condition: Callable[[], object]) -> None:
    """Wait, off the main thread, until *condition* holds (or PATIENCE ran out): for a flag nothing notifies."""

    deadline = time.monotonic() + PATIENCE
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.001)


def test_results_come_back_in_input_order_whatever_order_items_finish() -> None:
    def slow_first(item: int) -> int:
        time.sleep(0.02 * (8 - item))
        return item * item

    assert run_ordered(slow_first, list(range(8)), 8) == [item * item for item in range(8)]


def test_emit_runs_in_the_calling_thread_in_order_while_later_items_still_run() -> None:
    emitted: list[tuple[int, int]] = []
    first_emitted = threading.Event()
    caller = threading.get_ident()
    threads: set[int] = set()

    def work(item: int) -> int:
        threads.add(threading.get_ident())
        if item == 1:
            # Item 1 finishes only once item 0 was emitted: emission does not wait for the whole batch.
            assert first_emitted.wait(PATIENCE), "item 0 was not emitted while item 1 still ran"
        return item

    def emit(index: int, result: int) -> None:
        assert threading.get_ident() == caller
        emitted.append((index, result))
        if index == 0:
            first_emitted.set()

    assert run_ordered(work, [0, 1, 2, 3], 4, emit) == [0, 1, 2, 3]
    assert emitted == [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert caller not in threads


def test_workers_1_is_the_plain_loop_in_the_calling_thread() -> None:
    calls: list[tuple[str, int, int]] = []
    caller = threading.get_ident()

    def work(item: int) -> int:
        assert threading.get_ident() == caller
        calls.append(("work", item, item))
        return item + 1

    result = run_ordered(work, [5, 6, 7], 1, lambda index, value: calls.append(("emit", index, value)))
    assert result == [6, 7, 8]
    assert calls == [("work", 5, 5), ("emit", 0, 6), ("work", 6, 6), ("emit", 1, 7), ("work", 7, 7), ("emit", 2, 8)]

    def fail(_item: int) -> int:
        raise Boom

    with pytest.raises(Boom):
        run_ordered(fail, [1, 2], 1)


def test_the_first_failure_in_input_order_is_raised_after_the_completed_prefix_is_emitted() -> None:
    later_failed, two_running = threading.Event(), threading.Event()
    finished: set[int] = set()
    done = threading.Condition()

    def work(item: int) -> int:
        if item == 2:
            two_running.set()
            # Fails only after item 3 failed: the earlier one in input order is still the one raised.
            assert later_failed.wait(PATIENCE)
            raise Boom("two")
        if item == 3:
            # Fails only once items 0 and 1 finished and item 2 runs: no item starts after a failure, so a worker
            # that took one of them late would otherwise find the pool already stopped.
            with done:
                assert done.wait_for(lambda: {0, 1} <= finished, PATIENCE)
            assert two_running.wait(PATIENCE)
            later_failed.set()
            raise Boom("three")
        with done:
            finished.add(item)
            done.notify_all()
        return item

    emitted: list[int] = []
    with pytest.raises(Boom, match="two"):
        run_ordered(work, [0, 1, 2, 3], 4, lambda index, _value: emitted.append(index))
    assert emitted == [0, 1]
    assert finished == {0, 1}


def test_no_item_starts_after_the_stop_flag(pools: list[_pool._Pool[Any, Any]]) -> None:
    started: list[int] = []
    second = threading.Event()

    def work(item: int) -> int:
        started.append(item)
        if item == 0:
            assert second.wait(PATIENCE)
            raise Boom
        if item == 1:
            second.set()
            # Until the failure of item 0 set the stop flag (its worker notifies under the condition), so that
            # this worker takes no other item before.
            pool = pools[0]
            with pool.condition:
                assert pool.condition.wait_for(lambda: pool.stop, PATIENCE)
        return item

    with pytest.raises(Boom):
        run_ordered(work, list(range(10)), 2)
    assert sorted(started) == [0, 1]


def test_an_emit_failure_stops_the_batch_and_is_raised_after_the_drain() -> None:
    finished: Counter[int] = Counter()
    started, release = threading.Event(), threading.Event()

    def work(item: int) -> int:
        if item == 1:
            started.set()
            assert release.wait(PATIENCE)
        finished[item] += 1
        return item

    def emit(index: int, _value: int) -> None:
        if index == 0:
            # Item 1 must be running before the failure: no item starts after the stop flag.
            assert started.wait(PATIENCE)
            release.set()
            raise Boom("emit")

    with pytest.raises(Boom, match="emit"):
        run_ordered(work, list(range(6)), 2, emit)
    # Item 1 ran while item 0 was emitted; it finished before the call raised.
    assert finished[1] == 1 and all(count == 1 for count in finished.values())


@pytest.mark.parametrize("failing_start", [2, 4])
def test_a_failed_thread_start_leaves_the_items_to_the_started_threads(
    monkeypatch: pytest.MonkeyPatch, failing_start: int
) -> None:
    original = threading.Thread.start
    starts = Counter[str]()

    def start(thread: threading.Thread) -> None:
        starts["calls"] += 1
        if starts["calls"] == failing_start:
            raise RuntimeError("can't start new thread")
        original(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    runs: Counter[int] = Counter()
    threads: set[int] = set()
    lock = threading.Lock()
    caller = threading.get_ident()

    def work(item: int) -> int:
        with lock:
            runs[item] += 1
            threads.add(threading.get_ident())
        time.sleep(0.005)
        return item

    assert run_ordered(work, list(range(40)), 8) == list(range(40))
    assert runs == Counter({item: 1 for item in range(40)})
    assert starts["calls"] == failing_start
    assert 1 <= len(threads) <= failing_start - 1 and caller not in threads


def test_without_any_thread_the_calling_thread_runs_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    def start(_thread: threading.Thread) -> None:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", start)
    caller = threading.get_ident()
    emitted: list[int] = []

    def work(item: int) -> int:
        assert threading.get_ident() == caller
        return item

    assert run_ordered(work, [1, 2, 3], 8, lambda index, _value: emitted.append(index)) == [1, 2, 3]
    assert emitted == [0, 1, 2]


def test_workers_see_the_callers_context_variables() -> None:
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="unset")
    token = variable.set("caller")
    try:
        assert run_ordered(lambda _item: variable.get(), list(range(6)), 3) == ["caller"] * 6
    finally:
        variable.reset(token)


@pytest.fixture
def default_sigint() -> Iterator[None]:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    yield
    signal.signal(signal.SIGINT, previous)


def test_two_real_interrupts_wait_for_every_started_item(
    default_sigint: None, pools: list[_pool._Pool[Any, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    items = list(range(12))
    started = {item: threading.Event() for item in items}
    finished = {item: False for item in items}
    release = threading.Event()

    def work(item: int) -> int:
        started[item].set()
        release.wait(PATIENCE)
        time.sleep(0.05)
        finished[item] = True
        return item

    main = threading.main_thread().ident
    assert main is not None

    def interrupt() -> None:
        for item in range(4):
            started[item].wait(PATIENCE)
        signal.pthread_kill(main, signal.SIGINT)
        # Until the caller is draining and said what it waits for: workers are still parked, none took item 4.
        _poll(lambda: pools[0].noted)
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)  # the second interrupt is absorbed by the drain, before or after the release
        release.set()

    helper = threading.Thread(target=interrupt)
    helper.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            run_ordered(work, items, 4)
        ran = [item for item in items if started[item].is_set()]
        assert ran == [0, 1, 2, 3]
        assert all(finished[item] for item in ran)
    finally:
        release.set()
        helper.join()
    assert capsys.readouterr().err.count("waiting for 4 running operations to finish") == 1


def test_an_interrupt_raised_by_emit_still_waits_and_emits_the_prefix(default_sigint: None) -> None:
    started: set[int] = set()
    finished: set[int] = set()
    emitted: list[int] = []

    def work(item: int) -> int:
        started.add(item)
        time.sleep(0.05 if item else 0)
        finished.add(item)
        return item

    def emit(index: int, _value: int) -> None:
        emitted.append(index)
        if index == 0:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_ordered(work, [0, 1, 2], 3, emit)
    # Whichever items were admitted before the stop finished, and their completed prefix was emitted.
    assert 0 in started and finished == started
    assert emitted == list(range(len(emitted))) and set(emitted) <= finished


def _fire(main: int, count: int, *, low: float = 20e-6, high: float = 120e-6) -> None:
    for index in range(count):
        if index:
            spin = time.perf_counter() + random.uniform(low, high)
            while time.perf_counter() < spin:
                pass
        signal.pthread_kill(main, signal.SIGINT)


@pytest.mark.parametrize("burst", [2, 6])
def test_bursts_of_real_interrupts_never_leave_an_item_running(default_sigint: None, burst: int) -> None:
    main = threading.main_thread().ident
    assert main is not None
    rounds: list[tuple[set[int], set[int]]] = []
    handlers: set[bool] = set()
    for _ in range(150):
        started: set[int] = set()
        finished: set[int] = set()
        go = threading.Event()

        def work(
            item: int, started: set[int] = started, finished: set[int] = finished, go: threading.Event = go
        ) -> int:
            # While the pool lives, SIGINT is the pool's: an interrupt cannot surface mid-bookkeeping.
            handlers.add(signal.getsignal(signal.SIGINT) is signal.default_int_handler)
            started.add(item)
            go.wait(PATIENCE)
            finished.add(item)
            return item

        def fire(started: set[int] = started, go: threading.Event = go) -> None:
            deadline = time.monotonic() + PATIENCE
            while not started and time.monotonic() < deadline:
                time.sleep(0.0001)
            time.sleep(random.uniform(0, 0.001))
            _fire(main, burst, low=0.0)
            go.set()

        helper = threading.Thread(target=fire)
        helper.start()
        try:
            with pytest.raises(KeyboardInterrupt):
                run_ordered(work, list(range(8)), 4)
            # When the call raises, every item that started has finished.
            snapshot = set(started)
            assert snapshot == finished and snapshot
        finally:
            go.set()
            helper.join()
        rounds.append((started, snapshot))
    time.sleep(0.05)
    # And no item started afterwards.
    assert all(started == snapshot for started, snapshot in rounds)
    assert handlers == {False} and signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_a_failing_stderr_during_two_interrupts_still_ends_in_keyboard_interrupt(
    default_sigint: None, monkeypatch: pytest.MonkeyPatch, pools: list[_pool._Pool[Any, Any]]
) -> None:
    class Broken:
        def write(self, _text: str) -> int:
            raise BrokenPipeError

        def flush(self) -> None:
            raise BrokenPipeError

    monkeypatch.setattr(sys, "stderr", Broken())
    main = threading.main_thread().ident
    assert main is not None
    started: set[int] = set()
    finished: set[int] = set()
    release = threading.Event()

    def work(item: int) -> int:
        started.add(item)
        release.wait(PATIENCE)
        time.sleep(0.02)
        finished.add(item)
        return item

    def interrupt() -> None:
        deadline = time.monotonic() + PATIENCE
        while len(started) < 4 and time.monotonic() < deadline:
            time.sleep(0.001)
        signal.pthread_kill(main, signal.SIGINT)
        _poll(lambda: pools[0].noted)  # the drain has tried to say what it waits for (noted before printing)
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.1)
        release.set()

    helper = threading.Thread(target=interrupt)
    helper.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            run_ordered(work, list(range(12)), 4)
        assert started == finished == {0, 1, 2, 3}
    finally:
        release.set()
        helper.join()
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_a_previous_custom_handler_is_called_once_and_the_rest_runs_here(
    pools: list[_pool._Pool[Any, Any]],
) -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    calls: list[int] = []

    def custom(signum: int, _frame: object) -> None:
        calls.append(signum)

    previous = signal.signal(signal.SIGINT, custom)
    try:
        main = threading.main_thread().ident
        assert main is not None
        release = threading.Event()
        caller = threading.get_ident()
        where: dict[int, bool] = {}

        def work(item: int) -> int:
            where[item] = threading.get_ident() == caller
            if item < 3:
                release.wait(PATIENCE)
            return item * 2

        def interrupt() -> None:
            deadline = time.monotonic() + PATIENCE
            while len(where) < 3 and time.monotonic() < deadline:
                time.sleep(0.001)
            _fire(main, 3)
            # Until the pool recorded the interrupt: released earlier, a worker would take item 3.
            _poll(lambda: pools[0].stop)
            release.set()

        helper = threading.Thread(target=interrupt)
        helper.start()
        try:
            emitted: list[int] = []
            assert run_ordered(work, list(range(10)), 3, lambda index, _v: emitted.append(index)) == [
                item * 2 for item in range(10)
            ]
        finally:
            release.set()
            helper.join()
        assert calls == [signal.SIGINT] and emitted == list(range(10))
        # The parked items ran in the pool; once the handler let the interrupt pass, the rest ran here.
        assert not any(where[item] for item in range(3)) and all(where[item] for item in range(3, 10))
        assert signal.getsignal(signal.SIGINT) is custom
    finally:
        signal.signal(signal.SIGINT, previous)


def test_off_the_main_thread_an_interrupt_from_emit_still_drains() -> None:
    finished: set[int] = set()
    started: set[int] = set()
    outcome: list[BaseException] = []
    handler = signal.getsignal(signal.SIGINT)

    def work(item: int) -> int:
        started.add(item)
        time.sleep(0.03)
        finished.add(item)
        return item

    def emit(_index: int, _value: int) -> None:
        raise KeyboardInterrupt

    def call() -> None:
        try:
            run_ordered(work, list(range(8)), 4, emit)
        except BaseException as exc:
            outcome.append(exc)
            outcome.append(RuntimeError(str(started == finished)))

    thread = threading.Thread(target=call)
    thread.start()
    thread.join(PATIENCE)
    assert isinstance(outcome[0], KeyboardInterrupt) and str(outcome[1]) == "True"
    assert signal.getsignal(signal.SIGINT) is handler


def test_a_failure_is_raised_with_its_own_cause() -> None:
    def work(item: int) -> int:
        if item == 3:
            try:
                raise KeyError(item)
            except KeyError as exc:
                raise Boom("three") from exc
        return item

    with pytest.raises(Boom) as raised:
        run_ordered(work, list(range(6)), 3)
    assert isinstance(raised.value.__cause__, KeyError)


def test_on_quiet_is_called_once_whatever_the_ending() -> None:
    calls: list[str] = []
    assert run_ordered(lambda item: item, [1, 2, 3], 3, on_quiet=lambda: calls.append("pool")) == [1, 2, 3]
    assert run_ordered(lambda item: item, [1], 3, on_quiet=lambda: calls.append("plain")) == [1]

    def fail(_item: int) -> int:
        raise Boom

    with pytest.raises(Boom):
        run_ordered(fail, [1, 2, 3], 3, on_quiet=lambda: calls.append("failed"))
    assert calls == ["pool", "plain", "failed"]


@pytest.fixture
def custom_sigint() -> Iterator[list[int]]:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    calls: list[int] = []

    def custom(signum: int, _frame: object) -> None:
        calls.append(signum)

    previous = signal.signal(signal.SIGINT, custom)
    yield calls
    signal.signal(signal.SIGINT, previous)


def test_the_handler_is_restored_when_on_quiet_raises(default_sigint: None) -> None:
    def on_quiet() -> None:
        raise Boom("quiet")

    with pytest.raises(Boom, match="quiet"):
        run_ordered(lambda item: item, list(range(6)), 3, on_quiet=on_quiet)
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_the_handler_is_restored_when_an_exception_escapes_before_the_drain(
    default_sigint: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def quiesce(_pool: object) -> None:
        raise Boom("escaped")

    monkeypatch.setattr(_pool._Pool, "quiesce", quiesce)
    quiet: list[bool] = []
    with pytest.raises(Boom, match="escaped"):
        run_ordered(lambda item: item, list(range(6)), 3, on_quiet=lambda: quiet.append(True))
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    assert quiet == []  # nothing said the pool was quiet: a caller's flight stays, its owner fail-stops


def _interrupting_emit(main: int) -> Callable[[int, int], None]:
    def emit(index: int, _value: int) -> None:
        if index == 0:
            signal.pthread_kill(main, signal.SIGINT)  # recorded by the pool's handler
            time.sleep(0.01)
            raise ValueError("emit")

    return emit


def test_a_recorded_interrupt_wins_over_an_emit_failure(default_sigint: None) -> None:
    main = threading.main_thread().ident
    assert main is not None
    with pytest.raises(KeyboardInterrupt) as raised:
        run_ordered(lambda item: item, list(range(6)), 3, _interrupting_emit(main))
    assert isinstance(raised.value.__context__, ValueError)


def test_a_custom_handler_is_called_once_before_an_emit_failure_is_raised(custom_sigint: list[int]) -> None:
    main = threading.main_thread().ident
    assert main is not None
    with pytest.raises(ValueError, match="emit"):
        run_ordered(lambda item: item, list(range(6)), 3, _interrupting_emit(main))
    assert custom_sigint == [signal.SIGINT]


@pytest.mark.parametrize("absorbed", [SystemExit, KeyboardInterrupt])
def test_an_interrupt_absorbed_while_draining_wins_over_an_item_failure(
    monkeypatch: pytest.MonkeyPatch, absorbed: type[BaseException]
) -> None:
    real = _pool._Pool.drain
    calls: list[int] = []

    def drain(pool: Any) -> None:
        calls.append(1)
        if len(calls) == 1:
            raise absorbed  # as an interrupt landing in the drain: quiesce absorbs it and drains again
        real(pool)

    monkeypatch.setattr(_pool._Pool, "drain", drain)
    started: set[int] = set()
    finished: set[int] = set()
    outcome: list[BaseException] = []

    def work(item: int) -> int:
        started.add(item)
        if item == 0:
            raise Boom("item")
        time.sleep(0.05)
        finished.add(item)
        return item

    def call() -> None:  # off the main thread: no SIGINT handler, the absorbing path
        try:
            run_ordered(work, list(range(8)), 4)
        except BaseException as exc:
            outcome.append(exc)
            outcome.append(RuntimeError(str(started - {0} == finished)))

    thread = threading.Thread(target=call)
    thread.start()
    thread.join(PATIENCE)
    assert isinstance(outcome[0], absorbed) and isinstance(outcome[0].__context__, Boom)
    assert str(outcome[1]) == "True" and len(calls) == 2


def test_on_quiet_waits_for_every_running_item() -> None:
    started: set[int] = set()
    finished: set[int] = set()
    seen: list[bool] = []
    release, another = threading.Event(), threading.Event()

    def work(item: int) -> int:
        started.add(item)
        if item:
            another.set()
            release.wait(PATIENCE)
            time.sleep(0.02)
        finished.add(item)
        return item

    def emit(index: int, _value: int) -> None:
        if index == 0:
            # Another item must be running before the failure: no item starts after the stop flag.
            assert another.wait(PATIENCE)
            threading.Timer(0.2, release.set).start()
            raise ValueError("emit")

    with pytest.raises(ValueError, match="emit"):
        run_ordered(work, list(range(8)), 4, emit, on_quiet=lambda: seen.append(started == finished))
    assert seen == [True] and len(started) > 1


def test_on_quiet_is_called_when_the_pool_cannot_even_install(monkeypatch: pytest.MonkeyPatch) -> None:
    def install(_pool: object) -> object:
        raise KeyboardInterrupt  # as a SIGINT handled just before the pool's handler took over

    monkeypatch.setattr(_pool._Pool, "install", install)
    quiet: list[bool] = []
    with pytest.raises(KeyboardInterrupt):
        run_ordered(lambda item: item, list(range(6)), 3, on_quiet=lambda: quiet.append(True))
    assert quiet == [True]


# -- Feed: tasks submitted one at a time by a reading thread --------------------------------------------------------


class Task:
    """A submitted task: its number and a payload whose lifetime the tests follow."""

    def __init__(self, number: int, size: int = 0) -> None:
        self.number = number
        self.payload = bytes(size)


class Ledger:
    """What a feed's body and discard saw: each task's runs (and threads) and discards."""

    def __init__(self, delay: float = 0.0, failing: int | None = None) -> None:
        self.lock = threading.Lock()
        self.runs: Counter[int] = Counter()
        self.threads: dict[int, int] = {}
        self.discarded: Counter[int] = Counter()
        self.delay = delay
        self.failing = failing

    def body(self, task: Task) -> None:
        with self.lock:
            self.runs[task.number] += 1
            self.threads[task.number] = threading.get_ident()
        time.sleep(self.delay)
        if task.number == self.failing:
            raise Boom(task.number)

    def discard(self, task: Task) -> None:
        with self.lock:
            self.discarded[task.number] += 1


def feeding(feed: _pool.Feed[Task], count: int, weight: int = 1, size: int = 0) -> Callable[[], bool]:
    """A reader submitting *count* tasks, each after waiting for room; called again, it goes on where it stopped."""

    position = [0]

    def reader() -> bool:
        while position[0] < count:
            if not feed.wait(lambda: feed.room(weight)):
                return False
            feed.submit(Task(position[0], size), weight)
            position[0] += 1
        return True

    return reader


def test_a_feed_runs_every_task_once_within_its_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = Ledger(delay=0.002)
    feed = _pool.Feed(ledger.body, ledger.discard, max_tasks=3, max_weight=10)
    peaks: list[tuple[int, int]] = []
    real_submit = _pool.Feed.submit

    def submit(self: _pool.Feed[Task], task: Task, weight: int) -> int:
        index = real_submit(self, task, weight)
        with self.condition:
            peaks.append((self.pending, self.pending_weight))
        return index

    monkeypatch.setattr(_pool.Feed, "submit", submit)
    feed.run(feeding(feed, 40, weight=4), 4)
    assert ledger.runs == Counter({number: 1 for number in range(40)}) and not ledger.discarded
    assert max(pending for pending, _ in peaks) == 2 and max(weight for _, weight in peaks) == 8  # 3 tasks weigh 12
    assert feed.idle() and not feed.tasks and all(feed.finished(index) for index in range(40))
    assert threading.get_ident() not in ledger.threads.values()


def test_room_admits_one_task_of_any_weight_when_nothing_is_pending() -> None:
    feed: _pool.Feed[Task] = _pool.Feed(lambda _task: None, lambda _task: None, max_tasks=2, max_weight=10)
    assert feed.room(1000)
    feed.pending, feed.pending_weight = 1, 5
    assert feed.room(5) and not feed.room(6)
    feed.pending = 2
    assert not feed.room(0)


def test_a_failed_task_stops_the_feed_before_it_settles_and_untaken_tasks_are_discarded_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = Ledger(delay=0.01, failing=3)
    feed = _pool.Feed(ledger.body, ledger.discard, max_tasks=8, max_weight=1 << 30)
    settled: list[tuple[int, bool]] = []
    real_settle = _pool.Feed._settle

    def settle(self: _pool.Feed[Task], index: int, weight: int) -> None:
        settled.append((index, self.stop))
        real_settle(self, index, weight)

    monkeypatch.setattr(_pool.Feed, "_settle", settle)
    with pytest.raises(Boom):
        feed.run(feeding(feed, 60), 2)
    assert (3, True) in settled
    submitted = feed.submitted
    assert ledger.discarded and set(ledger.runs) | set(ledger.discarded) == set(range(submitted))
    assert not set(ledger.runs) & set(ledger.discarded) and max((ledger.runs + ledger.discarded).values()) == 1
    assert not feed.tasks and submitted < 60


def test_tasks_submitted_before_the_reader_fails_still_run_and_its_failure_is_raised() -> None:
    ledger = Ledger(delay=0.02)
    feed = _pool.Feed(ledger.body, ledger.discard, max_tasks=8, max_weight=1 << 30)
    submit = feeding(feed, 6)

    def reader() -> bool:
        submit()
        raise ValueError("refused")

    with pytest.raises(ValueError, match="refused"):
        feed.run(reader, 2)
    assert ledger.runs == Counter({number: 1 for number in range(6)}) and not ledger.discarded


def test_without_any_thread_a_feed_runs_every_task_in_the_calling_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    def start(_thread: threading.Thread) -> None:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", start)
    ledger = Ledger()
    feed = _pool.Feed(ledger.body, ledger.discard, max_tasks=2, max_weight=10)
    feed.run(feeding(feed, 10), 4)
    assert ledger.runs == Counter({number: 1 for number in range(10)})
    assert set(ledger.threads.values()) == {threading.get_ident()}


def test_a_feed_keeps_no_task_once_a_worker_took_it(monkeypatch: pytest.MonkeyPatch) -> None:
    import weakref

    ledger = Ledger(delay=0.001)
    feed = _pool.Feed(ledger.body, ledger.discard, max_tasks=8, max_weight=1 << 30)
    alive: list[weakref.ref[Task]] = []
    peak = [0]
    real_submit = _pool.Feed.submit

    def submit(self: _pool.Feed[Task], task: Task, weight: int) -> int:
        alive.append(weakref.ref(task))
        index = real_submit(self, task, weight)
        peak[0] = max(peak[0], sum(ref() is not None for ref in alive))
        return index

    monkeypatch.setattr(_pool.Feed, "submit", submit)
    feed.run(feeding(feed, 300, size=64 << 10), 2)
    # The pending tasks, the two running and the reader's own: never the whole stream.
    assert peak[0] <= 8 + 2 + 2
    assert not feed.tasks and all(ref() is None for ref in alive)


def test_a_worker_starting_late_takes_nothing_once_the_feed_resumed_inline(monkeypatch: pytest.MonkeyPatch) -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    main = threading.main_thread().ident
    assert main is not None
    caller = threading.get_ident()
    late, release = threading.Event(), threading.Event()
    parked = [threading.Event(), threading.Event()]
    real_work = _pool.Feed.work
    entered = Counter[str]()

    def work(self: _pool.Feed[Task]) -> None:
        entered["threads"] += 1
        if entered["threads"] == 3:
            late.wait(PATIENCE)  # started, but enters the loop only after the feed resumed inline
        real_work(self)

    class Parked(Ledger):
        def body(self, task: Task) -> None:
            if task.number < 2:
                parked[task.number].set()
                release.wait(PATIENCE)
            elif threading.get_ident() == caller:
                time.sleep(0.02)  # the inline tasks give a late worker every chance to take one
            super().body(task)

    ledger = Parked()
    feed = _pool.Feed(ledger.body, ledger.discard, max_tasks=6, max_weight=1 << 30)
    monkeypatch.setattr(_pool.Feed, "work", work)
    calls: list[int] = []

    def custom(signum: int, _frame: object) -> None:
        calls.append(signum)
        late.set()

    def interrupt() -> None:
        for event in parked:  # both workers run their parked task
            event.wait(PATIENCE)
        signal.pthread_kill(main, signal.SIGINT)
        # Until the feed recorded the interrupt: released earlier, a worker would take task 2.
        _poll(lambda: feed.stop)
        release.set()

    previous = signal.signal(signal.SIGINT, custom)
    helper = threading.Thread(target=interrupt)
    helper.start()
    try:
        feed.run(feeding(feed, 30), 3)
    finally:
        signal.signal(signal.SIGINT, previous)
        late.set()
        release.set()
        helper.join()
    time.sleep(0.1)
    assert calls == [signal.SIGINT]
    assert ledger.runs == Counter({number: 1 for number in range(30)}) and not ledger.discarded
    # Only the two parked tasks ran in workers; the rest ran here once the handler let the interrupt pass.
    assert {number for number, thread in ledger.threads.items() if thread != caller} == {0, 1}


# -- the helpers of a producer the caller joins (plan WORKFLOW_TRANSFER_SESSION §7.11, T6b) --------------------------


def test_interrupt_of_finds_the_interrupt_behind_a_lock_error() -> None:
    interrupt = KeyboardInterrupt()
    wrapped = RuntimeError("cannot release un-acquired lock")
    wrapped.__context__ = interrupt
    assert _pool.interrupt_of(wrapped) is interrupt
    plain = RuntimeError("can't start new thread")
    assert _pool.interrupt_of(plain) is plain
    other = ValueError("x")
    other.__context__ = interrupt
    assert _pool.interrupt_of(other) is other


class _Finisher:
    """A thread that sets ``state["done"]`` once the waiter asked it *asks* times how many operations run (a wait
    that polled that often, interrupts included, is the precondition of what is tested)."""

    def __init__(self, condition: threading.Condition, asks: int, count: int, on_ask: Callable[[int], None]) -> None:
        self.condition, self.asks, self.count, self.on_ask = condition, asks, count, on_ask
        self.done = False
        self.asked = 0
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._finish)
        self.thread.start()

    def running(self) -> int:
        self.asked += 1
        self.on_ask(self.asked)
        if self.asked >= self.asks:
            self.ready.set()
        return self.count

    def _finish(self) -> None:
        assert self.ready.wait(10.0)
        with self.condition:
            self.done = True
            self.condition.notify_all()


@pytest.mark.parametrize("interrupted", [False, True])
def test_wait_quietly_waits_and_says_so_only_after_an_interrupt(
    capsys: pytest.CaptureFixture[str], interrupted: bool
) -> None:
    condition = threading.Condition()
    finisher = _Finisher(condition, 1, 2, lambda _asked: None)  # done once the waiter polled with 2 running
    absorbed = _pool.wait_quietly(condition, lambda: finisher.done, finisher.running, interrupted=lambda: interrupted)
    finisher.thread.join()
    assert absorbed is None and finisher.done
    said = "waiting for 2 running operations to finish"
    assert capsys.readouterr().err.count(said) == (1 if interrupted else 0)


def test_wait_quietly_absorbs_interrupts_and_returns_the_first(
    default_sigint: None, capsys: pytest.CaptureFixture[str]
) -> None:
    condition = threading.Condition()

    def interrupt(asked: int) -> None:
        if asked <= 2:
            _thread.interrupt_main()  # a Ctrl-C from inside the wait, twice

    finisher = _Finisher(condition, 3, 1, interrupt)  # done only after both were absorbed
    absorbed = _pool.wait_quietly(condition, lambda: finisher.done, finisher.running)
    finisher.thread.join()
    assert isinstance(absorbed, KeyboardInterrupt) and finisher.done
    assert capsys.readouterr().err.count("waiting for 1 running operations to finish") == 1


def _until(condition: Callable[[], object]) -> None:
    """Spin (each turn lets a pending signal handler run) until *condition* holds."""

    deadline = time.monotonic() + 10.0
    while not condition():
        assert time.monotonic() < deadline


def test_deferred_interrupts_deliver_the_first_once_the_block_ends(default_sigint: None) -> None:
    reached = []
    with pytest.raises(KeyboardInterrupt), _pool.DeferredInterrupts() as deferred:
        _thread.interrupt_main()
        _until(lambda: deferred.signals > 0)  # recorded, not raised here
        _thread.interrupt_main()
        _until(lambda: deferred.signals > 1)
        reached.append(deferred.signals)
    assert reached == [2]  # both recorded, delivered once
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_deferred_interrupts_never_replace_an_interrupt_that_propagates(default_sigint: None) -> None:
    original = KeyboardInterrupt("the first")
    with pytest.raises(KeyboardInterrupt) as caught, _pool.DeferredInterrupts() as deferred:
        _thread.interrupt_main()
        _until(lambda: deferred.signals > 0)
        raise original
    assert caught.value is original


def test_deferred_interrupts_never_replace_a_propagating_system_exit(default_sigint: None) -> None:
    with pytest.raises(SystemExit) as caught, _pool.DeferredInterrupts() as deferred:
        _thread.interrupt_main()
        _until(lambda: deferred.signals > 0)
        raise SystemExit(17)
    assert caught.value.code == 17
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_deferred_interrupts_deliver_nothing_while_the_caller_cleans_up_after_an_exception(
    default_sigint: None,
) -> None:
    with _pool.DeferredInterrupts(propagating=LookupError("being handled")) as deferred:
        _thread.interrupt_main()
        _until(lambda: deferred.signals > 0)
    assert deferred.signals >= 1  # recorded, and dropped rather than raised over the caller's exception
