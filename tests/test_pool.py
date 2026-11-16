"""Tests of :func:`httk.workflow._pool.run_ordered`: order, progressive emission, failures, admission, interrupts."""

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
    later_failed = threading.Event()
    finished: set[int] = set()

    def work(item: int) -> int:
        if item == 2:
            # Fails only after item 3 failed: the earlier one in input order is still the one raised.
            assert later_failed.wait(PATIENCE)
            raise Boom("two")
        if item == 3:
            later_failed.set()
            raise Boom("three")
        finished.add(item)
        return item

    emitted: list[int] = []
    with pytest.raises(Boom, match="two"):
        run_ordered(work, [0, 1, 2, 3], 4, lambda index, _value: emitted.append(index))
    assert emitted == [0, 1]
    assert finished == {0, 1}


def test_no_item_starts_after_the_stop_flag() -> None:
    started: list[int] = []
    second, failing = threading.Event(), threading.Event()

    def work(item: int) -> int:
        started.append(item)
        if item == 0:
            assert second.wait(PATIENCE)
            failing.set()
            raise Boom
        if item == 1:
            second.set()
            # Long enough for the failure of item 0 to be recorded before this worker takes another item.
            assert failing.wait(PATIENCE)
            time.sleep(0.2)
        return item

    with pytest.raises(Boom):
        run_ordered(work, list(range(10)), 2)
    assert sorted(started) == [0, 1]


def test_an_emit_failure_stops_the_batch_and_is_raised_after_the_drain() -> None:
    finished: Counter[int] = Counter()
    release = threading.Event()

    def work(item: int) -> int:
        if item == 1:
            assert release.wait(PATIENCE)
        finished[item] += 1
        return item

    def emit(index: int, _value: int) -> None:
        if index == 0:
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
    default_sigint: None, capsys: pytest.CaptureFixture[str]
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
        time.sleep(0.2)  # the caller is now draining: workers are still parked
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)
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
    default_sigint: None, monkeypatch: pytest.MonkeyPatch
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
        time.sleep(0.25)  # the drain has tried to say what it waits for
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


def test_a_previous_custom_handler_is_called_once_and_the_rest_runs_here() -> None:
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
            time.sleep(0.2)
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
    release = threading.Event()

    def work(item: int) -> int:
        started.add(item)
        if item:
            release.wait(PATIENCE)
            time.sleep(0.02)
        finished.add(item)
        return item

    def emit(index: int, _value: int) -> None:
        if index == 0:
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
