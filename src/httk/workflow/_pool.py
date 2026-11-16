"""A bounded pool of threads for batch loops whose items touch disjoint jobs (plan: WORKFLOW_REMOTE_FILESYSTEM_COST §5).

Python releases the GIL in every filesystem call, so on a filesystem where each metadata call is a round trip the
waits of different jobs overlap. :func:`run_ordered` owns its threads and its queue instead of using
:class:`concurrent.futures.ThreadPoolExecutor`: an item is queued only after the threads that serve it started (a
failed start never strands a queued item), and completion is observed through counters under one
:class:`threading.Condition`, never through ``Thread.join``/``is_alive`` (an interrupted join can report a running
thread stopped).

On the main thread a pool replaces the ``SIGINT`` handler for its whole lifetime by one that only records the
interrupt and stops the pool, so a burst of interrupts cannot surface at an arbitrary point of the bookkeeping; the
recorded interrupt is delivered to the previous handler once every running item has finished.
"""

import contextlib
import contextvars
import queue
import signal
import sys
import threading
from collections.abc import Callable, Sequence
from types import FrameType

__all__ = ["run_ordered"]

#: How often a waiting caller looks at a stop recorded by the signal handler (which cannot notify).
_POLL = 0.1


class _Stopped(Exception):
    """The pool stopped (an item failed, or an interrupt was recorded) before every result was emitted."""


def _interrupt(exc: BaseException) -> BaseException:
    """The interrupt behind *exc*: an interrupt landing inside ``Condition.wait`` can surface as the RuntimeError of
    releasing a lock it left released."""

    if isinstance(exc, RuntimeError) and isinstance(exc.__context__, (KeyboardInterrupt, SystemExit)):
        return exc.__context__
    return exc


class _Pool[T, R]:
    def __init__(self, function: Callable[[T], R], items: Sequence[T], emit: Callable[[int, R], None] | None) -> None:
        self.function = function
        self.items = items
        self.emit = emit
        self.queue: queue.SimpleQueue[int | None] = queue.SimpleQueue()
        self.condition = threading.Condition()
        self.attempted = 0  # thread starts tried (each may have created a thread)
        self.started = 0  # thread starts that returned
        self.emitted = 0  # the caller's next index to emit
        self.signals = 0  # SIGINTs the pool's handler recorded
        self.interrupted = False  # any interrupt seen: the drain says once what it waits for
        self.absorbed: BaseException | None = None  # the first interrupt absorbed while draining
        self.noted = False
        # Guarded by the condition (stop is also set, as a plain store, by the signal handler and the caller): each
        # item's outcome (ok, value), workers inside work() (active), workers that left it (left), items running.
        self.outcomes: list[tuple[bool, object] | None] = [None] * len(items)
        self.stop = False
        self.active = 0
        self.left = 0
        self.running = 0

    def work(self) -> None:
        with self.condition:
            if self.stop:  # a thread whose start was interrupted, arriving after the pool stopped
                self.left += 1
                self.condition.notify_all()
                return
            self.active += 1
        try:
            while (index := self.queue.get()) is not None:
                with self.condition:
                    if self.stop:
                        return  # no item starts after the stop flag
                    self.running += 1
                try:
                    outcome: tuple[bool, object] = (True, self.function(self.items[index]))
                except BaseException as exc:
                    outcome = (False, exc)
                with self.condition:
                    self.outcomes[index] = outcome
                    self.running -= 1
                    if not outcome[0]:
                        self.stop = True
                    self.condition.notify_all()
        finally:
            with self.condition:
                self.active -= 1
                self.left += 1
                self.condition.notify_all()

    def on_sigint(self, _signum: int, _frame: FrameType | None) -> None:
        # Runs in the main thread between two bytecodes, maybe inside the caller's critical section: no lock.
        self.signals += 1
        self.interrupted = True
        self.stop = True
        for _ in range(self.attempted):
            self.queue.put(None)  # SimpleQueue.put is reentrant

    def install(self) -> object:
        """Replace the SIGINT handler (on the main thread, unless SIGINT is ignored): the previous one, else None."""

        if threading.current_thread() is not threading.main_thread():
            return None
        previous = signal.getsignal(signal.SIGINT)
        if previous is None or previous == signal.SIG_IGN:
            return None  # a handler not set from Python cannot be restored; an ignored SIGINT needs nothing
        signal.signal(signal.SIGINT, self.on_sigint)
        return previous

    def start(self, workers: int) -> int:
        """Start up to *workers* threads; how many started. Nothing is queued yet."""

        while self.attempted < workers:
            # Taken here, in the calling thread: report collection scopes (httk.core.report) apply in the workers.
            context = contextvars.copy_context()
            thread = threading.Thread(target=context.run, args=(self.work,), name="httk-pool", daemon=True)
            self.attempted += 1
            try:
                thread.start()
            except RuntimeError:
                break  # as under a login node's task limit: the threads started so far serve the queue
            self.started += 1
        return self.started

    def feed(self) -> None:
        for index in range(len(self.items)):
            self.queue.put(index)
        for _ in range(self.attempted):
            self.queue.put(None)

    def follow(self) -> None:
        """Emit each result in input order once the prefix before it is complete; raise once the pool stopped."""

        while self.emitted < len(self.items):
            with self.condition:
                while (outcome := self.outcomes[self.emitted]) is None and not (self.stop or self.left >= self.started):
                    self.condition.wait(_POLL)
            if outcome is None or not outcome[0]:
                raise _Stopped
            self.emit_next()

    def emit_next(self) -> None:
        index = self.emitted
        self.emitted += 1  # first: an interrupted emit is never repeated
        if self.emit is not None:
            self.emit(index, self.result(index))

    def result(self, index: int) -> R:
        outcome = self.outcomes[index]
        assert outcome is not None and outcome[0]
        return outcome[1]  # type: ignore[return-value]

    def drain(self) -> None:
        self.stop = True
        for _ in range(self.attempted):
            self.queue.put(None)  # wakes a worker waiting for an index; spare sentinels are harmless
        while True:
            with self.condition:
                if not self.active:
                    return
                self.condition.wait(_POLL)
                running = self.running
            if self.interrupted and running and not self.noted:
                self.noted = True
                # Best effort: a closed or failing stderr must not end the wait.
                with contextlib.suppress(BaseException):
                    print(f"waiting for {running} running operations to finish", file=sys.stderr)

    def quiesce(self) -> None:
        """Stop the pool and wait until no worker is inside an item, absorbing interrupts meanwhile."""

        while True:
            try:
                self.drain()
                return
            except BaseException as exc:
                interrupt = _interrupt(exc)
                if not isinstance(interrupt, (KeyboardInterrupt, SystemExit)):
                    raise
                self.interrupted = True
                self.absorbed = self.absorbed or interrupt

    def finish(self) -> None:
        """Emit the completed prefix, as the sequential loop would already have; a failure of *emit* ends it."""

        try:
            while self.emitted < len(self.items) and (outcome := self.outcomes[self.emitted]) is not None:
                if not outcome[0]:
                    return
                self.emit_next()
        except BaseException:
            return  # the first exception is the one raised

    def deliver(self, caught: BaseException | None, previous: object) -> list[R]:
        """After the drain and the handler's restoration: raise what stopped the pool, or finish in this thread.

        An interrupt wins: one raised in the caller, then one absorbed during the drain, then a recorded SIGINT;
        another exception raised meanwhile (an *emit* or item failure) becomes its ``__context__``.
        """

        first = None if caught is None or isinstance(caught, _Stopped) else _interrupt(caught)
        if isinstance(first, (KeyboardInterrupt, SystemExit)):
            raise first
        other = first if first is not None else self.failure()
        interrupt = self.absorbed
        if interrupt is None and self.signals and previous is signal.default_int_handler:
            interrupt = KeyboardInterrupt()
        if interrupt is not None:
            if other is not None and interrupt.__context__ is None:
                interrupt.__context__ = other
            raise interrupt
        if self.signals and previous == signal.SIG_DFL:
            signal.raise_signal(signal.SIGINT)
            raise KeyboardInterrupt  # not reached unless the default action did not end the process
        if self.signals and callable(previous):
            try:
                previous(signal.SIGINT, None)  # once, however many arrived
            except BaseException as exc:
                if other is not None and exc.__context__ is None:
                    exc.__context__ = other
                raise
        if other is not None:
            raise other  # as it was raised: an item failure keeps its own cause
        # Every item finished, or (no thread started, or a previous handler let the interrupt pass) the rest runs
        # here, as the plain loop would.
        while self.emitted < len(self.items):
            if self.outcomes[self.emitted] is None:
                self.outcomes[self.emitted] = (True, self.function(self.items[self.emitted]))
            self.emit_next()
        return [self.result(index) for index in range(len(self.items))]

    def failure(self) -> BaseException | None:
        """The first item failure in input order."""

        for outcome in self.outcomes:
            if outcome is not None and not outcome[0]:
                assert isinstance(outcome[1], BaseException)
                return outcome[1]
        return None


def run_ordered[T, R](
    function: Callable[[T], R],
    items: Sequence[T],
    workers: int,
    emit: Callable[[int, R], None] | None = None,
    *,
    on_quiet: Callable[[], None] | None = None,
) -> list[R]:
    """Call *function* on every item, up to *workers* at once, and return the results in input order.

    With ``workers <= 1`` or fewer than two items this is the plain loop (``emit`` after each item). Otherwise
    each item runs in a worker thread under a copy of the caller's :mod:`contextvars` context, and *emit* is
    called in the calling thread, in input order, as soon as the prefix of finished items allows. On the first
    item failure (in input order), an exception raised by *emit*, or an interrupt, no further item starts; the
    call waits until every running item has finished (saying once on standard error what it waits for), emits
    the completed prefix, and raises that first exception. On the main thread ``SIGINT`` is recorded by the
    pool for its lifetime and delivered to the previous handler after that wait: ``KeyboardInterrupt`` for the
    default handler; a custom handler is called once, and when it returns the remaining items run in the
    calling thread. An interrupt wins over another exception raised meanwhile, which becomes its context. When
    no thread can be started the calling thread runs the loop.

    Accepted limits: a pool nested in an item of a pool on the main thread defers the interrupt until the inner
    batch has finished (the inner pool records it and delivers it to the outer one's handler); and while a pool
    runs, Ctrl-C does not interrupt an *emit* that blocks (on a full pipe, say), since the interrupt is only
    recorded.

    :param function: The per-item work; it must not print (return what to print instead).
    :param items: The items, in the order results are returned and emitted.
    :param workers: The largest number of items running at once.
    :param emit: Called as ``emit(index, result)`` for each finished item, in input order.
    :param on_quiet: Called once no item runs in another thread and none can start there any more; if the call
        returns or raises without having called it, an exception (from another signal's handler) escaped
        before the wait, and items may still run.
    :return: The results, in input order.
    """

    if workers <= 1 or len(items) < 2:
        try:
            results: list[R] = []
            for index, item in enumerate(items):
                results.append(function(item))
                if emit is not None:
                    emit(index, results[-1])
            return results
        finally:
            if on_quiet is not None:
                on_quiet()
    quieted = False

    def quiet() -> None:
        nonlocal quieted
        if on_quiet is not None and not quieted:
            quieted = True
            on_quiet()

    previous: object = None
    caught: BaseException | None = None
    try:
        try:
            pool = _Pool(function, items, emit)
            previous = pool.install()
        except BaseException:
            quiet()  # nothing ran
            raise
        try:
            if pool.start(min(workers, len(items))):
                pool.feed()
                pool.follow()
        except BaseException as exc:
            pool.stop = True
            caught = exc
            pool.interrupted = pool.interrupted or isinstance(_interrupt(exc), (KeyboardInterrupt, SystemExit))
        pool.quiesce()
        quiet()  # before the handler goes back: a SIGINT now is still recorded, not raised
        if caught is not None:
            pool.finish()
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)  # type: ignore[arg-type]  # one SIGINT pending now is still ours
    return pool.deliver(caught, previous)
