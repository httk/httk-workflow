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

__all__ = ["Feed", "run_ordered"]

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


class Feed[T](_Pool[int, None]):
    """Tasks the calling thread submits one at a time (the reader of a stream), run by worker threads.

    The scheme is :func:`run_ordered`'s: the threads start before anything is queued, a task is run exactly once
    by the worker that takes it, completion is observed through counters under the one condition, a failed task
    stops the feed, and on the main thread ``SIGINT`` is recorded while the feed lives and delivered once every
    running task has finished. Every wait (:meth:`wait`) is a loop on that condition that also ends once the feed
    stopped or every worker left, so a failure always wakes the calling thread. When no thread can be started,
    :meth:`submit` runs each task in the calling thread.

    A task belongs to the feed from the moment :meth:`submit` is called: *body* releases its resources when it runs
    it, and *discard* releases them, in the calling thread after every worker stopped, when it never ran. The feed
    drops its reference to a task as soon as a worker takes it (or it is discarded), so a long stream keeps only
    the pending tasks' data alive.

    :param body: Runs one task, releasing its resources whatever happens.
    :param discard: Releases the resources of a task that never ran.
    :param max_tasks: How many submitted tasks may be unfinished at once (see :meth:`room`).
    :param max_weight: How much weight submitted tasks may have unfinished at once (see :meth:`room`).
    """

    def __init__(
        self, body: Callable[[T], None], discard: Callable[[T], None], *, max_tasks: int, max_weight: int
    ) -> None:
        super().__init__(self._serve, (), None)
        # A worker runs function(items[index]): with the indices themselves as items, _serve gets the index.
        self.items = range(sys.maxsize)
        self.body = body
        self.discard = discard
        self.max_tasks = max_tasks
        self.max_weight = max_weight
        # Guarded by the condition: the submitted tasks no one took yet (task, weight) by index, the indices of the
        # unfinished ones, their count and weight, and how many were submitted.
        self.tasks: dict[int, tuple[T, int]] = {}
        self.unsettled: set[int] = set()
        self.pending = 0
        self.pending_weight = 0
        self.submitted = 0
        self.inline = False  # tasks run in the calling thread: no thread started, or the feed resumed there
        # Resumed in the calling thread after a previous SIGINT handler returned: the stop flag stays set for the
        # workers (one starting late takes nothing), and no longer stops the calling thread (see stopped).
        self.resumed = False

    @property
    def stopped(self) -> bool:
        """Whether the calling thread must stop submitting: the feed stopped and was not resumed inline."""

        return self.stop and not self.resumed

    def _serve(self, index: int) -> None:
        with self.condition:
            claimed = self.tasks.pop(index, None)  # from here on the task is this thread's alone
        if claimed is None:  # pragma: no cover - a task is taken once; this only guards that
            return
        task, weight = claimed
        del claimed
        try:
            self.body(task)
        except BaseException:
            with self.condition:
                # Stopped before the task counts as finished: no waiter goes on past a failed task.
                self.stop = True
                self._settle(index, weight)
            raise
        finally:
            del task
        with self.condition:
            self._settle(index, weight)

    def _settle(self, index: int, weight: int) -> None:
        self.unsettled.discard(index)
        self.pending -= 1
        self.pending_weight -= weight
        self.condition.notify_all()

    def finished(self, index: int) -> bool:
        """Whether the task *index* finished; call it under :attr:`condition` (as :meth:`wait` does).

        :param index: The index :meth:`submit` returned.
        :return: Whether it finished (succeeded or failed).
        """

        return index not in self.unsettled

    def idle(self) -> bool:
        """Whether every submitted task finished; call it under :attr:`condition` (as :meth:`wait` does).

        :return: Whether nothing is pending.
        """

        return not self.pending

    def room(self, weight: int) -> bool:
        """Whether a task of *weight* may be submitted now; call it under :attr:`condition` (as :meth:`wait` does).

        :param weight: The task's weight.
        :return: Whether nothing is pending, or both bounds still hold with the task added.
        """

        return not self.pending or (self.pending < self.max_tasks and self.pending_weight + weight <= self.max_weight)

    def wait(self, ready: Callable[[], bool]) -> bool:
        """Wait under :attr:`condition` until *ready* holds, the feed stopped, or every worker left.

        :param ready: The predicate, called under the condition.
        :return: Whether *ready* held before the feed stopped (``False`` once it stopped, even when *ready* holds).
        """

        with self.condition:
            while not self.stopped:
                if ready():
                    return True
                if self.left >= self.started:
                    return False  # nobody is left to make progress
                self.condition.wait(_POLL)  # also how a stop recorded by the signal handler is seen
            return False

    def submit(self, task: T, weight: int) -> int:
        """Queue *task* for a worker, or run it here when the feed runs inline; it never waits for room.

        :param task: The task, which belongs to the feed from now on.
        :param weight: Its weight for :meth:`room`.
        :return: The task's index, for :meth:`finished`.
        """

        with self.condition:
            index = self.submitted
            self.tasks[index] = (task, weight)
            self.submitted += 1
            self.outcomes.append(None)
            self.unsettled.add(index)
            self.pending += 1
            self.pending_weight += weight
        del task
        if self.inline:
            self._run_here(index)
        else:
            self.queue.put(index)  # after a stop it is never taken, and discarded after the drain
        return index

    def _run_here(self, index: int) -> None:
        try:
            self._serve(index)
        except BaseException as exc:
            self.outcomes[index] = (False, exc)
            raise
        self.outcomes[index] = (True, None)

    def run(self, reader: Callable[[], bool], workers: int) -> None:
        """Start up to *workers* threads and call *reader*, which submits tasks; return once every task finished.

        *reader* returns ``True`` when it submitted everything, ``False`` when it saw the feed stop (:attr:`stopped`;
        it then stops before acting on anything more). The tasks submitted before *reader* returned or raised still
        run, so the failure raised is normally the first in submission order (a failed task's, else the reader's);
        a stop ends that wait at once, and a task a worker took just as another failed is then not run. Then, as in
        :func:`run_ordered`: every running task finishes (saying once on standard error what is waited for when
        interrupted), the tasks that never ran are discarded, and the first exception or the recorded interrupt is
        raised, an interrupt winning. An interrupt is seen by *reader* only between its waits, never inside its own
        work. When a previous custom ``SIGINT`` handler returns, the tasks that never ran run here, in order, and
        *reader* is called again to go on inline; but when *reader* had also failed, its failure is raised and the
        tasks that never ran are discarded, so the failure raised may be a later one in submission order than one
        those tasks would have met.

        :param reader: Submits the tasks, waiting with :meth:`wait` (for :meth:`room`, say) before each.
        :param workers: The most tasks running at once.
        :raises RuntimeError: If the feed stopped with neither a failure nor an interrupt (a defect).
        """

        previous: object = None
        caught: BaseException | None = None
        done = False
        try:
            try:
                previous = self.install()
                if not self.start(workers):
                    self.inline = True  # as under a login node's task limit: the calling thread runs every task
                try:
                    done = reader()
                except BaseException as exc:
                    if isinstance(_interrupt(exc), (KeyboardInterrupt, SystemExit)):
                        raise
                    caught = exc
                self.wait(self.idle)
            except BaseException as exc:
                self.stop = True
                caught = exc
                self.interrupted = self.interrupted or isinstance(_interrupt(exc), (KeyboardInterrupt, SystemExit))
            self.quiesce()
        finally:
            if previous is not None:
                signal.signal(signal.SIGINT, previous)  # type: ignore[arg-type]  # one SIGINT pending now is still ours
        self._deliver(caught, done, previous, reader)

    def _deliver(self, caught: BaseException | None, done: bool, previous: object, reader: Callable[[], bool]) -> None:
        """After the drain and the handler's restoration: raise what stopped the feed, or go on inline."""

        resumed = False
        try:
            first = None if caught is None else _interrupt(caught)
            if isinstance(first, (KeyboardInterrupt, SystemExit)):
                raise first
            failed = self.failure()
            other = failed if failed is not None else first
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
                raise other  # as it was raised: a task failure keeps its own cause
            if done and not self.tasks:
                return
            if not self.signals:
                raise RuntimeError("the feed stopped with neither a failure nor an interrupt")
            # A previous handler let the interrupt pass: the rest runs here, as the plain loop would. The stop flag
            # stays set, so that a worker thread starting only now takes nothing.
            resumed = True
            self.resumed = True
            self.inline = True
            try:
                for index in sorted(self.tasks):
                    self._run_here(index)
            except BaseException:
                self._discard_untaken()
                raise
            if not done and not reader():
                raise RuntimeError("the feed stopped with neither a failure nor an interrupt")
        finally:
            if not resumed:
                self._discard_untaken()

    def _discard_untaken(self) -> None:
        """Release every task that never ran, once; called only when no worker can take one any more."""

        error: BaseException | None = None
        while self.tasks:
            index = min(self.tasks)
            task, _ = self.tasks.pop(index)
            try:
                self.discard(task)
            except BaseException as exc:  # release the others first
                error = error if error is not None else exc
        if error is not None:
            raise error
