"""Structured, argv-only process supervision for native runners.

Every event is dispatched to the in-process monitors and to the executable
checkers under one supervisor-wide lock, so a monitor is never entered
concurrently with itself or with another monitor, and a checker never receives
an interleaved line on its stdin. A monitor therefore does not have to be
thread-safe, but it must return quickly: it delays the whole event stream.
"""

import json
import logging
import os
import signal
import stat
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Literal

from ._util import utc_now, write_json_atomic

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_CAPTURE_LIMIT",
    "DEFAULT_DIAGNOSTIC_LIMIT",
    "DEFAULT_FOLLOW_INTERVAL",
    "DEFAULT_TAIL_LIMIT",
    "DEFAULT_TICK_INTERVAL",
    "CheckerSpec",
    "Diagnostic",
    "FollowSource",
    "ProcessReport",
    "ProcessSupervisor",
    "SourceEvent",
]

type DiagnosticSeverity = Literal["info", "warning", "error", "fatal"]

DEFAULT_TICK_INTERVAL = 1.0
"""Seconds between the ``tick`` events delivered to monitors and checkers."""

DEFAULT_FOLLOW_INTERVAL = 0.25
"""Seconds between polls of a followed file that produced no new data."""

DEFAULT_CAPTURE_LIMIT = 4 * 1024 * 1024
"""Bytes of a stream retained in the report when no output path is given."""

DEFAULT_TAIL_LIMIT = 64 * 1024
"""Bytes of a stream retained in the report when an output path is given."""

DEFAULT_DIAGNOSTIC_LIMIT = 512
"""Diagnostics retained in one report before the middle is dropped."""

_PROCESS_POLL_INTERVAL = 0.05
_JOIN_TIMEOUT = 5.0
_CHECKER_TIMEOUT = 2.0
_KILL_WAIT = 5.0
_MAXIMUM_REAP_WAIT = 10.0


@dataclass(frozen=True)
class SourceEvent:
    """Describe one event delivered to a checker.

    :param event: Identify the event kind.
    :param source: Identify the stream or process that produced it.
    :param line: Preserve the event line when one exists.
    :param timestamp: Preserve the event timestamp, or generate one when absent.
    """

    event: Literal["start", "line", "tick", "source-eof", "process-exit"]
    source: str
    line: str | None = None
    timestamp: str = ""

    def as_mapping(self) -> dict[str, object]:
        """Return the checker protocol mapping for this event.

        :return: The serialized event mapping.
        """
        result: dict[str, object] = {
            "format": "httk-workflow-checker-event",
            "format_version": 2,
            "event": self.event,
            "source": self.source,
            "timestamp": self.timestamp or utc_now(),
        }
        if self.line is not None:
            result["line"] = self.line
        return result


@dataclass(frozen=True)
class Diagnostic:
    """Describe a structured observation made while supervising a program.

    :param code: Name the stable diagnostic code.
    :param severity: Classify the diagnostic severity.
    :param summary: Summarize the observation.
    :param source: Identify the source that produced it.
    :param evidence: Preserve supporting evidence when present.
    :param stop: Request termination of the supervised command.
    """

    code: str
    severity: DiagnosticSeverity
    summary: str
    source: str
    evidence: str | None = None
    stop: bool = False

    def as_mapping(self) -> dict[str, object]:
        """Return the serialized diagnostic mapping.

        :return: The diagnostic mapping.
        """
        result: dict[str, object] = {
            "code": self.code,
            "severity": self.severity,
            "summary": self.summary,
            "source": self.source,
            "stop": self.stop,
        }
        if self.evidence is not None:
            result["evidence"] = self.evidence
        return result


@dataclass(frozen=True)
class FollowSource:
    """Describe a file to follow while the child process is running.

    :param path: Locate the file to follow.
    :param name: Identify the source in emitted events.
    :param inactivity_timeout: Stop when no data arrives for this interval.
    """

    path: Path
    name: str | None = None
    inactivity_timeout: float | None = None


@dataclass(frozen=True)
class CheckerSpec:
    """Describe a versioned executable checker.

    :param argv: Supply the checker command and arguments.
    :param required: Require the checker to remain available for the run.
    :param sources: Select files the checker follows.
    """

    argv: tuple[str, ...]
    required: bool = True
    sources: tuple[FollowSource, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CheckerSpec":
        """Validate and build a checker specification from a mapping.

        :param value: Supply the serialized checker specification.
        :return: The validated checker specification.
        :raises ValueError: If the mapping has an invalid checker format or source.
        """
        if value.get("format") != "httk-workflow-checker-spec" or value.get("format_version") != 2:
            raise ValueError("checker spec must use httk-workflow-checker-spec version 2")
        raw = value.get("argv")
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise ValueError("checker argv must be an array")
        argv = tuple(raw)
        if not argv or not all(isinstance(item, str) and item for item in argv):
            raise ValueError("checker argv must contain nonempty strings")
        sources_raw = value.get("sources", [])
        if not isinstance(sources_raw, Sequence) or isinstance(sources_raw, (str, bytes)):
            raise ValueError("checker sources must be an array")
        sources: list[FollowSource] = []
        for raw_source in sources_raw:
            if not isinstance(raw_source, Mapping) or not isinstance(raw_source.get("path"), str):
                raise ValueError("checker source must contain a path")
            timeout_raw = raw_source.get("inactivity_timeout")
            timeout = None if timeout_raw is None else float(timeout_raw)
            if timeout is not None and timeout <= 0:
                raise ValueError("checker source inactivity_timeout must be positive")
            name_raw = raw_source.get("name")
            if name_raw is not None and not isinstance(name_raw, str):
                raise ValueError("checker source name must be a string")
            sources.append(FollowSource(Path(raw_source["path"]), name_raw, timeout))
        return cls(argv, bool(value.get("required", True)), tuple(sources))


@dataclass(frozen=True)
class ProcessReport:
    """The complete result of one supervised command.

    ``stdout`` and ``stderr`` are bounded: when an output path was given they
    hold at most the run's tail limit and the file named by ``stdout_path`` or
    ``stderr_path`` is authoritative; otherwise they hold at most the run's
    capture limit. ``stdout_bytes`` and ``stderr_bytes`` always count every byte
    the program produced, and ``stdout_truncated`` and ``stderr_truncated`` say
    whether anything was dropped from the retained tail.

    :param argv: Preserve the executed command.
    :param started_at: Record when execution began.
    :param finished_at: Record when execution ended.
    :param returncode: Record the process return code.
    :param termination: Record why execution ended.
    :param stdout: Preserve the captured stdout tail.
    :param stderr: Preserve the captured stderr tail.
    :param diagnostics: Preserve emitted diagnostics.
    :param stdout_path: Locate the authoritative stdout file when configured.
    :param stderr_path: Locate the authoritative stderr file when configured.
    :param stdout_bytes: Count all stdout bytes produced.
    :param stderr_bytes: Count all stderr bytes produced.
    :param stdout_truncated: Indicate that stdout was truncated.
    :param stderr_truncated: Indicate that stderr was truncated.
    :param dropped_diagnostics: Count diagnostics dropped from the middle.
    """

    argv: tuple[str, ...]
    started_at: str
    finished_at: str
    returncode: int
    termination: str
    stdout: bytes
    stderr: bytes
    diagnostics: tuple[Diagnostic, ...]
    stdout_path: str | None = None
    stderr_path: str | None = None
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    dropped_diagnostics: int = 0

    @property
    def timed_out(self) -> bool:
        """Report whether the process ended because of a timeout.

        :return: Whether the termination reason starts with ``timeout``.
        """
        return self.termination.startswith("timeout")

    def as_mapping(self) -> dict[str, object]:
        """Return the serialized process report.

        :return: The report mapping.
        """
        return {
            "format": "httk-workflow-process-report",
            "format_version": 2,
            "argv": list(self.argv),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "returncode": self.returncode,
            "termination": self.termination,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "stdout_bytes": self.stdout_bytes,
            "stderr_bytes": self.stderr_bytes,
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
            "dropped_diagnostics": self.dropped_diagnostics,
            "diagnostics": [item.as_mapping() for item in self.diagnostics],
        }

    def write(self, path: str | os.PathLike[str]) -> Path:
        """Write the serialized report atomically.

        :param path: Locate the report file to write.
        :return: The report path.
        """
        destination = Path(path)
        write_json_atomic(destination, self.as_mapping())
        return destination


type EventMonitor = Callable[[SourceEvent], Sequence[Diagnostic]]


def _signal_group(pid: int, signum: int) -> None:
    """Signal a whole process group, tolerating a group that already left."""

    try:
        os.killpg(pid, signum)
    except (ProcessLookupError, PermissionError):
        _LOGGER.debug("process group %d could not be signalled with %d", pid, signum)


class _Capture:
    """A thread-safe, bounded tail of one output stream."""

    def __init__(self, limit: int) -> None:
        self._limit = max(0, limit)
        self._chunks: deque[bytes] = deque()
        self._retained = 0
        self._lock = threading.Lock()
        self._total = 0
        self._truncated = False

    def add(self, chunk: bytes) -> None:
        if not chunk:
            return
        with self._lock:
            self._total += len(chunk)
            if self._limit == 0:
                self._truncated = True
                return
            self._chunks.append(chunk)
            self._retained += len(chunk)
            while self._retained > self._limit:
                self._truncated = True
                excess = self._retained - self._limit
                oldest = self._chunks[0]
                if len(oldest) <= excess:
                    self._chunks.popleft()
                    self._retained -= len(oldest)
                else:
                    self._chunks[0] = oldest[excess:]
                    self._retained -= excess

    @property
    def total(self) -> int:
        with self._lock:
            return self._total

    @property
    def truncated(self) -> bool:
        with self._lock:
            return self._truncated

    def value(self) -> bytes:
        with self._lock:
            return b"".join(self._chunks)


class _DiagnosticLog:
    """A thread-safe record of diagnostics that keeps a bounded head and tail."""

    def __init__(self, limit: int) -> None:
        self._head_limit = max(1, limit // 2)
        self._tail_limit = max(1, limit - self._head_limit)
        self._head: list[Diagnostic] = []
        self._tail: deque[Diagnostic] = deque(maxlen=self._tail_limit)
        self._lock = threading.Lock()
        self._dropped = 0

    def _append(self, item: Diagnostic) -> None:
        if len(self._head) < self._head_limit:
            self._head.append(item)
            return
        if len(self._tail) == self._tail_limit:
            self._dropped += 1
        self._tail.append(item)

    def add(self, item: Diagnostic) -> None:
        with self._lock:
            self._append(item)

    def extend(self, items: Sequence[Diagnostic]) -> None:
        if not items:
            return
        with self._lock:
            for item in items:
                self._append(item)

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped

    def values(self) -> tuple[Diagnostic, ...]:
        with self._lock:
            if self._dropped == 0:
                return (*self._head, *self._tail)
            marker = Diagnostic(
                "diagnostics_truncated",
                "warning",
                f"{self._dropped} intermediate diagnostics were dropped",
                "supervisor",
            )
            return (*self._head, marker, *self._tail)


class _Checker:
    def __init__(self, spec: CheckerSpec, diagnostics: _DiagnosticLog, stop: threading.Event) -> None:
        self.spec = spec
        self.diagnostics = diagnostics
        self.stop = stop
        self._send_lock = threading.Lock()
        self._closed = False
        self.process = subprocess.Popen(
            spec.argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
            encoding="utf-8",
        )
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _protocol_error(self, summary: str) -> None:
        self.diagnostics.add(
            Diagnostic(
                "checker_protocol_error",
                "fatal" if self.spec.required else "warning",
                summary,
                f"checker:{self.spec.argv[0]}",
                stop=self.spec.required,
            )
        )
        if self.spec.required:
            self.stop.set()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            try:
                value = json.loads(line)
                if not isinstance(value, dict) or value.get("format") != "httk-workflow-checker-result":
                    raise ValueError("wrong checker result format")
                if value.get("format_version") != 2:
                    raise ValueError("unsupported checker result version")
                severity = str(value.get("severity", "error"))
                if severity not in {"info", "warning", "error", "fatal"}:
                    raise ValueError("invalid diagnostic severity")
                diagnostic = Diagnostic(
                    str(value.get("code", "checker")),
                    severity,  # type: ignore[arg-type]
                    str(value.get("summary", "")),
                    str(value.get("source", f"checker:{self.spec.argv[0]}")),
                    None if value.get("evidence") is None else str(value["evidence"]),
                    bool(value.get("stop", False)),
                )
            except (ValueError, json.JSONDecodeError) as exc:
                self._protocol_error(f"checker emitted invalid JSON: {exc}")
                return
            self.diagnostics.add(diagnostic)
            if diagnostic.stop:
                self.stop.set()

    def send(self, event: SourceEvent) -> None:
        """Write one event line, serialized against every other sender."""

        payload = json.dumps(event.as_mapping(), separators=(",", ":")) + "\n"
        broken = False
        with self._send_lock:
            if self._closed or self.process.poll() is not None or self.process.stdin is None:
                return
            try:
                self.process.stdin.write(payload)
                self.process.stdin.flush()
            except (BrokenPipeError, OSError, ValueError):
                broken = True
        if broken:
            self._protocol_error("checker closed its input unexpectedly")

    def close(self) -> None:
        """Close the checker's input and reap it, escalating to SIGKILL."""

        with self._send_lock:
            if self._closed:
                return
            self._closed = True
            if self.process.stdin is not None and not self.process.stdin.closed:
                try:
                    self.process.stdin.close()
                except OSError:
                    _LOGGER.debug("checker %s input could not be closed", self.spec.argv[0], exc_info=True)
        returncode = self._reap()
        self.thread.join(timeout=_JOIN_TIMEOUT)
        if returncode is None:
            self._protocol_error("checker did not exit after SIGKILL")
        elif returncode and not self.stop.is_set():
            self._protocol_error(f"checker exited with status {returncode}")

    def _reap(self) -> int | None:
        for escalate in (None, self.process.terminate, self.process.kill):
            if escalate is not None:
                escalate()
            try:
                return self.process.wait(timeout=_CHECKER_TIMEOUT)
            except subprocess.TimeoutExpired:
                continue
        _LOGGER.error("checker %s survived SIGKILL", self.spec.argv[0])
        return None


class ProcessSupervisor:
    """Run one command, stream its output, and terminate its process group safely.

    Monitors and checkers observe the same event stream under one lock, so an
    in-process monitor does not have to be thread-safe. Whatever happens during
    a run - including an exception raised by the caller's monitor - the signal
    handlers are restored, the child's process group is terminated and reaped,
    the reader threads are joined, and the output handles are closed before
    :meth:`run` returns or propagates.

    :param monitors: Supply in-process event monitors.
    :param checkers: Supply executable checker specifications.
    :param follow: Supply files to follow during the run.
    """

    def __init__(
        self,
        *,
        monitors: Sequence[EventMonitor] = (),
        checkers: Sequence[CheckerSpec] = (),
        follow: Sequence[FollowSource] = (),
    ) -> None:
        self.monitors = tuple(monitors)
        self.checker_specs = tuple(checkers)
        self.follow = tuple(follow)

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: float | None = None,
        cwd: str | os.PathLike[str] | None = None,
        environment: Mapping[str, str] | None = None,
        termination_grace: float = 10.0,
        stdout_path: str | os.PathLike[str] | None = None,
        stderr_path: str | os.PathLike[str] | None = None,
        tick_interval: float = DEFAULT_TICK_INTERVAL,
        follow_interval: float = DEFAULT_FOLLOW_INTERVAL,
        capture_limit: int = DEFAULT_CAPTURE_LIMIT,
        tail_limit: int = DEFAULT_TAIL_LIMIT,
        diagnostic_limit: int = DEFAULT_DIAGNOSTIC_LIMIT,
    ) -> ProcessReport:
        """Run one command to completion and return its structured report.

        :param argv: Supply the command and arguments.
        :param timeout: Stop the command after this interval.
        :param cwd: Select the command working directory.
        :param environment: Supply the command environment.
        :param termination_grace: Wait this long after requesting termination.
        :param stdout_path: Write stdout to this authoritative file.
        :param stderr_path: Write stderr to this authoritative file.
        :param tick_interval: Set the interval between tick events.
        :param follow_interval: Set the interval between followed-file polls.
        :param capture_limit: Bound retained output without an output file.
        :param tail_limit: Bound retained output with an output file.
        :param diagnostic_limit: Bound retained diagnostics.
        :return: The structured process report.
        :raises ValueError: If command or supervision limits are invalid.
        """

        command = tuple(argv)
        if not command or not all(isinstance(item, str) and item for item in command):
            raise ValueError("command must be a nonempty sequence of nonempty strings")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if termination_grace < 0:
            raise ValueError("termination_grace cannot be negative")
        if tick_interval <= 0:
            raise ValueError("tick_interval must be positive")
        if follow_interval <= 0:
            raise ValueError("follow_interval must be positive")
        if capture_limit < 0 or tail_limit < 0:
            raise ValueError("capture limits cannot be negative")
        if diagnostic_limit < 2:
            raise ValueError("diagnostic_limit must be at least 2")

        started_at = utc_now()
        started = time.monotonic()
        log = _DiagnosticLog(diagnostic_limit)
        dispatch_lock = threading.Lock()
        stop_requested = threading.Event()
        process_done = threading.Event()
        checkers: list[_Checker] = []
        threads: list[threading.Thread] = []
        output_handles: list[IO[bytes] | None] = [None, None]
        previous_handlers: dict[int, Any] = {}
        received_signal: list[int] = []
        process: subprocess.Popen[bytes] | None = None
        captures = (
            _Capture(tail_limit if stdout_path is not None else capture_limit),
            _Capture(tail_limit if stderr_path is not None else capture_limit),
        )
        termination = "exit"
        killed = False

        def dispatch(event: SourceEvent) -> None:
            with dispatch_lock:
                for monitor in self.monitors:
                    try:
                        found = tuple(monitor(event))
                    except Exception as exc:
                        found = (
                            Diagnostic(
                                "monitor_error",
                                "fatal",
                                f"in-process monitor failed: {exc}",
                                event.source,
                                stop=True,
                            ),
                        )
                    log.extend(found)
                    if any(item.stop for item in found):
                        stop_requested.set()
                for checker in checkers:
                    checker.send(event)

        def read_stream(source: str, stream: IO[bytes], capture: _Capture, output: IO[bytes] | None) -> None:
            try:
                while line := stream.readline():
                    capture.add(line)
                    if output is not None:
                        output.write(line)
                        output.flush()
                    dispatch(SourceEvent("line", source, line.decode("utf-8", errors="replace").rstrip("\r\n")))
            except (OSError, ValueError):
                _LOGGER.debug("reading %s stopped early", source, exc_info=True)
            finally:
                dispatch(SourceEvent("source-eof", source))

        def drain(handle: IO[bytes], name: str) -> bool:
            seen = False
            while line := handle.readline():
                seen = True
                dispatch(SourceEvent("line", name, line.decode("utf-8", errors="replace").rstrip("\r\n")))
            return seen

        def follow_file(spec: FollowSource) -> None:
            name = spec.name or f"file:{spec.path.name}"
            handle: IO[bytes] | None = None
            identity: tuple[int, int] | None = None
            last_activity = time.monotonic()
            try:
                while True:
                    finishing = process_done.is_set()
                    try:
                        info = spec.path.stat()
                    except OSError:
                        info = None
                    if info is not None and stat.S_ISREG(info.st_mode):
                        current = (info.st_dev, info.st_ino)
                        if handle is not None and (current != identity or info.st_size < handle.tell()):
                            # The file was rotated or truncated: take what the old
                            # handle still holds, then start over on the new one.
                            drain(handle, name)
                            handle.close()
                            handle = None
                        if handle is None:
                            try:
                                handle = spec.path.open("rb")
                            except OSError:
                                _LOGGER.debug("followed file %s could not be opened", spec.path, exc_info=True)
                            else:
                                identity = current
                        if handle is not None and drain(handle, name):
                            last_activity = time.monotonic()
                    if finishing:
                        break
                    if (
                        spec.inactivity_timeout is not None
                        and time.monotonic() - last_activity >= spec.inactivity_timeout
                    ):
                        log.add(
                            Diagnostic(
                                "source_inactive",
                                "fatal",
                                f"{name} produced no data for {spec.inactivity_timeout:g} seconds",
                                name,
                                stop=True,
                            )
                        )
                        stop_requested.set()
                        break
                    time.sleep(follow_interval)
            finally:
                if handle is not None:
                    handle.close()
                dispatch(SourceEvent("source-eof", name))

        def forward(received: int, frame: object) -> None:
            del frame
            received_signal.append(received)
            stop_requested.set()

        try:
            for spec in self.checker_specs:
                checkers.append(_Checker(spec, log, stop_requested))
            if threading.current_thread() is threading.main_thread():
                # Installed before the child exists: a signal delivered in the
                # window between the fork and the handler would orphan the group.
                for signum in (signal.SIGTERM, signal.SIGINT):
                    previous_handlers[signum] = signal.getsignal(signum)
                    signal.signal(signum, forward)
            dispatch(SourceEvent("start", "process", timestamp=started_at))
            if stdout_path is not None:
                output_handles[0] = Path(stdout_path).open("ab")  # noqa: SIM115 - handle escapes to the reader thread
            if stderr_path is not None:
                output_handles[1] = Path(stderr_path).open("ab")  # noqa: SIM115 - handle escapes to the reader thread
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=None if environment is None else dict(environment),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            assert process.stdout is not None and process.stderr is not None
            threads.append(
                threading.Thread(
                    target=read_stream,
                    args=("stdout", process.stdout, captures[0], output_handles[0]),
                    daemon=True,
                )
            )
            threads.append(
                threading.Thread(
                    target=read_stream,
                    args=("stderr", process.stderr, captures[1], output_handles[1]),
                    daemon=True,
                )
            )
            threads.extend(threading.Thread(target=follow_file, args=(spec,), daemon=True) for spec in self.follow)
            for thread in threads:
                thread.start()

            term_sent: float | None = None
            next_tick = started + tick_interval
            while process.poll() is None:
                now = time.monotonic()
                if now >= next_tick:
                    next_tick = now + tick_interval
                    dispatch(SourceEvent("tick", "process"))
                if timeout is not None and now - started >= timeout and term_sent is None:
                    termination = "timeout"
                    stop_requested.set()
                if received_signal and term_sent is None:
                    termination = f"signal_{received_signal[0]}"
                if stop_requested.is_set() and term_sent is None:
                    if termination == "exit":
                        termination = "checker_stop"
                    _signal_group(process.pid, signal.SIGTERM)
                    term_sent = now
                if term_sent is not None and not killed and now - term_sent >= termination_grace:
                    # Escalate exactly once: a process group that survives SIGKILL
                    # (an uninterruptible wait) must not be signalled every poll.
                    killed = True
                    termination = f"{termination}_killed"
                    _signal_group(process.pid, signal.SIGKILL)
                time.sleep(_PROCESS_POLL_INTERVAL)
            process_done.set()
            for thread in threads:
                thread.join(timeout=_JOIN_TIMEOUT)
            dispatch(SourceEvent("process-exit", "process", str(process.returncode)))
        finally:
            process_done.set()
            if process is not None:
                _reap_group(process, termination_grace, escalated=killed)
            for thread in threads:
                thread.join(timeout=_JOIN_TIMEOUT)
            for checker in checkers:
                try:
                    checker.close()
                except Exception:
                    _LOGGER.warning("checker %s failed to close", checker.spec.argv[0], exc_info=True)
            for handle in output_handles:
                if handle is not None:
                    try:
                        handle.close()
                    except OSError:
                        _LOGGER.debug("an output handle could not be closed", exc_info=True)
            for restore_signum, handler in previous_handlers.items():
                signal.signal(restore_signum, handler)

        assert process is not None
        return ProcessReport(
            command,
            started_at,
            utc_now(),
            process.returncode,
            termination,
            captures[0].value(),
            captures[1].value(),
            log.values(),
            None if stdout_path is None else os.fspath(stdout_path),
            None if stderr_path is None else os.fspath(stderr_path),
            captures[0].total,
            captures[1].total,
            captures[0].truncated,
            captures[1].truncated,
            log.dropped,
        )


def _reap_group(process: "subprocess.Popen[bytes]", termination_grace: float, *, escalated: bool = False) -> None:
    """Make sure no member of a supervised process group outlives the run.

    ``escalated`` says the run already escalated to SIGKILL, in which case the
    polite signal is not repeated on the way out.
    """

    if process.poll() is not None:
        return
    if not escalated:
        _signal_group(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=min(max(termination_grace, 0.5), _MAXIMUM_REAP_WAIT))
        except subprocess.TimeoutExpired:
            _LOGGER.warning("process group %d ignored SIGTERM", process.pid)
    if process.poll() is None:
        _signal_group(process.pid, signal.SIGKILL)
        try:
            process.wait(timeout=_KILL_WAIT)
        except subprocess.TimeoutExpired:
            _LOGGER.error("process group %d survived SIGKILL", process.pid)
