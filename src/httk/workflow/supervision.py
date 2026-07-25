"""Structured, argv-only process supervision for native runners."""

import json
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from ._util import utc_now, write_json_atomic

type DiagnosticSeverity = Literal["info", "warning", "error", "fatal"]


@dataclass(frozen=True)
class SourceEvent:
    """One event delivered to an in-process or executable checker."""

    event: Literal["start", "line", "tick", "source-eof", "process-exit"]
    source: str
    line: str | None = None
    timestamp: str = ""

    def as_mapping(self) -> dict[str, object]:
        result: dict[str, object] = {
            "format": "httk-workflow-checker-event",
            "format_version": 1,
            "event": self.event,
            "source": self.source,
            "timestamp": self.timestamp or utc_now(),
        }
        if self.line is not None:
            result["line"] = self.line
        return result


@dataclass(frozen=True)
class Diagnostic:
    """A structured observation made while supervising a program."""

    code: str
    severity: DiagnosticSeverity
    summary: str
    source: str
    evidence: str | None = None
    stop: bool = False

    def as_mapping(self) -> dict[str, object]:
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
    """A file to follow while the child process is running."""

    path: Path
    name: str | None = None
    inactivity_timeout: float | None = None


@dataclass(frozen=True)
class CheckerSpec:
    """A versioned executable checker."""

    argv: tuple[str, ...]
    required: bool = True
    sources: tuple[FollowSource, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CheckerSpec":
        if value.get("format") != "httk-workflow-checker-spec" or value.get("format_version") != 1:
            raise ValueError("checker spec must use httk-workflow-checker-spec version 1")
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
    """The complete result of one supervised command."""

    argv: tuple[str, ...]
    started_at: str
    finished_at: str
    returncode: int
    termination: str
    stdout: bytes
    stderr: bytes
    diagnostics: tuple[Diagnostic, ...]

    @property
    def timed_out(self) -> bool:
        return self.termination.startswith("timeout")

    def as_mapping(self) -> dict[str, object]:
        return {
            "format": "httk-workflow-process-report",
            "format_version": 1,
            "argv": list(self.argv),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "returncode": self.returncode,
            "termination": self.termination,
            "diagnostics": [item.as_mapping() for item in self.diagnostics],
        }

    def write(self, path: str | os.PathLike[str]) -> Path:
        destination = Path(path)
        write_json_atomic(destination, self.as_mapping())
        return destination


type EventMonitor = Callable[[SourceEvent], Sequence[Diagnostic]]


class _Checker:
    def __init__(
        self,
        spec: CheckerSpec,
        diagnostics: list[Diagnostic],
        lock: threading.Lock,
        stop: threading.Event,
    ) -> None:
        self.spec = spec
        self.diagnostics = diagnostics
        self.lock = lock
        self.stop = stop
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
        diagnostic = Diagnostic(
            "checker_protocol_error",
            "fatal" if self.spec.required else "warning",
            summary,
            f"checker:{self.spec.argv[0]}",
            stop=self.spec.required,
        )
        with self.lock:
            self.diagnostics.append(diagnostic)
        if self.spec.required:
            self.stop.set()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            try:
                value = json.loads(line)
                if not isinstance(value, dict) or value.get("format") != "httk-workflow-checker-result":
                    raise ValueError("wrong checker result format")
                if value.get("format_version") != 1:
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
            with self.lock:
                self.diagnostics.append(diagnostic)
            if diagnostic.stop:
                self.stop.set()

    def send(self, event: SourceEvent) -> None:
        if self.process.poll() is not None:
            return
        assert self.process.stdin is not None
        try:
            self.process.stdin.write(json.dumps(event.as_mapping(), separators=(",", ":")) + "\n")
            self.process.stdin.flush()
        except (BrokenPipeError, OSError):
            self._protocol_error("checker closed its input unexpectedly")

    def close(self) -> None:
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            returncode = self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            returncode = self.process.wait(timeout=2)
        self.thread.join(timeout=2)
        if returncode and not self.stop.is_set():
            self._protocol_error(f"checker exited with status {returncode}")


class ProcessSupervisor:
    """Run one command, stream its output, and terminate its process group safely."""

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
    ) -> ProcessReport:
        command = tuple(argv)
        if not command or not all(isinstance(item, str) and item for item in command):
            raise ValueError("command must be a nonempty sequence of nonempty strings")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if termination_grace < 0:
            raise ValueError("termination_grace cannot be negative")
        started_at = utc_now()
        started = time.monotonic()
        diagnostics: list[Diagnostic] = []
        lock = threading.Lock()
        stop_requested = threading.Event()
        process_done = threading.Event()
        checkers = [_Checker(spec, diagnostics, lock, stop_requested) for spec in self.checker_specs]

        def dispatch(event: SourceEvent) -> None:
            for monitor in self.monitors:
                try:
                    found = monitor(event)
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
                with lock:
                    diagnostics.extend(found)
                if any(item.stop for item in found):
                    stop_requested.set()
            for checker in checkers:
                checker.send(event)

        dispatch(SourceEvent("start", "process", timestamp=started_at))
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=None if environment is None else dict(environment),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        received_signal: list[int] = []
        previous_handlers: dict[int, Any] = {}
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, signal.SIGINT):
                previous_handlers[signum] = signal.getsignal(signum)

                def forward(received: int, frame: object, *, _signum: int = signum) -> None:
                    del frame, _signum
                    received_signal.append(received)
                    stop_requested.set()

                signal.signal(signum, forward)
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []
        output_handles = [
            None if stdout_path is None else Path(stdout_path).open("ab"),
            None if stderr_path is None else Path(stderr_path).open("ab"),
        ]

        def read_stream(source: str, stream, chunks: list[bytes], output) -> None:
            while line := stream.readline():
                chunks.append(line)
                if output is not None:
                    output.write(line)
                    output.flush()
                dispatch(SourceEvent("line", source, line.decode("utf-8", errors="replace").rstrip("\r\n")))
            dispatch(SourceEvent("source-eof", source))

        stream_threads = [
            threading.Thread(
                target=read_stream,
                args=("stdout", process.stdout, stdout_chunks, output_handles[0]),
                daemon=True,
            ),
            threading.Thread(
                target=read_stream,
                args=("stderr", process.stderr, stderr_chunks, output_handles[1]),
                daemon=True,
            ),
        ]
        for thread in stream_threads:
            thread.start()

        def follow_file(spec: FollowSource) -> None:
            position = 0
            last_activity = time.monotonic()
            name = spec.name or f"file:{spec.path.name}"
            while not process_done.is_set():
                if spec.path.is_file():
                    with spec.path.open("rb") as handle:
                        handle.seek(position)
                        while line := handle.readline():
                            position = handle.tell()
                            last_activity = time.monotonic()
                            dispatch(
                                SourceEvent(
                                    "line",
                                    name,
                                    line.decode("utf-8", errors="replace").rstrip("\r\n"),
                                )
                            )
                if spec.inactivity_timeout is not None and time.monotonic() - last_activity >= spec.inactivity_timeout:
                    with lock:
                        diagnostics.append(
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
                time.sleep(0.1)
            if spec.path.is_file():
                with spec.path.open("rb") as handle:
                    handle.seek(position)
                    for line in handle:
                        dispatch(SourceEvent("line", name, line.decode("utf-8", errors="replace").rstrip("\r\n")))
            dispatch(SourceEvent("source-eof", name))

        follow_threads = [threading.Thread(target=follow_file, args=(spec,), daemon=True) for spec in self.follow]
        for thread in follow_threads:
            thread.start()

        termination = "exit"
        term_sent: float | None = None
        while process.poll() is None:
            now = time.monotonic()
            dispatch(SourceEvent("tick", "process"))
            if timeout is not None and now - started >= timeout and term_sent is None:
                termination = "timeout"
                stop_requested.set()
            if received_signal and term_sent is None:
                termination = f"signal_{received_signal[0]}"
            if stop_requested.is_set() and term_sent is None:
                if termination == "exit":
                    termination = "checker_stop"
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                term_sent = now
            if term_sent is not None and now - term_sent >= termination_grace and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                termination = f"{termination}_killed"
            time.sleep(0.05)
        process_done.set()
        for thread in stream_threads + follow_threads:
            thread.join(timeout=2)
        dispatch(SourceEvent("process-exit", "process", str(process.returncode)))
        for checker in checkers:
            checker.close()
        for handle in output_handles:
            if handle is not None:
                handle.close()
        for restore_signum, handler in previous_handlers.items():
            signal.signal(restore_signum, handler)
        return ProcessReport(
            command,
            started_at,
            utc_now(),
            process.returncode,
            termination,
            b"".join(stdout_chunks),
            b"".join(stderr_chunks),
            tuple(diagnostics),
        )
