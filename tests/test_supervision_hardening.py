"""Lifecycle guarantees of :mod:`httk.workflow.supervision`.

The protocol-level behaviour of a supervised command is covered in
:mod:`tests.test_native_bash_api`; this module covers what has to hold when a
run goes wrong: no orphaned process group, no torn checker input, no unbounded
memory, exactly one escalation to SIGKILL, a calm event cadence, and a followed
file that survives rotation.
"""

import json
import os
import signal
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path

import pytest

from conftest import TestProfile as _TestProfile
from httk.workflow.supervision import (
    CheckerSpec,
    Diagnostic,
    FollowSource,
    ProcessSupervisor,
    SourceEvent,
)

pytestmark = pytest.mark.xdist_group("process-timing")

_SLEEPER = """import os, sys, time
sys.stdout.write(str(os.getpid()) + "\\n")
sys.stdout.flush()
time.sleep(60)
"""

_STUBBORN = """import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
sys.stdout.write(str(os.getpid()) + "\\n")
sys.stdout.flush()
time.sleep(60)
"""

_CHECKER = """import json, sys
ticks = 0
lines = 0
def emit(code, summary, evidence=None):
    print(json.dumps({"format": "httk-workflow-checker-result", "format_version": 1,
                      "code": code, "severity": "info", "summary": summary,
                      "source": "checker", "evidence": evidence}), flush=True)
for line in sys.stdin:
    try:
        event = json.loads(line)
        assert event["format"] == "httk-workflow-checker-event"
    except Exception as exc:
        emit("torn_line", str(exc), line[:120])
        continue
    if event["event"] == "tick":
        ticks += 1
        emit("tick", str(ticks))
    elif event["event"] == "line":
        lines += 1
emit("line_total", str(lines))
"""


class _Collector:
    """An in-process monitor that records every event it is given."""

    def __init__(self, *, raise_on: str | None = None) -> None:
        self.events: list[SourceEvent] = []
        self.raise_on = raise_on

    def __call__(self, event: SourceEvent) -> Sequence[Diagnostic]:
        self.events.append(event)
        if self.raise_on is not None and event.event == self.raise_on:
            raise KeyboardInterrupt("monitor abandoned the run")
        return ()

    def lines(self, source: str) -> list[str]:
        return [item.line or "" for item in self.events if item.event == "line" and item.source == source]


class _Reentrancy:
    """A monitor that records whether it was ever entered concurrently."""

    def __init__(self) -> None:
        self.calls = 0
        self.overlaps = 0
        self._active = 0
        self._guard = threading.Lock()

    def __call__(self, event: SourceEvent) -> Sequence[Diagnostic]:
        with self._guard:
            self._active += 1
            self.calls += 1
            overlapping = self._active > 1
        time.sleep(0.0005)
        with self._guard:
            if overlapping or self._active > 1:
                self.overlaps += 1
            self._active -= 1
        return ()


def _script(tmp_path: Path, name: str, body: str) -> str:
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return str(path)


def _alive(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def test_exception_mid_run_reaps_the_group_and_restores_handlers(tmp_path: Path) -> None:
    program = _script(tmp_path, "sleeper.py", _SLEEPER)
    pid_file = tmp_path / "child.out"
    collector = _Collector(raise_on="tick")
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)

    with pytest.raises(KeyboardInterrupt):
        ProcessSupervisor(monitors=(collector,)).run(
            [sys.executable, program],
            stdout_path=pid_file,
            tick_interval=0.5,
            termination_grace=0.1,
        )

    assert signal.getsignal(signal.SIGTERM) is before_term
    assert signal.getsignal(signal.SIGINT) is before_int
    pid = int(pid_file.read_text(encoding="utf-8").strip())
    deadline = time.monotonic() + 5
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _alive(pid), "the supervised process group outlived the failed run"


def test_checker_input_is_serialized_under_concurrent_event_storms(tmp_path: Path, test_profile: _TestProfile) -> None:
    checker = _script(tmp_path, "checker.py", _CHECKER)
    source_count = test_profile.scale(normal=2, extended=4)
    output_lines = test_profile.scale(normal=80, extended=200)
    tick_interval = 0.02
    run_seconds = 0.6
    output_event_count = source_count * output_lines
    # Each profile emits one output line for every source/output-line pair.
    # The floor therefore keeps the same all-output-lines plus ten-ticks
    # contract at either scale, rather than loosening the normal assertion.
    expected_tick_events = int(run_seconds / tick_interval) // 3
    minimum_monitor_events = output_event_count + expected_tick_events
    sources = [tmp_path / f"source-{index}.log" for index in range(source_count)]
    for path in sources:
        path.write_text("", encoding="utf-8")
    stop = threading.Event()

    def hammer(path: Path) -> None:
        # The lines are deliberately far larger than one pipe buffer, so an
        # unserialized write to the checker's stdin is split into several
        # os.write calls that another source's write can interleave with.
        index = 0
        deadline = time.monotonic() + 0.4
        while not stop.is_set() and time.monotonic() < deadline:
            with path.open("a", encoding="utf-8") as handle:
                for _ in range(4):
                    index += 1
                    handle.write(f"{path.name} line {index} " + "x" * 20000 + "\n")
            time.sleep(0.02)

    writers = [threading.Thread(target=hammer, args=(path,), daemon=True) for path in sources]
    monitor = _Reentrancy()
    for writer in writers:
        writer.start()
    try:
        report = ProcessSupervisor(
            monitors=(monitor,),
            checkers=(CheckerSpec((sys.executable, checker)),),
            follow=tuple(FollowSource(path, path.stem) for path in sources),
        ).run(
            [
                sys.executable,
                "-c",
                f"import time\nfor i in range({output_event_count}):\n    print('out %d ' % i + 'o' * 20000)\ntime.sleep({run_seconds})\n",
            ],
            tick_interval=tick_interval,
            follow_interval=0.005,
        )
    finally:
        stop.set()
        for writer in writers:
            writer.join(timeout=5)

    torn = [item for item in report.diagnostics if item.code == "torn_line"]
    assert not torn, torn[:3]
    assert not [item for item in report.diagnostics if item.code == "checker_protocol_error"]
    totals = [int(item.summary) for item in report.diagnostics if item.code == "line_total"]
    assert totals and totals[0] >= output_event_count, f"the checker only saw {totals} line events"
    assert monitor.calls >= minimum_monitor_events
    assert monitor.overlaps == 0, f"{monitor.overlaps} of {monitor.calls} dispatches overlapped"


def test_large_output_stays_on_disk_and_only_a_bounded_tail_is_reported(tmp_path: Path) -> None:
    out = tmp_path / "program.out"
    program = "import sys\nline = 'y' * 999 + '\\n'\nfor _ in range(3000):\n    sys.stdout.write(line)\n"
    report = ProcessSupervisor().run(
        [sys.executable, "-c", program],
        stdout_path=out,
        tail_limit=64 * 1024,
    )
    assert report.returncode == 0
    assert out.stat().st_size == 3000 * 1000
    assert report.stdout_bytes == 3000 * 1000
    assert report.stdout_truncated
    assert len(report.stdout) <= 64 * 1024
    assert report.stdout.endswith(b"y" * 999 + b"\n")
    assert report.stdout_path == str(out)
    assert report.as_mapping()["stdout_bytes"] == 3000 * 1000


def test_in_memory_capture_is_capped_when_no_path_is_given() -> None:
    program = "import sys\nfor index in range(2000):\n    sys.stdout.write('%06d' % index + 'z' * 93 + '\\n')\n"
    report = ProcessSupervisor().run([sys.executable, "-c", program], capture_limit=8 * 1024)
    assert report.stdout_bytes == 2000 * 100
    assert report.stdout_truncated
    assert len(report.stdout) <= 8 * 1024
    assert report.stdout.endswith(b"z" * 93 + b"\n")


def test_diagnostics_are_bounded_with_a_dropped_marker(tmp_path: Path) -> None:
    def monitor(event: SourceEvent) -> Sequence[Diagnostic]:
        if event.event != "line":
            return ()
        return (Diagnostic("noisy", "info", event.line or "", event.source),)

    program = "import sys\nfor index in range(200):\n    sys.stdout.write('line %d\\n' % index)\n"
    report = ProcessSupervisor(monitors=(monitor,)).run([sys.executable, "-c", program], diagnostic_limit=20)
    kept = list(report.diagnostics)
    assert report.dropped_diagnostics >= 150
    assert len(kept) <= 21
    markers = [item for item in kept if item.code == "diagnostics_truncated"]
    assert len(markers) == 1
    assert kept[0].summary == "line 0"
    assert kept[-1].summary == "line 199"


def test_kill_escalation_happens_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    program = _script(tmp_path, "stubborn.py", _STUBBORN)
    real_killpg = os.killpg
    sent: list[tuple[int, int]] = []
    lock = threading.Lock()

    def recording_killpg(pgid: int, signum: int) -> None:
        with lock:
            sent.append((pgid, signum))
            first_kill = signum == signal.SIGKILL and sum(1 for item in sent if item[1] == signal.SIGKILL) == 1
        if signum == signal.SIGKILL:
            # Let the group linger the way an uninterruptible wait would, so a
            # supervisor that re-fires the escalation would be caught doing so.
            if first_kill:
                threading.Timer(0.4, real_killpg, (pgid, signum)).start()
            return
        real_killpg(pgid, signum)

    monkeypatch.setattr(os, "killpg", recording_killpg)
    report = ProcessSupervisor().run(
        [sys.executable, program],
        timeout=0.2,
        termination_grace=0.1,
    )
    assert report.termination == "timeout_killed"
    assert report.timed_out
    assert [item[1] for item in sent].count(signal.SIGKILL) == 1
    assert [item[1] for item in sent].count(signal.SIGTERM) == 1


def test_tick_cadence_is_gentle_by_default(tmp_path: Path) -> None:
    checker = _script(tmp_path, "checker.py", _CHECKER)
    report = ProcessSupervisor(checkers=(CheckerSpec((sys.executable, checker)),)).run(
        [sys.executable, "-c", "import time; time.sleep(2.5)"]
    )
    ticks = [item for item in report.diagnostics if item.code == "tick"]
    assert 1 <= len(ticks) <= 5, f"checker received {len(ticks)} ticks"


def test_tick_cadence_is_configurable() -> None:
    collector = _Collector()
    ProcessSupervisor(monitors=(collector,)).run(
        [sys.executable, "-c", "import time; time.sleep(1.0)"],
        tick_interval=0.1,
    )
    ticks = [item for item in collector.events if item.event == "tick"]
    assert len(ticks) >= 5


def test_follow_survives_a_file_rotation(tmp_path: Path) -> None:
    followed = tmp_path / "OSZICAR"
    followed.write_text("before one\nbefore two\n", encoding="utf-8")
    collector = _Collector()
    rotated = tmp_path / "OSZICAR.new"

    def rotate() -> None:
        time.sleep(0.4)
        rotated.write_text("after one\nafter two\n", encoding="utf-8")
        os.replace(rotated, followed)
        time.sleep(0.2)
        with followed.open("a", encoding="utf-8") as handle:
            handle.write("after three\n")

    worker = threading.Thread(target=rotate, daemon=True)
    worker.start()
    try:
        ProcessSupervisor(
            monitors=(collector,),
            follow=(FollowSource(followed, "OSZICAR"),),
        ).run([sys.executable, "-c", "import time; time.sleep(1.2)"], follow_interval=0.05)
    finally:
        worker.join(timeout=5)

    seen = collector.lines("OSZICAR")
    assert "before one" in seen
    assert "after one" in seen
    assert "after two" in seen
    assert "after three" in seen


def test_report_json_is_still_the_versioned_process_report(tmp_path: Path) -> None:
    report = ProcessSupervisor().run([sys.executable, "-c", "print('hello')"])
    value = json.loads(report.write(tmp_path / "report.json").read_text(encoding="utf-8"))
    assert value["format"] == "httk-workflow-process-report"
    assert value["format_version"] == 1
    assert value["returncode"] == 0
    assert value["termination"] == "exit"
    assert value["stdout_path"] is None
    assert value["dropped_diagnostics"] == 0
    assert report.stdout == b"hello\n"
