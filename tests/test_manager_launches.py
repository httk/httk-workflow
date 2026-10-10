"""The manager admits, starts and supervises the confined launches of its attempts.

The attempt sandbox is a pass-through (as in ``test_manager_confinement.py``) and the rank sandbox is
``tests/fake_bwrap.py``, so a launch runs the real launch client, the real rank helper and the real inner
exec on this host without namespaces. The launch template is a recording script around the rank helper,
so the "parallel start" is one local rank. ``fake_bwrap`` provides no ``--die-with-parent``: a rank that
outlives its SIGKILLed helper is killed by the test itself.
"""

import json
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace, _confine, _confine_rank, _kernel, _manager_launches
from httk.workflow._allocation import Allocation, Node
from httk.workflow._launch_protocol import trusted_name
from httk.workflow._logging import reset_logging
from httk.workflow._sandbox import PreparedSandbox
from httk.workflow._state import read_state_unowned
from httk.workflow.errors import ConfinementUnavailableError
from test_manager_confinement import find, submit_runner

_FAKE_BWRAP = Path(__file__).with_name("fake_bwrap.py")
_TIMEOUT = 60.0

_RUNNER = """#!/usr/bin/env python3
import glob
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

from httk.workflow import _launch_protocol as protocol

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
results = Path(os.environ["HTTK_TEST_RESULTS"])
results.mkdir(parents=True, exist_ok=True)
launch = shlex.split(os.environ.get("HTTK_WORKFLOW_LAUNCH", ""))
launch_dir = control / "launch"
result = {"launch": os.environ.get("HTTK_WORKFLOW_LAUNCH")}
published = False


def run(argv, cwd=None):
    started = time.monotonic()
    process = subprocess.run(argv, capture_output=True, cwd=cwd or workdir)
    return {
        "code": process.returncode,
        "stdout": process.stdout.decode(),
        "stderr": process.stderr.decode(),
        "seconds": time.monotonic() - started,
    }


def signalled(signum, rank, pid_path):
    # Run the client for rank, then signal the client once the rank has written its pid.
    started = time.monotonic()
    client = subprocess.Popen(
        [*launch, "sh", "-c", rank, "sh", str(pid_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=workdir,
    )
    wait_for(pid_path)
    client.send_signal(signum)
    stdout, stderr = client.communicate()
    return {
        "code": client.returncode,
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
        "seconds": time.monotonic() - started,
    }


def wait_for(path, seconds=30.0):
    deadline = time.monotonic() + seconds
    while not Path(path).exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    return Path(path).exists()


def statuses():
    found = {}
    for path in sorted(glob.glob(str(launch_dir / "*.status.json"))):
        status = protocol.decode_status(Path(path).read_bytes())
        found[status.request_id] = {"state": status.state, "exit_code": status.exit_code, "error": status.error}
    return found


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def request(request_id, *, stdout=False):
    launch_dir.mkdir(exist_ok=True)
    if stdout:
        (launch_dir / protocol.stdout_name(request_id)).write_text("forged")
    document = protocol.LaunchRequest(request_id, context["attempt_id"], ("true",), ".", ())
    (launch_dir / protocol.request_name(request_id)).write_bytes(protocol.encode_request(document))
    wait_for(launch_dir / protocol.status_name(request_id))
    return statuses().get(request_id)


def publish():
    global published
    if published:
        return
    published = True
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    (temporary / "outcome.json").write_text(json.dumps({
        "format": "httk-workflow-outcome",
        "format_version": 2,
        "job_id": context["job_id"],
        "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"],
        "action": "succeed",
    }))
    os.rename(temporary, control / "outcome.ready")


__BODY__
result["statuses"] = statuses()
(results / (context["job_id"] + ".json")).write_text(json.dumps(result))
publish()
"""

#: The launch template: it records the nodefile it was given, then execs the rank helper.
_RECORDER = """#!/bin/sh
nodefile="$1"
shift
printf 'nodefile=%s hosts=%s args=%s\\n' "$nodefile" "$(tr '\\n' ',' < "$nodefile")" "$*" >> {record}
exec "$@"
"""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    reset_logging()
    yield
    reset_logging()


class _Bench:
    """A workspace, the pass-through attempt sandbox and the fake rank sandbox."""

    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.root = root
        self.workspace = Workspace.initialize(root / "workspace", policy={"visibility_deadline_seconds": 0.05})
        self.results = root / "results"
        self.results.mkdir()
        self.pids = root / "pids"
        self.pids.mkdir()
        bin_directory = root / "bin"
        bin_directory.mkdir()
        self.bwrap = bin_directory / "bwrap"
        self.bwrap.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{_FAKE_BWRAP}" "$@"\n', encoding="utf-8")
        self.bwrap.chmod(0o755)
        self.record = root / "launches.log"
        self.recorder = bin_directory / "record-launch"
        self.recorder.write_text(_RECORDER.format(record=shlex.quote(str(self.record))), encoding="utf-8")
        self.recorder.chmod(0o755)
        self.shm = _host_shm_directory()
        self.settings = {
            "manager.confine": "bwrap",
            "confine.bwrap": str(self.bwrap),
            "confine.readonly_paths": "/usr",
            "confine.shm_root": str(self.shm),
            "manager.launch_template": f"{self.recorder} {{nodefile}}",
        }
        self.allocation = Allocation("host", None, (Node(socket.gethostname(), 2, 1000),), {})
        monkeypatch.setenv("HTTK_TEST_RESULTS", str(self.results))
        monkeypatch.setattr(_confine, "probe_bwrap", lambda settings: True)
        monkeypatch.setattr(_confine, "prepare_attempt_sandbox", lambda settings, **arguments: PreparedSandbox([], ()))
        monkeypatch.setattr(_manager_launches, "SCAN_INTERVAL", 0.05)

    def manager(self, *, confined: bool = True, **options: Any) -> TaskManager:
        options.setdefault("heartbeat_interval", 0.01)
        return TaskManager(
            self.workspace,
            resources=self.allocation.capacity(),
            allocation=self.allocation,
            setting_overrides=self.settings
            if confined
            else {"manager.launch_template": self.settings["manager.launch_template"]},
            **options,
        )

    def submit(self, tag: str, body: str, *, resources: Mapping[str, int] | None = None) -> str:
        runner = _RUNNER.replace("__BODY__", body)
        return submit_runner(
            self.workspace, self.root / "source", tag, runner, resources={"procs": 1, **(resources or {})}
        )[1]

    def result(self, job_id: str) -> dict[str, Any]:
        return json.loads((self.results / f"{job_id}.json").read_text(encoding="utf-8"))

    def outcome(self, job_id: str) -> tuple[str, str | None]:
        ref = find(self.workspace, job_id)
        if ref.state != "failed":
            return ref.state, None
        doc, _ = read_state_unowned(ref.path / "state.json")
        assert doc is not None and doc.failure is not None
        return ref.state, str(doc.failure["code"])

    def pid_file(self, name: str) -> Path:
        return self.pids / name

    def kill_recorded(self) -> None:
        for path in self.pids.iterdir():
            try:
                os.kill(int(path.read_text().strip()), signal.SIGKILL)
            except (ValueError, OSError):
                pass


def _host_shm_directory() -> Path:
    """Return a fresh private directory on the host's tmpfs, which rank-helper subprocesses accept as shm_root."""

    try:
        descriptor = os.open("/dev/shm", os.O_RDONLY | os.O_DIRECTORY)
        try:
            observed = _confine_rank._filesystem_type(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        observed = "unreadable"
    if observed != "tmpfs":
        pytest.skip("/dev/shm is not a tmpfs on this host")
    return Path(tempfile.mkdtemp(prefix="httk-test-", dir="/dev/shm"))


@pytest.fixture
def bench(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Bench]:
    created = _Bench(tmp_path.resolve(), monkeypatch)
    try:
        yield created
    finally:
        created.kill_recorded()
        shutil.rmtree(created.shm, ignore_errors=True)


def _events(caplog: pytest.LogCaptureFixture, name: str) -> list[Any]:
    return [record for record in caplog.records if getattr(record, "event", None) == name]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _until(manager: TaskManager, condition: Any, *, seconds: float = _TIMEOUT) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        manager.tick()
        if condition():
            return
        time.sleep(0.02)
    raise AssertionError("condition not reached")


# -- a launch end to end -------------------------------------------------------------------------------


_BASIC = """
os.environ["FOO"] = "bar"
(control / "nodefile").write_text("evil-host\\n")
(control / "binding.json").write_text('{"nodes": [{"host": "evil-host"}], "launch": ["evil"]}')
(workdir / "sub").mkdir(exist_ok=True)
result["first"] = run([*launch, "sh", "-c", 'pwd; echo "out $FOO"; echo err >&2; exit 3'], cwd=workdir / "sub")
"""


@pytest.mark.timing
def test_a_confined_launch_runs_through_the_manager_and_reports_its_exit(
    bench: _Bench, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = bench.submit("basic", _BASIC)
    with caplog.at_level("INFO", logger="httk.workflow.manager"), bench.manager() as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
        trusted_root = manager.owner.path / "launches"
        owned = bench.workspace.jobs / "owned" / manager.manager_id
    # The launch directories went with the launch and the attempt, and the owner with them.
    assert not trusted_root.exists()
    assert bench.outcome(job_id) == ("succeeded", None)
    result = bench.result(job_id)
    assert result["launch"] == _manager_launches.client_prefix()
    first = result["first"]
    assert first["code"] == 3, first
    # The client's working directory relative to the job directory and its environment reach the rank, which
    # runs in the job's directory below this owner's jobs/owned/ while the attempt runs.
    workdir, out = first["stdout"].splitlines()
    assert Path(workdir).parent.parent.parent == owned and workdir.endswith("/run/sub") and out == "out bar"
    assert first["stderr"] == "err\n"
    (status,) = result["statuses"].values()
    assert status == {"state": "exited", "exit_code": 3, "error": None}
    # H2: the launch used the manager's nodefile, rendered from the in-memory placement.
    (line,) = bench.record.read_text(encoding="utf-8").splitlines()
    fields = dict(item.split("=", 1) for item in line.split(" ", 2))
    (request_id,) = result["statuses"]
    (started,) = _events(caplog, "launch_started")
    trusted = trusted_root / trusted_name(started.attempt_id, request_id)
    assert fields["nodefile"] == str(trusted / "nodefile")
    assert fields["hosts"] == f"{socket.gethostname()},"
    assert fields["args"].endswith(f"-m httk.workflow._confine_rank --launch-dir {trusted}")
    (finished,) = _events(caplog, "launch_finished")
    for record in (started, finished):
        assert record.request_id == request_id and record.job_key == find(bench.workspace, job_id).job_key
        assert not hasattr(record, "argv")
    assert finished.exit_code == 3


def _record_launches(monkeypatch: pytest.MonkeyPatch, on_record: Any) -> list[subprocess.Popen[Any]]:
    """Record the launch gates started, and run *on_record* in place of a launch's process.json write."""

    started: list[subprocess.Popen[Any]] = []
    real_popen = subprocess.Popen
    real_write = _manager_launches.write_process_record

    def recording_popen(*arguments: Any, **options: Any) -> subprocess.Popen[Any]:
        process = real_popen(*arguments, **options)
        started.append(process)
        return process

    def write(manager: Any, directory: Path, attempt_id: str, n: str, pid: int, *, ranks_local_only: bool) -> None:
        if n != "0":
            on_record(manager)
        real_write(manager, directory, attempt_id, n, pid, ranks_local_only=ranks_local_only)

    monkeypatch.setattr(_manager_launches.subprocess, "Popen", recording_popen)
    monkeypatch.setattr(_manager_launches, "write_process_record", write)
    return started


def test_a_launch_whose_process_record_cannot_be_written_never_starts_its_prefix(
    bench: _Bench, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(_manager: Any) -> None:
        raise OSError("disk full")

    started = _record_launches(monkeypatch, refuse)
    bench.submit("gated", _BASIC)
    with bench.manager() as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    # The gate read EOF instead of the newline: the launch prefix (which records itself) never ran.
    assert not bench.record.exists()
    gates = [process for process in started if process.args[1:2] == ["-c"]]  # type: ignore[index]
    assert gates and all(process.wait(timeout=_TIMEOUT) in (125, -signal.SIGTERM) for process in gates)


def test_a_launch_of_an_owner_declared_dead_at_the_gate_never_starts_its_prefix(
    bench: _Bench, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A manager frozen since admission wakes after an operator declared its owner dead: the gate stays closed,
    # and the next tick fail-stops.
    def declare_dead(manager: Any) -> None:
        _kernel.attest_dead(bench.workspace, manager.manager_id, by="operator", evidence=[], reason="test")

    started = _record_launches(monkeypatch, declare_dead)
    bench.submit("dead", _BASIC)
    manager = bench.manager()
    with pytest.raises(_kernel.OwnerLost):
        manager.run_until_idle(timeout=_TIMEOUT)
    manager.close()
    assert not bench.record.exists()
    gates = [process for process in started if process.args[1:2] == ["-c"]]  # type: ignore[index]
    assert gates and all(process.wait(timeout=_TIMEOUT) in (125, -signal.SIGTERM) for process in gates)


def test_the_launch_gate_runs_nothing_without_its_newline(tmp_path: Path) -> None:
    marker = tmp_path / "ran"
    command = [*_manager_launches._LAUNCH_GATE, "touch", str(marker)]
    closed = subprocess.Popen(command, stdin=subprocess.PIPE)
    assert closed.stdin is not None
    closed.stdin.close()
    assert closed.wait(timeout=_TIMEOUT) == 125
    assert not marker.exists()
    opened = subprocess.Popen(command, stdin=subprocess.PIPE)
    assert opened.stdin is not None
    opened.stdin.write(b"\n")
    opened.stdin.close()
    assert opened.wait(timeout=_TIMEOUT) == 0
    assert marker.exists()


def test_an_unconfined_attempt_keeps_the_rendered_launch_prefix(bench: _Bench) -> None:
    job_id = bench.submit("plain", "")
    with bench.manager(confined=False) as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    launch = shlex.split(bench.result(job_id)["launch"])
    assert launch[0] == str(bench.recorder)
    # The nodefile is the owner's, in the runner launch's record directory.
    assert Path(launch[1]).name == "nodefile" and Path(launch[1]).parent.parent.name == "launches"


# -- refusals ------------------------------------------------------------------------------------------


_FORGED = """
import secrets
result["forged_stdout"] = request(secrets.token_hex(16), stdout=True)
"""


def test_a_request_with_forged_output_is_refused(bench: _Bench) -> None:
    job_id = bench.submit("forged", _FORGED)
    with bench.manager() as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    forged = bench.result(job_id)["forged_stdout"]
    assert forged["state"] == "refused" and "output files already exist" in forged["error"]
    assert not bench.record.exists()


_AFTER_PUBLISH = """
# The manager terminates a fenced attempt; this one finishes its request first.
signal.signal(signal.SIGTERM, signal.SIG_IGN)
publish()
result["late"] = run([*launch, "true"])
"""


def test_a_request_after_the_outcome_is_published_is_refused(bench: _Bench) -> None:
    job_id = bench.submit("published", _AFTER_PUBLISH)
    with bench.manager() as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    late = bench.result(job_id)["late"]
    assert late["code"] == 2 and "refused" in late["stderr"]
    assert not bench.record.exists()


def test_the_manager_names_the_rank_helper_launch_file() -> None:
    from httk.workflow import _confine_rank

    assert _manager_launches.LAUNCH_FILE == _confine_rank.LAUNCH_FILE


_AFTER_TIMEOUT = """
# The manager marks the attempt timed out before it sends SIGTERM, so the request follows the timeout.
terminated = []
signal.signal(signal.SIGTERM, lambda *_: terminated.append(1))
deadline = time.monotonic() + 30
while not terminated and time.monotonic() < deadline:
    time.sleep(0.02)
result["late"] = run([*launch, "true"])
"""


@pytest.mark.timing
def test_a_request_after_the_attempt_timed_out_is_refused(bench: _Bench) -> None:
    job_id = bench.submit("timeout", _AFTER_TIMEOUT, resources={"maxtime": 1})
    with bench.manager(cancel_grace_seconds=20.0) as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None) or bench.outcome(job_id)[1] == "timeout"
    late = bench.result(job_id)["late"]
    assert late["code"] == 2 and "maxtime" in late["stderr"]


_DURING_DRAIN = """
received = []
signal.signal(signal.SIGTERM, lambda *_: received.append(1))
(results / (context["job_id"] + ".started")).touch()
deadline = time.monotonic() + 30
while not received and time.monotonic() < deadline:
    time.sleep(0.02)
result["late"] = run([*launch, "true"])
"""


@pytest.mark.timing
def test_a_request_during_a_drain_is_refused(bench: _Bench) -> None:
    job_id = bench.submit("drain", _DURING_DRAIN)

    def drain() -> None:
        deadline = time.monotonic() + 30.0
        while not (bench.results / f"{job_id}.started").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = threading.Thread(target=drain, daemon=True)
    with bench.manager() as manager:
        stopper.start()
        manager.run_until_idle(timeout=_TIMEOUT, drain_timeout=30.0, drain_grace_seconds=20.0)
    stopper.join(timeout=5.0)
    late = bench.result(job_id)["late"]
    assert late["code"] == 2 and "draining" in late["stderr"]


def _attempt(**overrides: Any) -> SimpleNamespace:
    process = SimpleNamespace(poll=lambda: None)
    values: dict[str, Any] = {"cancel": None, "timed_out": False, "interrupted": False, "process": process}
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    ("attempt", "draining", "closed", "published", "expected"),
    [
        (_attempt(), False, None, False, None),
        (_attempt(cancel=object()), False, None, False, "being cancelled"),
        (_attempt(timed_out=True), False, None, False, "maxtime"),
        (_attempt(interrupted=True), False, None, False, "draining"),
        (_attempt(), True, None, False, "draining"),
        (_attempt(process=SimpleNamespace(poll=lambda: 0)), False, None, False, "process exited"),
        (_attempt(), False, "an earlier launch was uncertain", False, "uncertain"),
        (_attempt(), False, None, True, "published its outcome"),
    ],
)
def test_admission_is_refused_for_every_attempt_that_is_not_live(
    attempt: SimpleNamespace, draining: bool, closed: str | None, published: bool, expected: str | None
) -> None:
    manager = SimpleNamespace(_draining=draining)
    state = _manager_launches.AttemptLaunches("attempts/x", None, closed=closed)
    control = SimpleNamespace(exists_dir=lambda name: published and name == "outcome.ready")
    refusal = _manager_launches._admission_refusal(manager, attempt, state, control)  # type: ignore[arg-type]
    if expected is None:
        assert refusal is None
    else:
        assert refusal is not None and expected in refusal


# -- stopping launches -------------------------------------------------------------------------------


_CLIENT_SIGNALS = """
term_pid = results.parent / "pids" / "term"
kill_pid = results.parent / "pids" / "kill"
rank = 'echo $$ > "$1"; exec sleep 60'
result["term"] = signalled(signal.SIGTERM, rank, term_pid)
result["term_rank_alive"] = alive(int(term_pid.read_text()))
result["kill"] = signalled(signal.SIGKILL, rank, kill_pid)
time.sleep(1.0)
# Nothing tells the manager that a SIGKILLed client is gone: its launch runs until the attempt ends.
result["kill_rank_alive"] = alive(int(kill_pid.read_text()))
"""


@pytest.mark.timing
def test_a_signalled_client_stops_its_launch_and_a_killed_one_leaves_it_to_the_attempt_end(
    bench: _Bench, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = bench.submit("signals", _CLIENT_SIGNALS)
    with caplog.at_level("INFO", logger="httk.workflow.manager"), bench.manager(cancel_grace_seconds=5.0) as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    result = bench.result(job_id)
    # SIGTERM: the client wrote its stop marker and returned only once the ranks were gone (H2).
    assert result["term"]["code"] == 128 + signal.SIGTERM, result["term"]
    assert "stopped: the client asked to stop the launch" in result["term"]["stderr"]
    assert result["term_rank_alive"] is False
    # SIGKILL: the launch outlived its client, and the attempt's end stopped it.
    assert result["kill"]["code"] == -signal.SIGKILL, result["kill"]
    assert result["kill_rank_alive"] is True
    assert not _alive(int(bench.pid_file("kill").read_text()))
    reasons = sorted(record.getMessage() for record in _events(caplog, "launch_stopping"))
    assert any("the client asked to stop the launch" in reason for reason in reasons)
    ending = ("the attempt published its outcome", "the attempt process exited")
    assert any(any(text in reason for text in ending) for reason in reasons)
    assert len(_events(caplog, "launch_stopped")) == 2


_QUEUED = """
# The first launch runs until the runner releases it, which it does only once the second request is
# published; the second must then wait for the first launch's final status.
first = subprocess.Popen(
    [*launch, "sh", "-c", "echo first-start >> order; while [ ! -e release ]; do sleep 0.02; done; echo first-end >> order"],
    cwd=workdir,
)
wait_for(workdir / "order")
second = subprocess.Popen(
    [*launch, "sh", "-c", "echo second >> order"], cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE
)
deadline = time.monotonic() + 30
while len(glob.glob(str(launch_dir / "*.request.json"))) < 2 and time.monotonic() < deadline:
    time.sleep(0.02)
(workdir / "release").touch()
first.wait()
second.communicate()
result["second"] = {"code": second.returncode}
result["order"] = (workdir / "order").read_text().split()
"""


@pytest.mark.timing
def test_a_second_request_waits_for_the_first_launch_to_finish(bench: _Bench) -> None:
    job_id = bench.submit("queued", _QUEUED)
    with bench.manager() as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    result = bench.result(job_id)
    assert result["second"]["code"] == 0
    assert result["order"] == ["first-start", "first-end", "second"]


_IGNORING_TERM = """
rank = 'trap "" TERM; echo $$ > "$1"; while :; do sleep 0.1; done'
result["stop"] = signalled(signal.SIGTERM, rank, results.parent / "pids" / "ignoring")
"""


@pytest.mark.timing
def test_a_launch_ignoring_sigterm_is_killed_after_the_cancel_grace(
    bench: _Bench, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = bench.submit("ignoring", _IGNORING_TERM)
    with caplog.at_level("INFO", logger="httk.workflow.manager"), bench.manager(cancel_grace_seconds=1.0) as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    stop = bench.result(job_id)["stop"]
    assert stop["code"] == 128 + signal.SIGKILL and stop["seconds"] >= 1.0, stop
    (status,) = bench.result(job_id)["statuses"].values()
    assert status["state"] == "stopped" and status["exit_code"] == 128 + signal.SIGKILL
    assert _events(caplog, "launch_kill")


_UNCERTAIN = """
result["stop"] = signalled(signal.SIGTERM, 'echo $$ > "$1"; exec sleep 60', results.parent / "pids" / "stuck")
result["after"] = run([*launch, "true"])
"""


@pytest.mark.timing
def test_a_launch_that_cannot_be_reaped_is_uncertain_and_closes_admission(
    bench: _Bench, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stuck = {"on": True}
    real = _manager_launches.process_group_alive
    monkeypatch.setattr(_manager_launches, "process_group_alive", lambda group: stuck["on"] or real(group))
    job_id = bench.submit("uncertain", _UNCERTAIN)
    with caplog.at_level("INFO", logger="httk.workflow.manager"), bench.manager(cancel_grace_seconds=0.5) as manager:
        _until(manager, lambda: (bench.results / f"{job_id}.json").exists())
        result = bench.result(job_id)
        # The attempt stays tracked while its launch is not confirmed gone.
        _until(manager, lambda: all(local.process.poll() is not None for local in manager._running.values()))
        assert manager.running_attempts == 1
        assert bench.outcome(job_id)[0] == "owned"
        stuck["on"] = False
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    assert result["stop"]["code"] == 2 and "uncertain" in result["stop"]["stderr"]
    assert result["after"]["code"] == 2 and "could not be confirmed stopped" in result["after"]["stderr"]
    assert _events(caplog, "launch_uncertain")
    states = sorted(status["state"] for status in result["statuses"].values())
    assert states == ["refused", "uncertain"]


# -- ownership -----------------------------------------------------------------------------------------


_OUTLIVING = """
rank_pid = results.parent / "pids" / "outliving"
rank = 'trap "" TERM; echo $$ > "$1"; sleep 4'
client = subprocess.Popen([*launch, "sh", "-c", rank, "sh", str(rank_pid)], start_new_session=True, cwd=workdir)
(results.parent / "pids" / "client").write_text(str(client.pid))
wait_for(rank_pid)
"""


@pytest.mark.timing
def test_the_attempt_keeps_its_placement_and_its_commit_waits_until_its_launch_is_reaped(bench: _Bench) -> None:
    job_id = bench.submit("outliving", _OUTLIVING)
    with bench.manager(cancel_grace_seconds=2.0) as manager:
        _until(manager, lambda: (bench.results / f"{job_id}.json").exists())
        _until(
            manager,
            lambda: (
                bool(manager._running) and all(local.process.poll() is not None for local in manager._running.values())
            ),
        )
        (local,) = manager._running.values()
        assert _manager_launches.unreaped(local)
        # The process record names the allocation, so that another manager can ask whether it ended, and says
        # the ranks run in its process group on this host (a local launch template).
        launches = manager.owner.path / "launches"
        (record,) = [path for path in launches.iterdir() if not path.name.endswith(".0")]
        process = json.loads((record / _manager_launches.PROCESS_FILE).read_text())
        assert process["hostname"] == socket.gethostname() and process["attempt_id"] == local.attempt_id
        assert process["allocation"] == {"probe": "host", "kind": "host", "identity": None, "end_time": None}
        assert process["ranks_local_only"] is True and process["pgid"] == process["pid"]
        held = 0
        deadline = time.monotonic() + _TIMEOUT
        while time.monotonic() < deadline:
            manager.tick()
            kind = bench.outcome(job_id)[0]
            if kind == "succeeded":
                break
            if _manager_launches.unreaped(local):
                # The launch still runs after the attempt process exited: the placement is held and
                # the job stays owned and uncommitted.
                assert kind == "owned"
                assert manager.running_attempts == 1
                assert manager._available_resources()["procs"] == 1
                held += 1
            time.sleep(0.02)
        assert held > 0
        assert not _manager_launches.unreaped(local)
        manager.run_until_idle(timeout=_TIMEOUT)
        assert manager._available_resources()["procs"] == 2
    assert bench.outcome(job_id) == ("succeeded", None)
    assert not (find(bench.workspace, job_id).path / "attempts").exists()
    client = int(bench.pid_file("client").read_text())
    deadline = time.monotonic() + 5
    while _alive(client):  # the commit removed launch/; the client must not poll it forever
        assert time.monotonic() < deadline, "the launch client outlived its launch directory"
        time.sleep(0.05)


_SAME_REQUEST = """
(results / (context["job_id"] + ".ready")).touch()
deadline = time.monotonic() + 30
while len(glob.glob(str(results / "*.ready"))) < 2 and time.monotonic() < deadline:
    time.sleep(0.02)
result["same"] = request(os.environ["HTTK_TEST_REQUEST_ID"])
"""


def test_two_attempts_may_use_the_same_request_id(bench: _Bench, monkeypatch: pytest.MonkeyPatch) -> None:
    # Request ids are chosen by jobs and visible to other jobs; one job cannot block another's launch.
    monkeypatch.setenv("HTTK_TEST_REQUEST_ID", "0123456789abcdef0123456789abcdef")
    identifiers = [bench.submit(tag, _SAME_REQUEST) for tag in ("one", "two")]
    with bench.manager(maximum_workers=2) as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    for job_id in identifiers:
        assert bench.outcome(job_id) == ("succeeded", None)
        assert bench.result(job_id)["same"] == {"state": "exited", "exit_code": 0, "error": None}
    directories = {line.rsplit("--launch-dir ", 1)[1] for line in bench.record.read_text().splitlines()}
    assert len(directories) == 2


_SEQUENTIAL = """
for index in range(3):
    result[f"run{index}"] = run([*launch, "true"])
(results / (context["job_id"] + ".launched")).touch()
wait_for(results / (context["job_id"] + ".release"), 60)
"""


@pytest.mark.timing
def test_a_reaped_launch_leaves_no_trusted_record(bench: _Bench) -> None:
    job_id = bench.submit("sequential", _SEQUENTIAL)
    with bench.manager() as manager:
        _until(manager, lambda: (bench.results / f"{job_id}.launched").exists())
        for _ in range(3):
            manager.tick()
        # Three launches ran; the attempt is still running, and only its runner's record remains.
        assert manager.running_attempts == 1
        launches = manager.owner.path / "launches"
        assert [path.name.rsplit(".", 1)[1] for path in launches.iterdir()] == ["0"]
        (bench.results / f"{job_id}.release").touch()
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    result = bench.result(job_id)
    assert [result[f"run{index}"]["code"] for index in range(3)] == [0, 0, 0]
    assert len(result["statuses"]) == 3


_SHARED_MEMORY = """
os.environ["SHM_ROOT"] = os.environ["HTTK_TEST_SHM_ROOT"]
os.environ["WORKSPACE"] = os.environ["HTTK_WORKFLOW_WORKSPACE_DIR"]
listing = 'ls "$SHM_ROOT"; cat "$WORKSPACE"/.httk-workspace/owners/*/launches/*/launch.json'
result["shm"] = run([*launch, "sh", "-c", listing])
"""


def test_the_shared_memory_directory_is_named_by_the_launch_token(
    bench: _Bench, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTK_TEST_SHM_ROOT", str(bench.shm))
    job_id = bench.submit("shm", _SHARED_MEMORY)
    with bench.manager() as manager:
        manager.run_until_idle(timeout=_TIMEOUT)
    assert bench.outcome(job_id) == ("succeeded", None)
    listing, description = bench.result(job_id)["shm"]["stdout"].split("\n", 1)
    trusted = json.loads(description)
    assert listing == f"httk-{trusted['token']}"
    assert trusted["token"] != trusted["request_id"]
    # The manager removed it on its node when it reaped the launch.
    assert not list(bench.shm.iterdir())


# -- one real Bubblewrap launch ------------------------------------------------------------------------


_REAL = """
# Outside the job directory the rank sees the workspace read-only.
os.environ["PLANT_TARGET"] = os.path.join(os.environ["HTTK_WORKFLOW_WORKSPACE_DIR"], "planted.txt")
script = 'echo ok > out.txt; echo hi; if echo x > "$PLANT_TARGET"; then echo planted; else echo refused; fi'
result["real"] = run([*launch, "sh", "-c", script])
result["out"] = (workdir / "out.txt").read_text()
"""


@pytest.mark.timing
def test_a_real_bubblewrap_launch_runs_its_ranks_confined(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_manager_confinement import _unsupported_namespace_failure

    bwrap = shutil.which("bwrap")
    if bwrap is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    pinned = {"manager.confine": "bwrap", "manager.launch_template": "env HTTK_TEST_NODEFILE={nodefile}"}
    try:
        _confine.probe_bwrap(_confine.confine_settings(pinned))
    except ConfinementUnavailableError as exc:
        _unsupported_namespace_failure(str(exc))
        raise
    workspace = Workspace.initialize(tmp_path / "workspace")
    allocation = Allocation("host", None, (Node(socket.gethostname(), 1, 1000),), {})
    job_id = submit_runner(
        workspace, tmp_path / "source", "real", _RUNNER.replace("__BODY__", _REAL), resources={"procs": 1}
    )[1]
    # The runner writes its results inside its own working directory, the only writable path it has.
    monkeypatch.setenv("HTTK_TEST_RESULTS", "results")
    with TaskManager(
        workspace,
        heartbeat_interval=0.01,
        resources=allocation.capacity(),
        allocation=allocation,
        setting_overrides=pinned,
    ) as manager:
        manager.run_until_idle(timeout=120.0)
    finished = find(workspace, job_id)
    if finished.state != "succeeded":
        stdio = finished.path / "logs" / "stdio.out"
        _unsupported_namespace_failure(stdio.read_text(errors="replace") if stdio.exists() else finished.state)
    result = json.loads((finished.path / "run" / "results" / f"{job_id}.json").read_text(encoding="utf-8"))
    assert result["launch"] == _manager_launches.client_prefix()
    assert result["real"]["code"] == 0, result["real"]
    assert result["real"]["stdout"] == "hi\nrefused\n"
    assert result["out"] == "ok\n"
    assert not (workspace.root / "planted.txt").exists()
