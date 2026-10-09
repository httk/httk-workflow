"""A job that tampers with its own control paths fails alone, never redirecting or stalling the manager.

Each scenario runs a real :class:`~httk.workflow.TaskManager` over a job whose
runner plants a symlink, FIFO, oversized or damaged document on one of the
manager's control paths in its own directory and then exits, so the manager
meets the tampering while ending that attempt and launching the next one. A
healthy job in the same workspace must still succeed, nothing outside the job
directory may change, and nothing may block.
"""

import json
import os
import signal
import stat
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import Any, Self

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _kernel, _manager_commit, _store
from httk.workflow._logging import reset_logging
from test_commit_ownership import _gone
from test_crash_injection import _log

pytestmark = pytest.mark.slow

_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, signal, subprocess, sys, time

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job = pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
parameters = json.loads((job / "job.json").read_text())["parameters"]
OUTSIDE = pathlib.Path(parameters["outside"])
LIMIT = parameters.get("limit", 0)


def outcome(action="succeed"):
    document = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
    document.update(format="httk-workflow-outcome", format_version=2, action=action)
    return document


def publish(document=None):
    temporary = control / "outcome.tmp.test"
    temporary.mkdir()
    (temporary / "outcome.json").write_text(json.dumps(document or outcome()))
    os.rename(temporary, control / "outcome.ready")


(OUTSIDE / ("runner." + context["attempt_id"])).write_text(__file__)
if context["attempt_ordinal"] == 1:
    exec(parameters.get("tamper", "pass"))
publish()
"""

#: What the first attempt of each scenario does to its own job directory, and the failure code the job must end
#: with. A scenario that exits 1 leaves the next attempt (``retry_on: process_failure``) to meet the tampering.
_SCENARIOS: dict[str, tuple[str, str]] = {
    # (a) the stdio chronicle is a symlink to a file outside the job directory.
    "stdio_symlink": (
        """
(job / "logs" / "stdio.out").unlink()
(job / "logs" / "stdio.out").symlink_to(OUTSIDE / "target")
sys.exit(1)
""",
        "protocol_error",
    ),
    # (b) the whole log directory is a symlink to a directory outside.
    "logs_symlink": (
        """
(job / "logs").rename(job / "logs.moved")
(job / "logs").symlink_to(OUTSIDE, target_is_directory=True)
sys.exit(1)
""",
        "protocol_error",
    ),
    # (c) the stdio chronicle is a FIFO nobody reads.
    "stdio_fifo": (
        """
(job / "logs" / "stdio.out").unlink()
os.mkfifo(job / "logs" / "stdio.out")
sys.exit(1)
""",
        "protocol_error",
    ),
    # (d) the owner run log is a symlink to a file outside.
    "runlog_symlink": (
        """
(job / "logs" / "runlog.jsonl").unlink()
(job / "logs" / "runlog.jsonl").symlink_to(OUTSIDE / "target")
sys.exit(1)
""",
        "protocol_error",
    ),
    # (e) the attempt container is a symlink to a directory outside.
    "attempts_symlink": (
        """
os.rename(job / "attempts", job / "attempts.moved")
(job / "attempts").symlink_to(OUTSIDE, target_is_directory=True)
sys.exit(1)
""",
        "protocol_error",
    ),
    # (f) outcome.ready is a symlink to an outside directory holding a valid outcome.
    "outcome_symlink": (
        """
(OUTSIDE / "published").mkdir()
(OUTSIDE / "published" / "outcome.json").write_text(json.dumps(outcome()))
(control / "outcome.ready").symlink_to(OUTSIDE / "published", target_is_directory=True)
sys.exit(0)
""",
        "protocol_error",
    ),
    # (g) .httk-job is a symlink to an outside directory holding a "seal". The job gets a single attempt and
    # fails, so the failure release itself is what must not follow it.
    "state_symlink": (
        """
(OUTSIDE / "state").mkdir()
(OUTSIDE / "state" / "seal.json").write_text("{}")
(job / ".httk-job").symlink_to(OUTSIDE / "state", target_is_directory=True)
sys.exit(1)
""",
        "process_failure",
    ),
    # (h) the workdir is a symlink to a directory outside.
    "workdir_symlink": (
        """
os.chdir(job)
(job / "run").rename(job / "run.moved")
(job / "run").symlink_to(OUTSIDE, target_is_directory=True)
sys.exit(1)
""",
        "protocol_error",
    ),
    # (i) an oversized outcome document.
    "outcome_oversized": (
        """
temporary = control / "outcome.tmp.test"
temporary.mkdir()
with (temporary / "outcome.json").open("wb") as handle:
    handle.truncate(LIMIT + 1)
os.rename(temporary, control / "outcome.ready")
sys.exit(0)
""",
        "protocol_error",
    ),
    # (j) the outcome document is a FIFO nobody writes.
    "outcome_fifo": (
        """
temporary = control / "outcome.tmp.test"
temporary.mkdir()
os.mkfifo(temporary / "outcome.json")
os.rename(temporary, control / "outcome.ready")
sys.exit(0)
""",
        "protocol_error",
    ),
    # (k) the job overwrites its owner's state.json with garbage, then publishes a valid outcome.
    "state_json_garbage": (
        """
(job / "state.json").write_text("{ not json")
""",
        "protocol_error",
    ),
    # (l) the job replaces its owner's state.json with a symlink to a file outside.
    "state_json_symlink": (
        """
(job / "state.json").unlink()
(job / "state.json").symlink_to(OUTSIDE / "target")
""",
        "protocol_error",
    ),
}
#: Scenarios whose job gets no second attempt.
_SINGLE_ATTEMPT = {"state_symlink"}


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    reset_logging()
    yield
    reset_logging()


@pytest.fixture(autouse=True)
def _watchdog() -> Iterator[None]:
    """Turn a blocked manager into a test failure instead of a hung test run."""

    def expire(number: int, frame: FrameType | None) -> None:
        raise TimeoutError("the manager blocked")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.alarm(90)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "jobdir", executables={"run": _RUNNER})


@pytest.fixture()
def outside(tmp_path: Path) -> Path:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "target").write_text("outside\n", encoding="utf-8")
    return outside


def _submit(
    ws: Workspace, installed: _store.Installed, outside: Path, tag: str, *, tamper: str = "pass", attempts: int = 1
) -> _kernel.JobRef:
    return h.submit(
        ws,
        installed,
        {"start": "tamper"},
        tag=tag,
        placement=f"project/{tag}",
        parameters={"outside": str(outside), "tamper": tamper, "limit": _manager_commit._OUTCOME_LIMIT},
        retry_policy={
            "maximum_attempts_per_activation": attempts,
            "maximum_total_attempts": attempts,
            "retry_on": ["process_failure"] if attempts > 1 else [],
        },
    )


def _snapshot(directory: Path) -> dict[str, Any]:
    """Record every entry below *directory*, following nothing."""

    state: dict[str, Any] = {}
    for current, directories, files in os.walk(directory):
        for name in (*directories, *files):
            path = Path(current, name)
            information = path.lstat()
            content = path.read_bytes() if path.is_file() and not path.is_symlink() else None
            state[str(path.relative_to(directory))] = (information.st_mode, information.st_size, content)
    return state


def _failure_code(ws: Workspace, job_id: str) -> tuple[str, str | None]:
    ref = h.find(ws, job_id)
    if ref.state != "failed":
        return ref.state, None
    doc = h.state_of(ref)
    assert doc.failure is not None
    return ref.state, str(doc.failure["code"])


#: Scenarios that reveal manager defects (reported; the tests stay strict so a fix turns them green).
_DEFECTS = {
    "logs_symlink": "OwnedJob.append_log follows a symlinked logs/ directory: the manager appends runlog.jsonl "
    "outside the job directory",
    "stdio_fifo": "_fs.append_file opens an existing file without O_NONBLOCK: a FIFO planted at logs/stdio.out "
    "blocks the manager (every job) until a reader appears",
    "runlog_symlink": "a symlink at logs/runlog.jsonl raises UnsafePath, which reconcile does not treat as job "
    "damage: the job stays owned forever and its manager never becomes idle",
    "state_json_symlink": "a symlink at state.json raises UnsafePath, which reconcile does not treat as job "
    "damage: the job stays owned forever and its manager never becomes idle",
}
#: How long a scenario may run before the test opens every FIFO in the workspace for reading, unblocking a
#: manager stuck on one (which the test then reports as a stall instead of hanging).
_STALL_SECONDS = 20.0


class _Rescuer:
    """After the stall deadline, keep giving every FIFO in the workspace a partner until stopped.

    Opening a FIFO read-write completes a blocked open on either side; closing it again then ends a blocked read
    (end of file) or a blocked write (a broken pipe). A manager stuck on a planted FIFO thus moves on, and the test
    reports the stall instead of hanging.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.fired = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=10.0)

    def _run(self) -> None:
        if self._stop.wait(_STALL_SECONDS):
            return
        while not self._stop.wait(0.2):
            for current, _directories, files in os.walk(self.root):
                for name in files:
                    path = Path(current, name)
                    try:
                        if not stat.S_ISFIFO(path.lstat().st_mode):
                            continue
                        descriptor = os.open(path, os.O_RDWR | os.O_NONBLOCK)
                    except OSError:
                        continue
                    self.fired = True
                    os.close(descriptor)


@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(name, marks=pytest.mark.xfail(strict=True, reason=_DEFECTS[name])) if name in _DEFECTS else name
        for name in sorted(_SCENARIOS)
    ],
)
def test_tampering_with_a_control_path_fails_only_that_job(
    ws: Workspace, installed: _store.Installed, outside: Path, scenario: str
) -> None:
    tamper, expected = _SCENARIOS[scenario]
    attempts = 1 if scenario in _SINGLE_ATTEMPT else 2
    hostile = _submit(ws, installed, outside, "hostile", tamper=tamper, attempts=attempts)
    healthy = _submit(ws, installed, outside, "healthy")

    started = time.monotonic()
    with _Rescuer(ws.root) as rescuer:
        with TaskManager(ws, heartbeat_interval=0.01, cancel_grace_seconds=1.0) as manager:
            manager.run_until_idle(timeout=_STALL_SECONDS)
            # The tampering only makes the manager record a failure, so it is still serving.
            manager.tick()
        elapsed = time.monotonic() - started
    assert not rescuer.fired, "the manager blocked on a FIFO the job planted"

    # Nothing the manager did was written through the planted links: the only entries outside are the target and
    # what the job itself put there.
    planted = _snapshot(outside)
    assert planted.pop("target")[2] == b"outside\n"
    planted = {name: value for name, value in planted.items() if not name.startswith("runner.")}
    assert set(planted) <= {"published", "published/outcome.json", "state", "state/seal.json"}
    assert _failure_code(ws, hostile.job_id) == ("failed", expected)
    assert _failure_code(ws, healthy.job_id) == ("succeeded", None)
    assert elapsed < _STALL_SECONDS


def test_a_process_left_in_the_attempt_group_ignoring_sigterm_is_killed_before_the_commit(
    ws: Workspace, installed: _store.Installed, outside: Path
) -> None:
    """The runner exits, but a process it left in its group ignores SIGTERM: the manager kills the group."""

    tamper = """
script = "import os, signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); " \\
         "open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(120)"
subprocess.Popen([sys.executable, "-c", script, str(OUTSIDE / "pid")])
while not (OUTSIDE / "pid").exists():
    time.sleep(0.01)
"""
    lingering = _submit(ws, installed, outside, "lingering", tamper=tamper)
    started = time.monotonic()
    with TaskManager(ws, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as manager:
        manager.run_until_idle(timeout=60.0)
        assert not manager.running_attempts
    elapsed = time.monotonic() - started

    assert _failure_code(ws, lingering.job_id) == ("succeeded", None)
    assert _gone(int((outside / "pid").read_text(encoding="utf-8")))
    assert elapsed < 30.0


def test_a_commit_error_is_retried_by_self_healing_and_spares_other_jobs(
    ws: Workspace, installed: _store.Installed, outside: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bad = _submit(ws, installed, outside, "bad")
    good = _submit(ws, installed, outside, "good")
    real = TaskManager._commit_outcome
    failed: list[str] = []

    def commit(self: TaskManager, owned: _kernel.OwnedJob, *arguments: Any) -> None:
        if owned.job_id == bad.job_id and not failed:
            failed.append(owned.job_key)
            # What an unexpected I/O error while committing reports.
            raise OSError("simulated storage error while committing")
        real(self, owned, *arguments)

    monkeypatch.setattr(TaskManager, "_commit_outcome", commit)
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
        reported = dict(manager._reported)

    assert failed == [bad.job_key]
    assert any(key == f"commit:{bad.job_key}" for key in reported)
    # The job stayed owned with its published outcome; the next tick's self-healing committed it.
    assert _failure_code(ws, bad.job_id) == ("succeeded", None)
    assert [line["event"] for line in _log(h.find(ws, bad.job_id))].count("launched") == 1
    assert _failure_code(ws, good.job_id) == ("succeeded", None)


def test_an_installed_runner_is_executed_by_its_installed_path(
    ws: Workspace, installed: _store.Installed, outside: Path
) -> None:
    submitted = _submit(ws, installed, outside, "runner")
    h.run(ws)
    done = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    # The runner sees its real path in the installed package, so it can locate the files beside it.
    (seen,) = outside.glob("runner.*")
    assert seen.read_text(encoding="utf-8") == str(installed.package / "run")
    (started,) = [line for line in _log(done) if line["event"] == "attempt_started"]
    detail = started["detail"]
    assert isinstance(detail, dict) and detail["command"] == [str(installed.package / "run")]


def test_a_fifo_job_definition_of_a_ready_job_never_stalls_the_manager(
    ws: Workspace, installed: _store.Installed, outside: Path
) -> None:
    hostile = _submit(ws, installed, outside, "hostile")
    healthy = _submit(ws, installed, outside, "healthy")
    # Something outside the protocol replaced the ready job's definition with a FIFO nobody writes.
    (hostile.path / "job.json").unlink()
    os.mkfifo(hostile.path / "job.json")
    h.run(ws)
    assert _failure_code(ws, healthy.job_id) == ("succeeded", None)
    # The job is skipped, never claimed.
    assert h.find(ws, hostile.job_id).path == hostile.path


@pytest.mark.xfail(
    strict=True,
    reason="the kernel reads job.json lazily (OwnedJob.header at the first release) with a blocking open: a FIFO "
    "the running job planted at job.json blocks the manager",
)
def test_a_fifo_job_definition_planted_by_a_running_job_fails_it_without_stalling_the_manager(
    ws: Workspace, installed: _store.Installed, outside: Path
) -> None:
    tamper = """
(job / "job.json").unlink()
os.mkfifo(job / "job.json")
"""
    hostile = _submit(ws, installed, outside, "hostile", tamper=tamper)
    healthy = _submit(ws, installed, outside, "healthy")
    with _Rescuer(ws.root) as rescuer, TaskManager(ws, heartbeat_interval=0.01, cancel_grace_seconds=0.5) as manager:
        manager.run_until_idle(timeout=_STALL_SECONDS)
        assert not manager.running_attempts
    assert not rescuer.fired, "the manager blocked on a FIFO the job planted"
    assert _failure_code(ws, healthy.job_id) == ("succeeded", None)
    state, code = _failure_code(ws, hostile.job_id)
    assert (state, code) == ("failed", "protocol_error")


def test_a_tampered_job_seal_path_is_a_discrepancy_not_an_error(tmp_path: Path) -> None:
    from httk.workflow.seals import INVALID, is_job_sealed, verify_job_seal

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "seal.json").write_text("{}", encoding="utf-8")
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / ".httk-job").symlink_to(outside, target_is_directory=True)
    verdict = verify_job_seal(payload)
    assert not verdict.valid and verdict.verdict == INVALID
    assert [(item.path, item.kind) for item in verdict.discrepancies] == [("payload", "invalid")]
    assert not is_job_sealed(payload)

    (payload / ".httk-job").unlink()
    (payload / ".httk-job").mkdir()
    os.mkfifo(payload / ".httk-job" / "seal.json")
    verdict = verify_job_seal(payload)
    assert [(item.path, item.kind) for item in verdict.discrepancies] == [("payload", "invalid")]
    assert not is_job_sealed(payload)
    assert json.loads((outside / "seal.json").read_text(encoding="utf-8")) == {}
