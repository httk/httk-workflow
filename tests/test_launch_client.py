"""The launch client against a fake manager that watches the attempt's ``launch/`` directory."""

import fcntl
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _launch_client
from httk.workflow._launch_protocol import (
    LaunchRequest,
    LaunchStatus,
    decode_request,
    encode_status,
    lock_name,
    status_name,
    stderr_name,
    stdout_name,
    stop_name,
)

_SRC = str(Path(__file__).parents[1] / "src")
_TIMEOUT = 30.0


@dataclass
class _Attempt:
    job: Path
    control: Path
    attempt_id: str
    locks: Path

    @property
    def launch(self) -> Path:
        return self.control / "launch"

    def environment(self) -> dict[str, str]:
        return {
            "HTTK_WORKFLOW_CONTROL_DIR": str(self.control),
            "HTTK_WORKFLOW_JOB_DIR": str(self.job),
            "HTTK_WORKFLOW_LAUNCH_LOCKS": str(self.locks),
            "HTTK_WORKFLOW_CONTEXT": json.dumps({"attempt_id": self.attempt_id, "step": "main"}),
        }


@pytest.fixture
def attempt(tmp_path: Path) -> _Attempt:
    attempt_id = str(uuid.uuid4())
    job = tmp_path / "ws" / "jobs" / str(uuid.uuid4())
    control = job / "attempts" / attempt_id
    control.mkdir(parents=True)
    (job / "sub").mkdir()
    # Stands in for the manager's ``<shm_root>/httk-launch-<attempt_id>`` directory on host tmpfs.
    locks = tmp_path / "shm" / f"httk-launch-{attempt_id}"
    locks.mkdir(parents=True, mode=0o700)
    return _Attempt(job, control, attempt_id, locks)


def _start(attempt: _Attempt, *command: str, cwd: Path | None = None, **extra: str) -> subprocess.Popen[bytes]:
    environment = os.environ | attempt.environment() | extra
    environment["PYTHONPATH"] = _SRC
    return subprocess.Popen(
        [sys.executable, "-m", "httk.workflow._launch_client", *command],
        cwd=attempt.job / "sub" if cwd is None else cwd,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _lock_held(path: Path) -> bool:
    descriptor = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    finally:
        os.close(descriptor)
    return False


def _wait_for_request(attempt: _Attempt, process: subprocess.Popen[bytes]) -> LaunchRequest:
    """Wait for the published request, checking that the client held its lock before publishing."""

    deadline = time.monotonic() + _TIMEOUT
    while True:
        names = sorted(attempt.launch.glob("*.request.json")) if attempt.launch.is_dir() else []
        if names:
            request = decode_request(names[0].read_bytes())
            assert _lock_held(attempt.locks / lock_name(request.request_id)), "request published without a held lock"
            return request
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(f"client exited early with {process.returncode}: {stdout!r} {stderr!r}")
        assert time.monotonic() < deadline, "the client did not publish a request"
        time.sleep(0.01)


def _create_stream(attempt: _Attempt, name: str) -> int:
    return os.open(attempt.launch / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)


def _write_status(attempt: _Attempt, status: LaunchStatus) -> None:
    temporary = attempt.launch / f".{status.request_id}.status.tmp"
    temporary.write_bytes(encode_status(status))
    os.replace(temporary, attempt.launch / status_name(status.request_id))


@pytest.fixture
def clients() -> Iterator[list[subprocess.Popen[bytes]]]:
    started: list[subprocess.Popen[bytes]] = []
    yield started
    for process in started:
        if process.poll() is None:
            process.kill()
        process.communicate()


def test_streams_are_reproduced_separately_and_exit_code_returned(
    attempt: _Attempt, clients: list[subprocess.Popen[bytes]]
) -> None:
    process = _start(attempt, "mpi_app", "a b", "", FOO="bar", PMI_RANK="0", HTTK_X="1", SLURM_JOB_ID="7")
    clients.append(process)
    request = _wait_for_request(attempt, process)
    assert request.attempt_id == attempt.attempt_id
    assert request.argv == ("mpi_app", "a b", "")
    assert request.cwd == "sub"
    names = dict(request.environment)
    assert names["FOO"] == "bar"
    assert not any(name.startswith(("PMI_", "HTTK_", "SLURM_")) for name in names)
    out = _create_stream(attempt, stdout_name(request.request_id))
    err = _create_stream(attempt, stderr_name(request.request_id))
    try:
        os.write(out, b"first \xff\n")
        os.write(err, b"warning 1\n")
        time.sleep(0.3)
        assert process.poll() is None
        os.write(out, b"second\n")
        os.write(err, b"warning 2\n")
    finally:
        os.close(out)
        os.close(err)
    _write_status(attempt, LaunchStatus(request.request_id, "exited", 3))
    stdout, stderr = process.communicate(timeout=_TIMEOUT)
    assert process.returncode == 3
    assert stdout == b"first \xff\nsecond\n"
    assert stderr == b"warning 1\nwarning 2\n"


def test_request_from_the_job_directory_itself(attempt: _Attempt, clients: list[subprocess.Popen[bytes]]) -> None:
    process = _start(attempt, "true", cwd=attempt.job)
    clients.append(process)
    request = _wait_for_request(attempt, process)
    assert request.cwd == "."
    _write_status(attempt, LaunchStatus(request.request_id, "exited", 0))
    assert process.wait(timeout=_TIMEOUT) == 0


@pytest.mark.parametrize(
    ("state", "exit_code", "error", "expected"),
    [
        ("exited", 0, None, 0),
        ("exited", 137, None, 137),
        ("stopped", 9, None, 9),
        ("stopped", None, "stopped with the attempt", 143),
        ("refused", None, "another launch of this attempt is active", 2),
        ("uncertain", None, "the launch could not be reaped", 2),
    ],
)
def test_exit_code_mapping(
    attempt: _Attempt,
    clients: list[subprocess.Popen[bytes]],
    state: str,
    exit_code: int | None,
    error: str | None,
    expected: int,
) -> None:
    process = _start(attempt, "app")
    clients.append(process)
    request = _wait_for_request(attempt, process)
    _write_status(attempt, LaunchStatus(request.request_id, state, exit_code, error))  # type: ignore[arg-type]
    stdout, stderr = process.communicate(timeout=_TIMEOUT)
    assert process.returncode == expected
    assert stdout == b""
    if error is not None:
        assert error in stderr.decode()


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT, signal.SIGHUP])
def test_signal_creates_stop_and_waits_for_the_stopped_status(
    attempt: _Attempt, clients: list[subprocess.Popen[bytes]], signum: signal.Signals
) -> None:
    process = _start(attempt, "app")
    clients.append(process)
    request = _wait_for_request(attempt, process)
    out = _create_stream(attempt, stdout_name(request.request_id))
    process.send_signal(signum)
    stop = attempt.launch / stop_name(request.request_id)
    deadline = time.monotonic() + _TIMEOUT
    while not stop.exists():
        assert process.poll() is None, "the client exited on a stop signal"
        assert time.monotonic() < deadline, "the client did not create its stop marker"
        time.sleep(0.01)
    process.send_signal(signum)
    time.sleep(0.3)
    assert process.poll() is None, "the client exited before the stopped status"
    assert _lock_held(attempt.locks / lock_name(request.request_id))
    os.write(out, b"ranks reaped\n")
    os.close(out)
    _write_status(attempt, LaunchStatus(request.request_id, "stopped"))
    stdout, _stderr = process.communicate(timeout=_TIMEOUT)
    assert process.returncode == 143
    assert stdout == b"ranks reaped\n"


def test_sigkill_releases_the_lock(attempt: _Attempt, clients: list[subprocess.Popen[bytes]]) -> None:
    process = _start(attempt, "app")
    clients.append(process)
    request = _wait_for_request(attempt, process)
    lock = attempt.locks / lock_name(request.request_id)
    assert _lock_held(lock)
    process.kill()
    process.wait(timeout=_TIMEOUT)
    assert not _lock_held(lock)
    assert not (attempt.launch / stop_name(request.request_id)).exists()


def test_a_removed_launch_directory_ends_the_client(attempt: _Attempt, clients: list[subprocess.Popen[bytes]]) -> None:
    process = _start(attempt, "app")
    clients.append(process)
    _wait_for_request(attempt, process)
    shutil.rmtree(attempt.launch)
    _stdout, stderr = process.communicate(timeout=5)
    assert process.returncode == 2
    assert b"the launch directory was removed before its status arrived" in stderr


def _in_process(
    monkeypatch: pytest.MonkeyPatch, attempt: _Attempt, cwd: Path, environment: dict[str, str] | None = None
) -> None:
    for name in (
        "HTTK_WORKFLOW_CONTROL_DIR",
        "HTTK_WORKFLOW_JOB_DIR",
        "HTTK_WORKFLOW_CONTEXT",
        "HTTK_WORKFLOW_LAUNCH_LOCKS",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in (attempt.environment() if environment is None else environment).items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(cwd)


def test_the_lock_lives_in_the_lock_directory_not_the_launch_directory(
    attempt: _Attempt, clients: list[subprocess.Popen[bytes]]
) -> None:
    process = _start(attempt, "app")
    clients.append(process)
    request = _wait_for_request(attempt, process)
    assert (attempt.locks / lock_name(request.request_id)).is_file()
    assert not (attempt.launch / lock_name(request.request_id)).exists()


def test_a_missing_lock_directory_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt
) -> None:
    shutil.rmtree(attempt.locks)
    _in_process(monkeypatch, attempt, attempt.job)
    assert _launch_client.main(["app"]) == 2
    assert "HTTK_WORKFLOW_LAUNCH_LOCKS" in capsys.readouterr().err
    assert not attempt.launch.exists()


def test_a_symlinked_lock_directory_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt
) -> None:
    real = attempt.locks.with_name("real")
    attempt.locks.rename(real)
    attempt.locks.symlink_to(real)
    _in_process(monkeypatch, attempt, attempt.job)
    assert _launch_client.main(["app"]) == 2
    assert "HTTK_WORKFLOW_LAUNCH_LOCKS" in capsys.readouterr().err
    assert list(real.iterdir()) == []


def test_a_pre_existing_lock_file_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt
) -> None:
    request_id = "a" * 32
    monkeypatch.setattr(_launch_client, "new_request_id", lambda: request_id)
    forged = attempt.locks / lock_name(request_id)
    forged.write_bytes(b"")
    _in_process(monkeypatch, attempt, attempt.job)
    assert _launch_client.main(["app"]) == 2
    assert "cannot create the client lock" in capsys.readouterr().err
    assert not list(attempt.launch.glob("*.request.json"))
    assert forged.exists()


def test_a_flock_failure_is_a_plain_refusal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt
) -> None:
    _in_process(monkeypatch, attempt, attempt.job)

    def failing(_descriptor: int, _operation: int) -> None:
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(fcntl, "flock", failing)
    assert _launch_client.main(["app"]) == 2
    assert "cannot lock the client lock" in capsys.readouterr().err
    assert list(attempt.locks.iterdir()) == []


@pytest.mark.parametrize(
    "missing",
    ["HTTK_WORKFLOW_CONTROL_DIR", "HTTK_WORKFLOW_JOB_DIR", "HTTK_WORKFLOW_CONTEXT", "HTTK_WORKFLOW_LAUNCH_LOCKS"],
)
def test_missing_environment_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt, missing: str
) -> None:
    environment = attempt.environment()
    del environment[missing]
    _in_process(monkeypatch, attempt, attempt.job, environment)
    assert _launch_client.main(["app"]) == 2
    assert missing in capsys.readouterr().err
    assert not attempt.launch.exists()


@pytest.mark.parametrize(
    "context",
    [
        "not json",
        "[]",
        "{}",
        json.dumps({"attempt_id": "x"}),
        json.dumps({"attempt_id": "5e0f2dce-e297-4a38-bd5e-9e9e36bb5962"}),
    ],
)
def test_invalid_attempt_context_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt, context: str
) -> None:
    _in_process(monkeypatch, attempt, attempt.job, attempt.environment() | {"HTTK_WORKFLOW_CONTEXT": context})
    assert _launch_client.main(["app"]) == 2
    assert "HTTK_WORKFLOW_" in capsys.readouterr().err
    assert not attempt.launch.exists()


def test_empty_command_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt
) -> None:
    _in_process(monkeypatch, attempt, attempt.job)
    assert _launch_client.main([]) == 2
    assert "no command given" in capsys.readouterr().err
    assert not attempt.launch.exists()


def test_working_directory_outside_the_job_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt, tmp_path: Path
) -> None:
    outside = tmp_path / "ws" / "jobs"
    _in_process(monkeypatch, attempt, outside)
    assert _launch_client.main(["app"]) == 2
    assert "is not inside the job directory" in capsys.readouterr().err
    sibling = tmp_path / "ws" / "jobs" / (attempt.job.name + "x")
    sibling.mkdir()
    _in_process(monkeypatch, attempt, sibling)
    assert _launch_client.main(["app"]) == 2
    link = attempt.job / "escape"
    link.symlink_to(sibling)
    _in_process(monkeypatch, attempt, link)
    assert _launch_client.main(["app"]) == 2
    assert not attempt.launch.exists()


def test_oversized_request_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], attempt: _Attempt
) -> None:
    _in_process(monkeypatch, attempt, attempt.job)
    assert _launch_client.main(["app", "x" * (64 * 1024 + 1)]) == 2
    assert "protocol bounds" in capsys.readouterr().err
    assert not attempt.launch.exists()
