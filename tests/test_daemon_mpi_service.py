"""Real-process lifecycle tests for the confined MPI allocation service."""

import argparse
import json
import os
import selectors
import socket
import stat
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import httk.workflow._daemon_mpi_service as service_module
from httk.workflow._daemon_mpi_protocol import (
    decode_terminal,
    encode_request,
    recv_frame,
    send_frame,
)
from httk.workflow._daemon_policy import Policy, Profile

HANDLE = "a" * 32
REQUEST_ONE = "1" * 32
REQUEST_TWO = "2" * 32


def _write_srun(path: Path, root: Path, socket_path: Path) -> None:
    body = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root = Path({str(root)!r})
record = {{
    "argv": sys.argv[1:],
    "environment": dict(os.environ),
    "socket_exists": Path({str(socket_path)!r}).is_socket(),
}}
with (root / "calls.jsonl").open("a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, sort_keys=True) + "\\n")
    stream.flush()

if "--mpi=none" in sys.argv:
    mode = root / "manager-mode"
    if mode.exists():
        raise SystemExit(int(mode.read_text(encoding="ascii")))
    descendant = root / "manager-descendant"
    if descendant.exists():
        child_code = (
            "import os,signal,time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"open({{str(root / 'manager-child-pid')!r}}, 'w').write(str(os.getpid())); "
            "time.sleep(60)"
        )
        subprocess.Popen([sys.executable, "-c", child_code])
    marker = root / "manager-exit"
    while not marker.exists():
        time.sleep(0.01)
    text = marker.read_text(encoding="ascii").strip()
    raise SystemExit(int(text or "0"))

request_id = sys.argv[-1]
behavior = (root / (request_id + ".behavior")).read_text(encoding="ascii").strip()
if behavior.startswith("normal:"):
    sys.stdout.buffer.write(b"stdout-one\\n")
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(b"stderr-one\\n")
    sys.stderr.buffer.flush()
    sys.stdout.buffer.write(b"stdout-two\\n")
    sys.stdout.buffer.flush()
    raise SystemExit(int(behavior.split(":", 1)[1]))
if behavior == "hold":
    (root / (request_id + ".pid")).write_text(str(os.getpid()), encoding="ascii")
    while True:
        time.sleep(0.01)
if behavior == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    (root / (request_id + ".pid")).write_text(str(os.getpid()), encoding="ascii")
    while True:
        time.sleep(0.01)
if behavior == "flood":
    (root / (request_id + ".pid")).write_text(str(os.getpid()), encoding="ascii")
    sys.stdout.buffer.write(b"x" * (16 * 1024 * 1024))
    sys.stdout.buffer.flush()
    raise SystemExit(0)
if behavior == "descendant-pipes":
    child_code = (
        "import os,signal,time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"open({{str(root / (request_id + '.child-pid'))!r}}, 'w').write(str(os.getpid())); "
        "time.sleep(60)"
    )
    subprocess.Popen([sys.executable, "-c", child_code], stdout=sys.stdout, stderr=sys.stderr)
    raise SystemExit(0)
raise SystemExit(97)
"""
    path.write_text(body, encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)


def _configuration(
    tmp_path: Path, *, max_steps: int = 128, termination_grace: float = 0.1
) -> tuple[Path, Path, Policy, Profile, argparse.Namespace]:
    socket_path = tmp_path / "control.sock"
    srun = tmp_path / "srun"
    _write_srun(srun, tmp_path, socket_path)
    mpi = SimpleNamespace(srun=srun, max_steps=max_steps, termination_grace=termination_grace)
    policy = cast(
        Policy,
        SimpleNamespace(
            mpi=mpi,
            python=Path(sys.executable),
            slurm_conf=tmp_path / "slurm.conf",
        ),
    )
    profile = cast(
        Profile,
        SimpleNamespace(name="parallel", cpus=3, mpi=SimpleNamespace(nodes=2, ranks=4, ntasks_per_node=2)),
    )
    arguments = argparse.Namespace(
        job_id="123",
        node="node01",
        policy_source="/protected/policy.json",
        profile="parallel",
        handle=HANDLE,
        control_source="/protected/control/allocation",
        procs="12",
        mem_mb="2048",
    )
    return socket_path, srun, policy, profile, arguments


def _wait_for(predicate: Callable[[], object], message: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(message)


def _calls(root: Path) -> list[dict[str, object]]:
    path = root / "calls.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _start_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    max_steps: int = 128,
    termination_grace: float = 0.1,
) -> tuple[service_module._Service, threading.Thread, list[int], list[BaseException], Path, Path]:
    socket_path, srun, policy, profile, arguments = _configuration(
        tmp_path,
        max_steps=max_steps,
        termination_grace=termination_grace,
    )
    monkeypatch.setattr(service_module, "_SOCKET", socket_path)
    service = service_module._Service(policy, profile, arguments)
    results: list[int] = []
    errors: list[BaseException] = []

    def target() -> None:
        try:
            results.append(service.run())
        except BaseException as exc:  # pragma: no cover - asserted by caller
            errors.append(exc)

    thread = threading.Thread(target=target)
    thread.start()
    _wait_for(lambda: socket_path.is_socket() and bool(_calls(tmp_path)), "service did not start")
    return service, thread, results, errors, socket_path, srun


def _connect(socket_path: Path) -> socket.socket:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(2)
    connection.connect(str(socket_path))
    return connection


def _request(socket_path: Path, request_id: str) -> socket.socket:
    connection = _connect(socket_path)
    send_frame(connection, b"Q", encode_request(request_id))
    return connection


def _response(connection: socket.socket) -> tuple[bytes, bytes, int, str | None]:
    stdout = bytearray()
    stderr = bytearray()
    while True:
        kind, payload = recv_frame(connection)
        if kind == b"O":
            stdout.extend(payload)
        elif kind == b"E":
            stderr.extend(payload)
        elif kind == b"X":
            code, error = decode_terminal(payload)
            return bytes(stdout), bytes(stderr), code, error
        else:  # pragma: no cover - protocol helper prevents other service kinds
            raise AssertionError(kind)


def _stop_manager(root: Path, thread: threading.Thread, code: int = 0) -> None:
    (root / "manager-exit").write_text(str(code), encoding="ascii")
    thread.join(5)
    assert not thread.is_alive()


def _process_gone(path: Path) -> bool:
    if not path.exists():
        return False
    pid = int(path.read_text(encoding="ascii"))
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def test_fixed_commands_clean_environment_listener_order_and_nonzero_streaming(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("normal:7", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)
    with _request(socket_path, REQUEST_ONE) as connection:
        stdout, stderr, code, error = _response(connection)
    assert stdout == b"stdout-one\nstdout-two\n"
    assert stderr == b"stderr-one\n"
    assert (code, error) == (7, None)
    _stop_manager(tmp_path, thread)
    assert errors == [] and results == [0]

    manager, application = _calls(tmp_path)
    manager_argv = cast(list[str], manager["argv"])
    application_argv = cast(list[str], application["argv"])
    assert manager["socket_exists"] is True
    assert application["socket_exists"] is True
    assert manager["environment"] == {
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
        "SLURM_CONF": str(tmp_path / "slurm.conf"),
    }
    assert application["environment"] == manager["environment"]
    assert {"--mpi=none", "--nodes=1", "--ntasks=1", "--cpus-per-task=1", "--nodelist=node01"} <= set(manager_argv)
    assert "--ntasks-per-node=2" not in manager_argv
    assert {
        "--mpi=pmix",
        "--nodes=2",
        "--ntasks=4",
        "--ntasks-per-node=2",
        "--cpus-per-task=3",
        "--request-id",
        REQUEST_ONE,
    } <= set(application_argv)
    assert manager_argv[manager_argv.index("--control-source") :] == [
        "--control-source",
        "/protected/control/allocation",
        "--procs",
        "12",
        "--mem-mb",
        "2048",
    ]
    assert "--procs" not in application_argv and "--mem-mb" not in application_argv
    assert "--jobid=123" in manager_argv and "--jobid=123" in application_argv
    assert "--clusters" not in " ".join(manager_argv + application_argv)
    assert "solver" not in " ".join(application_argv)


def test_one_active_request_is_busy_and_disconnect_ends_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("hold", encoding="ascii")
    service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)
    active = _request(socket_path, REQUEST_ONE)
    _wait_for(lambda: service.application is not None, "application did not start")
    pid_path = tmp_path / f"{REQUEST_ONE}.pid"
    _wait_for(pid_path.exists, "application pid was not recorded")
    with _request(socket_path, REQUEST_TWO) as refused:
        assert _response(refused)[2:] == (2, "an MPI application is already active")
    active.close()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(pid_path), "application survived disconnect")


@pytest.mark.parametrize(
    ("max_steps", "request_id", "message"), [(128, REQUEST_ONE, "already"), (1, REQUEST_TWO, "limit")]
)
def test_used_ids_and_cumulative_quota_never_relaunch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    max_steps: int,
    request_id: str,
    message: str,
) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("normal:0", encoding="ascii")
    service, thread, results, errors, socket_path, _srun = _start_service(
        tmp_path,
        monkeypatch,
        max_steps=max_steps,
    )
    with _request(socket_path, REQUEST_ONE) as first:
        assert _response(first)[2:] == (0, None)
    with _request(socket_path, request_id) as refused:
        code, error = _response(refused)[2:]
    assert code == 2 and error is not None and message in error
    assert len(service.used) == 1
    _stop_manager(tmp_path, thread)
    assert errors == [] and results == [0]
    assert len(_calls(tmp_path)) == 2


def test_malformed_oversized_and_slow_requests_close_without_poisoning_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(service_module, "_IO_TIMEOUT", 0.1)
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("normal:0", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)

    for wire in (b"Q\x00\x00\x00\x02{}", b"Q\x00\x00\x10\x01"):
        with _connect(socket_path) as connection:
            connection.sendall(wire)
            assert connection.recv(1) == b""
    with _connect(socket_path) as slow:
        slow.sendall(b"Q\x00")
        assert slow.recv(1) == b""

    with _request(socket_path, REQUEST_ONE) as valid:
        assert _response(valid)[2:] == (0, None)
    _stop_manager(tmp_path, thread)
    assert errors == [] and results == [0]


def test_application_popen_failure_reserves_id_and_service_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, thread, results, errors, socket_path, srun = _start_service(tmp_path, monkeypatch)
    srun.unlink()
    with _request(socket_path, REQUEST_ONE) as connection:
        code, error = _response(connection)[2:]
    assert code == 2 and error is not None and "will not be retried" in error
    assert service.used == {REQUEST_ONE}
    _stop_manager(tmp_path, thread)
    assert errors == [] and results == [0]


def test_partial_application_registration_failure_stops_and_reaps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("hold", encoding="ascii")
    socket_path, _srun, policy, profile, arguments = _configuration(tmp_path)
    monkeypatch.setattr(service_module, "_SOCKET", socket_path)
    service = service_module._Service(policy, profile, arguments)
    register = service.selector.register

    def fail_stdout_registration(fileobj: Any, events: int, data: Any = None) -> selectors.SelectorKey:
        if data == "stdout":
            raise OSError("injected selector failure")
        return register(fileobj, events, data)

    monkeypatch.setattr(service.selector, "register", fail_stdout_registration)
    results: list[int] = []
    thread = threading.Thread(target=lambda: results.append(service.run()))
    thread.start()
    _wait_for(lambda: socket_path.is_socket() and bool(_calls(tmp_path)), "service did not start")
    with _request(socket_path, REQUEST_ONE) as connection:
        assert connection.recv(1) == b""
    thread.join(5)
    assert not thread.is_alive()
    assert results == [2]
    assert service.used == {REQUEST_ONE}


def test_stalled_output_reader_hits_write_deadline_and_ends_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(service_module, "_IO_TIMEOUT", 0.05)
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("flood", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(
        tmp_path,
        monkeypatch,
        termination_grace=0.05,
    )
    connection = _request(socket_path, REQUEST_ONE)
    pid_path = tmp_path / f"{REQUEST_ONE}.pid"
    _wait_for(pid_path.exists, "application pid was not recorded")
    thread.join(5)
    assert not thread.is_alive()
    connection.close()
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(pid_path), "application survived stalled output reader")


def test_terminal_delivery_failure_ends_allocation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("normal:0", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)

    def fail_terminal(_connection: socket.socket, _code: int, _error: str | None = None) -> None:
        raise BrokenPipeError

    monkeypatch.setattr(service_module._Service, "_terminal", staticmethod(fail_terminal))
    with _request(socket_path, REQUEST_ONE) as connection:
        while connection.recv(65_536):
            pass
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [2]


def test_manager_startup_failure_sees_listener_and_reports_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "manager-mode").write_text("9", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [9]
    assert _calls(tmp_path)[0]["socket_exists"] is True
    assert not socket_path.exists()


def test_manager_popen_failure_closes_listener_and_removes_owned_socket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    socket_path, srun, policy, profile, arguments = _configuration(tmp_path)
    srun.unlink()
    monkeypatch.setattr(service_module, "_SOCKET", socket_path)
    service = service_module._Service(policy, profile, arguments)
    with pytest.raises(FileNotFoundError):
        service.run()
    assert not socket_path.exists()
    assert service.selector.get_map() is None


def test_final_manager_group_cleanup_changes_success_to_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "manager-descendant").touch()
    _service, thread, results, errors, _socket_path, _srun = _start_service(
        tmp_path,
        monkeypatch,
        termination_grace=0.05,
    )
    child_path = tmp_path / "manager-child-pid"
    _wait_for(child_path.exists, "manager descendant pid was not recorded")
    _stop_manager(tmp_path, thread)
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(child_path), "manager descendant survived cleanup")


def test_manager_failure_during_application_cancels_step_and_fails_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("hold", encoding="ascii")
    service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)
    connection = _request(socket_path, REQUEST_ONE)
    _wait_for(lambda: service.application is not None, "application did not start")
    (tmp_path / "manager-exit").write_text("8", encoding="ascii")
    assert connection.recv(1) == b""
    connection.close()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(tmp_path / f"{REQUEST_ONE}.pid"), "application survived manager failure")


def test_term_ignoring_process_group_is_killed_on_client_disconnect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("ignore-term", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(
        tmp_path,
        monkeypatch,
        termination_grace=0.05,
    )
    connection = _request(socket_path, REQUEST_ONE)
    pid_path = tmp_path / f"{REQUEST_ONE}.pid"
    _wait_for(pid_path.exists, "application pid was not recorded")
    connection.close()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(pid_path), "TERM-ignoring application survived KILL")


def test_descendant_held_pipes_force_uncertain_cleanup_and_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(service_module, "_IO_TIMEOUT", 0.1)
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("descendant-pipes", encoding="ascii")
    _service, thread, results, errors, socket_path, _srun = _start_service(
        tmp_path,
        monkeypatch,
        termination_grace=0.05,
    )
    connection = _request(socket_path, REQUEST_ONE)
    child_path = tmp_path / f"{REQUEST_ONE}.child-pid"
    _wait_for(child_path.exists, "descendant pid was not recorded")
    assert connection.recv(1) == b""
    connection.close()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(child_path), "pipe-holding descendant survived cleanup")


def test_explicit_shutdown_closes_client_and_reaps_both_groups(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / f"{REQUEST_ONE}.behavior").write_text("hold", encoding="ascii")
    service, thread, results, errors, socket_path, _srun = _start_service(tmp_path, monkeypatch)
    connection = _request(socket_path, REQUEST_ONE)
    pid_path = tmp_path / f"{REQUEST_ONE}.pid"
    _wait_for(pid_path.exists, "application pid was not recorded")
    service.failed = True
    service.stop = True
    assert connection.recv(1) == b""
    connection.close()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [] and results == [2]
    _wait_for(lambda: _process_gone(pid_path), "application survived service shutdown")


def test_application_step_without_profile_cpus_uses_srun_default(tmp_path: Path) -> None:
    _socket_path, _srun, policy, profile, arguments = _configuration(tmp_path)
    profile = cast(Profile, SimpleNamespace(name="parallel", cpus=None, mpi=SimpleNamespace(nodes=2, ranks=4)))
    arguments.mem_mb = None
    application = service_module._command(policy, profile, arguments, REQUEST_ONE)
    assert not any(item.startswith("--cpus-per-task") for item in application)
    manager = service_module._command(policy, profile, arguments, None)
    assert "--cpus-per-task=1" in manager
    assert manager[-2:] == ["--procs", "12"]


@pytest.mark.parametrize("procs", ["0", "012", "4x", str(2**63), " 4"])
def test_main_refuses_malformed_capacity(
    procs: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _socket_path, _srun, policy, profile, arguments = _configuration(tmp_path)
    monkeypatch.setattr(service_module, "load_policy", lambda _path: policy)
    monkeypatch.setattr(policy, "profile", lambda _name: profile, raising=False)
    started: list[object] = []
    monkeypatch.setattr(service_module, "_Service", lambda *args: started.append(args))
    argv = [
        "--policy-source",
        arguments.policy_source,
        "--profile",
        "parallel",
        "--handle",
        HANDLE,
        "--job-id",
        "123",
        "--node",
        "node01",
        "--control-source",
        arguments.control_source,
        "--procs",
        procs,
    ]
    assert service_module.main(argv) == 2
    assert "--procs" in capsys.readouterr().err
    assert started == []
