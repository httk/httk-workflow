"""The adapter ``session`` operation and :class:`AdapterSession`: one far-side process for many reads."""

import json
import os
import shlex
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote, register_ws
from httk.workflow import Workspace, adapter_runtime
from httk.workflow.adapters import AdapterSession, SessionUnsupported
from httk.workflow.projects import initialize_project
from test_adapter_refusal import _unrecognized
from test_job_paging import _job as _marker

HELLO = 'print(json.dumps({"format": "httk-workflow-session", "format_version": 1}), flush=True)'


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    initialize_project(project, name="adapter-session")
    return project


def _fake_server(tmp_path: Path, name: str, body: str) -> str:
    """Write a stand-in far-side ``httk`` and return the ``httk_command`` that runs it."""

    script = tmp_path / f"{name}.py"
    script.write_text(f"import json, sys, time\n{textwrap.dedent(body)}", encoding="utf-8")
    return shlex.join([sys.executable, str(script)])


def test_session_round_trips_over_one_ssh_login(tmp_path: Path, remote: Remote) -> None:
    project = _project(tmp_path)
    bundle = fake_remote(project)
    root = remote.root / "runs" / "workspace"
    job_id = _marker(Workspace.initialize(root), "ready", "jobs")
    register_ws(CLIContext("httk", project), root, "station")
    listing = ["httk", "job", "list", "--json", "--counts", "--limit", "5", "--workspace", "station"]

    with AdapterSession(bundle, timeout=60) as session:
        for _ in range(2):
            returncode, stdout, stderr = session.run(listing)
            assert returncode == 0, stderr
            assert [job["state"] for job in json.loads(stdout)["jobs"]] == ["ready"]
        returncode, stdout, stderr = session.run(
            ["httk", "job", "show", "--json", job_id, "--no-children", "--workspace", "station"]
        )
        assert returncode == 0, stderr
        assert job_id in stdout

    commands = remote.commands()
    assert len(commands) == 1 and "workflow session" in commands[0], commands


def test_session_skips_a_login_banner(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    project = _project(tmp_path)
    bundle = fake_remote(project)
    root = remote.root / "workspace"
    Workspace.initialize(root)
    register_ws(CLIContext("httk", project), root, "station")
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER", '{"motd": "welcome"}')
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER_WHEN", "workflow session")

    with AdapterSession(bundle, timeout=60) as session:
        returncode, stdout, stderr = session.run(["httk", "job", "list", "--json", "--workspace", "station"])
    assert returncode == 0, stderr
    assert json.loads(stdout)["jobs"] == []


def test_refused_login_is_not_a_fallback(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = fake_remote(_project(tmp_path))
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "someone@fake.example.test: Permission denied (publickey).")

    with pytest.raises(RuntimeError, match="Permission denied") as raised:
        AdapterSession(bundle, timeout=60).open()
    assert not isinstance(raised.value, SessionUnsupported)


def test_far_side_without_session_or_refusing_kind_is_unsupported(tmp_path: Path) -> None:
    project = _project(tmp_path)
    old = _fake_server(
        tmp_path, "old", 'print("httk: error: invalid choice: \'session\'", file=sys.stderr)\nsys.exit(2)'
    )
    bundle = fake_remote(project, template="local", name="old", httk_command=old)

    with pytest.raises(SessionUnsupported, match="invalid choice"):
        AdapterSession(bundle, timeout=60).open()
    with pytest.raises(SessionUnsupported, match="'torque' is not implemented"):
        AdapterSession(_unrecognized(project)).open()


def test_silent_session_times_out_and_is_killed(tmp_path: Path) -> None:
    silent = _fake_server(tmp_path, "silent", f"{HELLO}\nsys.stdin.readline()\ntime.sleep(600)")
    bundle = fake_remote(_project(tmp_path), template="local", httk_command=silent)

    session = AdapterSession(bundle, timeout=1)
    session.open()
    assert session._process is not None
    pid = session._process.pid
    with pytest.raises(TimeoutError):
        session.run(["httk", "job", "list"])
    assert session._process is None
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_wrong_response_id_closes_the_session(tmp_path: Path) -> None:
    body = f"""
    {HELLO}
    sys.stdin.readline()
    print(json.dumps({{"id": 999, "returncode": 0, "stdout": "", "stderr": ""}}), flush=True)
    time.sleep(600)
    """
    bundle = fake_remote(_project(tmp_path), template="local", httk_command=_fake_server(tmp_path, "wrong", body))

    with AdapterSession(bundle, timeout=10) as session, pytest.raises(RuntimeError, match="bad remote session"):
        session.run(["httk", "job", "list"])
    assert session._process is None


def test_ended_session_reopens_on_next_run(tmp_path: Path) -> None:
    starts = tmp_path / "starts"
    body = f"""
    with open({str(starts)!r}, "a") as log:
        log.write("start\\n")
    {HELLO}
    request = json.loads(sys.stdin.readline())
    print(json.dumps({{"id": request["id"], "returncode": 0, "stdout": "once", "stderr": ""}}), flush=True)
    """
    bundle = fake_remote(_project(tmp_path), template="local", httk_command=_fake_server(tmp_path, "once", body))

    with AdapterSession(bundle, timeout=10) as session:
        assert session.run(["httk", "job", "list"]) == (0, "once", "")
        assert session._process is not None
        session._process.wait(timeout=10)
        assert session.run(["httk", "job", "list"]) == (0, "once", "")
    assert starts.read_text(encoding="utf-8").splitlines() == ["start", "start"]


def test_no_controlling_terminal_means_no_prompt() -> None:
    completed = subprocess.run(
        [sys.executable, "-c", "from httk.workflow import adapter_runtime; print(adapter_runtime._terminal_present())"],
        start_new_session=True,
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    assert completed.stdout.strip() == "False"


def test_ssh_session_command_keeps_the_connection_alive(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[list[str]] = []
    monkeypatch.setattr(adapter_runtime.os, "execvp", lambda _file, argv: executed.append(list(argv)))
    adapter_runtime._session("ssh", {"argv": ["httk", "workflow", "session"], "remote_settings": {"host": "h"}})

    (command,) = executed
    transport = command[: command.index("--")]
    assert transport.count("ServerAliveInterval=15") == 1 and transport.count("ServerAliveCountMax=3") == 1
    assert command[-1].endswith("httk workflow session")


def test_close_during_open_is_not_a_fallback(tmp_path: Path) -> None:
    slow = _fake_server(tmp_path, "slow", f"time.sleep(3)\n{HELLO}\nsys.stdin.readline()\ntime.sleep(600)")
    bundle = fake_remote(_project(tmp_path), template="local", httk_command=slow)
    session = AdapterSession(bundle, timeout=30)
    raised: list[BaseException] = []

    def worker() -> None:
        try:
            session.run(["httk", "job", "list"])
        except BaseException as exc:
            raised.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    deadline = time.monotonic() + 10
    while session._process is None and time.monotonic() < deadline:
        time.sleep(0.02)
    session.close()
    thread.join(timeout=10)

    (error,) = raised
    assert isinstance(error, RuntimeError) and not isinstance(error, SessionUnsupported), error
    assert "closed" in str(error)
    assert session._process is None
