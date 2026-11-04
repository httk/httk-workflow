"""The monitor reads and acts on a remote workspace through one far-side session, or falls back per call."""

import os
import shlex
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, configure_identity, fake_remote, register_ws
from httk.workflow import Workspace, _kernel
from httk.workflow._state import read_state_unowned
from httk.workflow.adapters import AdapterSession
from httk.workflow.launchers import add_launcher
from httk.workflow.monitor import actions
from httk.workflow.monitor.data import WorkspaceView
from httk.workflow.projects import initialize_project
from test_adapter_session import HELLO, _fake_server
from test_moving import WORKFLOW, place
from v3_helpers import job_mapping


def _station(
    tmp_path: Path, remote: Remote, template: str = "ssh", **settings: str
) -> tuple[CLIContext, Workspace, str]:
    """A client project with remote ``cluster`` and the far-side workspace ``station`` holding one ready job."""

    project = tmp_path / "project"
    initialize_project(project, name="monitor-session")
    fake_remote(project, template=template, **settings)
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    job_id = place(workspace, job_mapping(WORKFLOW, {"start": "succeed"}, tag="watched"), "ready").job_id
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")
    return context, workspace, job_id


def test_monitor_reads_over_one_ssh_login(tmp_path: Path, remote: Remote) -> None:
    context, _workspace, job_id = _station(tmp_path, remote)
    view = WorkspaceView("cluster:station", context, adapter_timeout=60)

    view.connect()
    try:
        for _ in range(3):
            assert [row["job_id"] for row in view.page(limit=5).jobs] == [job_id]
            view.refresh()
        assert view.detail(job_id)["job_id"] == job_id
        assert isinstance(view.why(job_id), dict)
        assert isinstance(view.log(job_id), list)
    finally:
        view.close()
    commands = remote.commands()
    assert len(commands) == 1 and "workflow session" in commands[0], commands


def test_monitor_actions_share_the_session(tmp_path: Path, remote: Remote) -> None:
    configure_identity()
    context, workspace, job_id = _station(tmp_path, remote)
    # The far side's working directory is the remote root: its manager start resolves this Slurm profile there.
    initialize_project(remote.root, name="far-side")
    add_launcher("cluster", template="slurm", project=remote.root)
    workspace.set_setting("manager.launch", "cluster")
    view = WorkspaceView("cluster:station", context, adapter_timeout=60)

    view.connect()
    try:
        assert view.page(limit=5).jobs
        assert actions.request(view, "pause", [job_id], "checking") == "requested pause for 1 job(s)"
        assert actions.start_managers(view, 1) == "started 1 manager(s)"
    finally:
        view.close()
    assert len(list(remote.spool.glob("*.sbatch"))) == 1
    ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
    assert ref is not None
    state, _damage = read_state_unowned(ref.path / "state.json")
    assert state is not None
    assert [entry.get("action") for entry in state.history_tail if entry.get("event") == "request_applied"] == ["pause"]
    assert len(remote.commands()) == 1, remote.commands()


def test_monitor_falls_back_per_call_without_a_far_side_session(tmp_path: Path, remote: Remote) -> None:
    starts = tmp_path / "starts"
    old = tmp_path / "old_httk.py"
    old.write_text(
        "import os, sys\n"
        f"with open({str(starts)!r}, 'a') as log:\n"
        "    log.write(' '.join(sys.argv[1:3]) + '\\n')\n"
        "if sys.argv[1:3] == ['workflow', 'session']:\n"
        "    sys.exit(\"httk: error: invalid choice: 'session'\")\n"
        "os.execv(sys.executable, [sys.executable, '-m', 'httk.core.cli', *sys.argv[1:]])\n",
        encoding="utf-8",
    )
    context, _workspace, job_id = _station(
        tmp_path, remote, template="local", httk_command=shlex.join([sys.executable, str(old)])
    )
    view = WorkspaceView("cluster:station", context, adapter_timeout=60)

    view.connect()
    try:
        for _ in range(3):
            assert [row["job_id"] for row in view.page(limit=5).jobs] == [job_id]
            view.refresh()
    finally:
        view.close()
    assert starts.read_text(encoding="utf-8").splitlines() == ["workflow session", *["job list"] * 3]


def _sessions(remote: Remote) -> int:
    return sum("workflow session" in command for command in remote.commands())


def _refused_reads(view: WorkspaceView, job_id: str, *, concurrent: bool) -> None:
    """Run a page and a detail read, at once or one after the other, and expect both to be refused."""

    reads = (lambda: view.page(limit=5), lambda: view.detail(job_id))
    if concurrent:
        with ThreadPoolExecutor(len(reads)) as pool:
            futures = [pool.submit(read) for read in reads]
        errors = [future.exception() for future in futures]
    else:
        errors = []
        for read in reads:
            try:
                read()
            except RuntimeError as exc:
                errors.append(exc)
    assert len(errors) == 2 and all(isinstance(error, RuntimeError) for error in errors), errors
    assert all("Permission denied" in str(error) for error in errors), errors


def test_refused_login_waits_for_refresh_then_retries_once(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, _workspace, job_id = _station(tmp_path, remote)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "someone@fake.example.test: Permission denied (publickey).")
    view = WorkspaceView("cluster:station", context, adapter_timeout=60)

    view.connect()
    assert _sessions(remote) == 1
    for _ in range(2):
        _refused_reads(view, job_id, concurrent=False)
    assert _sessions(remote) == 1
    view.refresh()
    _refused_reads(view, job_id, concurrent=True)
    assert _sessions(remote) == 2
    _refused_reads(view, job_id, concurrent=False)
    assert _sessions(remote) == 2
    view.close()


def test_dead_session_with_refused_reconnect_waits_for_refresh(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, _workspace, job_id = _station(tmp_path, remote)
    view = WorkspaceView("cluster:station", context, adapter_timeout=60)
    view.connect()
    assert view.page(limit=5).jobs
    session = view._session
    assert session is not None and session._process is not None
    session._process.kill()
    session._process.wait(timeout=10)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "someone@fake.example.test: Permission denied (publickey).")

    view.expire()  # what a timed refresh does
    _refused_reads(view, job_id, concurrent=True)
    assert _sessions(remote) == 2
    view.expire()
    _refused_reads(view, job_id, concurrent=False)
    assert _sessions(remote) == 2
    view.refresh()
    _refused_reads(view, job_id, concurrent=True)
    assert _sessions(remote) == 3
    view.close()


def test_close_does_not_wait_for_a_round_trip_in_flight(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="close-in-flight")
    silent = _fake_server(tmp_path, "silent", f"{HELLO}\nsys.stdin.readline()\ntime.sleep(600)")
    session = AdapterSession(fake_remote(project, template="local", httk_command=silent), timeout=60)
    session.open()
    assert session._process is not None
    pid = session._process.pid
    raised: list[BaseException] = []

    def blocked() -> None:
        try:
            session.run(["httk", "job", "list"])
        except BaseException as exc:
            raised.append(exc)

    reader = threading.Thread(target=blocked)
    reader.start()
    time.sleep(0.5)
    started = time.monotonic()
    session.close()
    assert time.monotonic() - started < 10
    reader.join(timeout=10)
    assert not reader.is_alive()
    assert len(raised) == 1 and isinstance(raised[0], RuntimeError), raised
    assert session._process is None
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
