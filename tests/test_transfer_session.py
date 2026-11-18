"""The transfer session: the far-side ``httk workflow transfer-session`` and :class:`TransferSession`."""

import contextlib
import io
import json
import os
import shlex
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import IO, Any, BinaryIO

import pytest

from conftest import Remote, fake_remote
from httk.workflow import _frames, adapters
from httk.workflow._frames import (
    DATA,
    END,
    PROGRESS,
    REQUEST,
    RESPONSE,
    Frame,
    FrameReader,
    FrameWriter,
    encode_frame,
    encode_json,
)
from httk.workflow.adapters import (
    TRANSFER_SESSION_FORMAT,
    RequestAborted,
    SessionFailed,
    SessionUnsupported,
    TransferSession,
)
from httk.workflow.errors import StreamAborted
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer_session
from httk.workflow.workflow_cli._transfer_session import OPS, Op, Request, serve
from test_adapter_refusal import _unrecognized

MiB = 1 << 20

#: A stand-in far side: the real serve loop with the ops ``OPS`` the body defines added (or replaced). An op may
#: write raw frames through ``request._frames`` to break the grammar on purpose.
FAR_SIDE = """
import os, subprocess, sys, threading, time
from httk.workflow import _frames
from httk.workflow._frames import DATA, END, PROGRESS, RESPONSE
from httk.workflow.errors import StreamAborted
from httk.workflow.workflow_cli import _transfer_session as session

{body}

with open({starts!r}, "a") as log:
    log.write("start\\n")
sys.exit(session.serve(sys.stdin.buffer, sys.stdout.buffer, ops={{**session.OPS, **OPS}}))
"""

#: Far-side ops several tests share.
COMMON_OPS = """
def grandchild(request):
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    with open(request.header["pidfile"], "w") as stream:
        stream.write(str(child.pid))
    return child

def silent(request):
    if "pidfile" in request.header:
        grandchild(request)
    time.sleep(60)
    return {}

def vanish(request):
    grandchild(request)
    os._exit(0)

def boom(request):
    if "pidfile" in request.header:
        grandchild(request)
    raise KeyError("lost owner")

def refuse(request):
    raise session.Refused("refused")

def unread(request):
    return {}

def keep(request):
    try:
        request.upload.finish()
    except StreamAborted:
        pass
    if not request.upload.succeeded:
        raise session.Refused("aborted", terminal=request.upload.terminal)
    return {"terminal": request.upload.terminal}

def big(request):
    if request.header.get("stuck"):
        request.progress("extract", 3)
    with request.download() as stream:
        for _ in range(request.header["mib"]):
            stream.write(bytes(1 << 20))
    return {}

def half(request):
    stream = request.download()
    stream.write(b"half")
    stream.flush()
    stream.close(ok=False, abort="the source went away")
    raise session.Refused("half")

def nothing(request):
    if request.header.get("ok") is False:
        raise session.Refused("nothing")
    return {}

def mark(request):
    with open(request.header["marker"], "w") as stream:
        stream.write("ran")
    return {}

def stale(request):
    frames = request._frames
    threading.Timer(0.2, lambda: frames.send(DATA, request.id, b"late")).start()
    return {}

def answer_early(request):
    request._frames.send_json(RESPONSE, request.id, {"ok": True})
    time.sleep(3)
    request.upload.discard()
    raise session.Refused("early")

def stand(request):
    request.progress(request.header["phase"], 3)
    time.sleep(request.header["seconds"])
    if request.upload is not None:
        request.upload.discard()
        raise session.Refused("stood")
    return {}

def slow(request):
    request.progress("adopt", 0)
    for _ in range(request.header["steps"]):
        time.sleep(0.1)
        request.advance()
    return {"steps": request.header["steps"]}

def trickle(request):
    request.progress("extract", 1)
    with request.download() as stream:
        for _ in range(request.header["pieces"]):
            stream.write(b"x" * 1024)
            stream.flush()
            time.sleep(request.header["gap"])
    return {}

def pause(request):
    with request.download() as stream:
        stream.write(b"a")
        stream.flush()
        time.sleep(request.header["gap"])
        stream.write(b"b")
    return {}

def violate(request):
    frames, rid, how = request._frames, request.id, request.header["how"]
    if how == "foreign id":
        frames.send_json(PROGRESS, rid + 7, {"phase": "x", "count": 1})
    elif how == "response inside download":
        frames.send(DATA, rid, b"x")
    elif how in ("data after terminal", "second terminal"):
        with request.download() as stream:
            stream.write(b"x")
        if how == "data after terminal":
            frames.send(DATA, rid, b"y")
        else:
            frames.send_json(END, rid, {"ok": True})
    elif how == "malformed progress":
        frames.send_json(PROGRESS, rid, {"phase": 3})
    return {}

def take(request):
    request.progress("extract", 3)
    while request.upload.read(65536):
        if request.header["counting"]:
            request.progress("extract", request.upload.bytes)
    request.upload.finish()
    return {"bytes": request.upload.bytes}

def sip(request):
    request.progress("extract", 3)
    while request.upload.read(1 << 20):
        time.sleep(0.15)
    request.upload.finish()
    return {"bytes": request.upload.bytes}

def empties(request):
    request.progress("extract", 3)
    for _ in range(12):
        request._frames.send(DATA, request.id, b"")
        time.sleep(0.9)
    request._frames.send_json(END, request.id, {"ok": True})
    return {}

def partial(request):
    request._frames.send(DATA, request.id, b"x")
    sys.stdout.buffer.write(b"R\\x00\\x00")
    sys.stdout.buffer.flush()
    os._exit(0)

OPS = {name: op for name, op in dict(globals()).items() if callable(op) and name in {
    "silent", "vanish", "boom", "refuse", "unread", "keep", "big", "half", "nothing", "mark", "stale",
    "answer_early", "stand", "slow", "trickle", "pause", "violate", "take", "sip", "empties", "partial"}}
"""


@pytest.fixture
def quick(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the progress interval this process assumes, so a request's bound may be short."""

    monkeypatch.setattr(_frames, "TICK", 0.1)


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    initialize_project(project, name="transfer-session")
    return project


def _starts(tmp_path: Path) -> list[str]:
    log = tmp_path / "starts"
    return log.read_text(encoding="utf-8").splitlines() if log.exists() else []


def _far_side(tmp_path: Path, body: str = "", *, tick: float = 10.0, template: str = "local") -> Path:
    """Return a remote whose far side serves :data:`COMMON_OPS`, the ops *body* adds, at progress interval *tick*."""

    script = tmp_path / "far_side.py"
    source = f"_frames.TICK = {tick!r}\n{textwrap.dedent(COMMON_OPS)}\n{textwrap.dedent(body)}"
    script.write_text(FAR_SIDE.format(body=source, starts=str(tmp_path / "starts")), encoding="utf-8")
    return fake_remote(
        _project(tmp_path), template=template, name="far", httk_command=shlex.join([sys.executable, str(script)])
    )


def _script_remote(tmp_path: Path, name: str, source: str, *, template: str = "local") -> Path:
    script = tmp_path / f"{name}.py"
    script.write_text(textwrap.dedent(source), encoding="utf-8")
    project = tmp_path / f"project-{name}"
    initialize_project(project, name=name)
    return fake_remote(project, template=template, name=name, httk_command=shlex.join([sys.executable, str(script)]))


def _opened(bundle: Path, bound: float | None = None, **options: Any) -> TransferSession:
    """Open a session with a generous bound for the interpreter start, then tighten it for the requests."""

    session = TransferSession(bundle, timeout=60, **options)
    session.open()
    if bound is not None:
        session._limit = bound
    return session


def _writer(*chunks: bytes) -> Callable[[BinaryIO], None]:
    def write(stream: BinaryIO) -> None:
        stream.writelines(chunks)

    return write


def _collector() -> tuple[Callable[[BinaryIO], None], bytearray]:
    received = bytearray()

    def read(stream: BinaryIO) -> None:
        while chunk := stream.read(65536):
            received.extend(chunk)

    return read, received


def _echo(session: TransferSession, data: bytes) -> tuple[dict[str, Any], bytes]:
    read, received = _collector()
    chunks = [data[offset : offset + 300_000] for offset in range(0, len(data), 300_000)]
    response = session.request("echo", upload=_writer(*chunks), download=read)
    return response, bytes(received)


def _dead(pid: int) -> bool:
    """Whether *pid* is gone (or a zombie awaiting its reaper)."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(")")[-1].split()[0] == "Z"
    except OSError:
        return True


def _wait_dead(pid: int) -> bool:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if _dead(pid):
            return True
        time.sleep(0.05)
    return False


# Opening, fallback and closing.


def test_hello_echo_and_bye_over_one_ssh_login(tmp_path: Path, remote: Remote) -> None:
    bundle = fake_remote(_project(tmp_path))
    payload = os.urandom(900_000) + bytes(range(256)) * 9

    with _opened(bundle) as session:
        assert {"echo", "bye"} <= session.ops
        response, received = _echo(session, payload)
        assert response == {"ok": True, "bytes": len(payload)}
        assert received == payload
        assert session._process is not None and os.getpgid(session._process.pid) == session._process.pid
    with pytest.raises(SessionFailed, match="closed"):
        session.request("nothing")

    commands = remote.commands()
    assert len(commands) == 1 and "workflow transfer-session" in commands[0], commands


def test_close_says_bye_and_the_far_side_ends_cleanly(tmp_path: Path, remote: Remote) -> None:
    said = tmp_path / "said-bye"
    body = f"""
    def bye(request):
        with open({str(said)!r}, "w") as stream:
            stream.write("bye")
        return session.OPS["bye"](request)

    OPS["bye"] = bye
    """
    session = _opened(_far_side(tmp_path, body))
    session.close()
    assert said.read_text() == "bye"
    assert session._status == 0  # it ended on its own after the bye, not by a kill
    assert session._reader is not None and not session._reader.is_alive()


def test_a_far_side_without_the_command_is_unsupported_once(tmp_path: Path, remote: Remote) -> None:
    old = tmp_path / "old.py"
    old.write_text(
        "import sys\nprint(\"httk: error: invalid choice: 'transfer-session'\", file=sys.stderr)\nsys.exit(2)\n"
    )
    bundle = fake_remote(_project(tmp_path), httk_command=shlex.join([sys.executable, str(old)]))

    session = TransferSession(bundle, timeout=60)
    with pytest.raises(SessionUnsupported, match="invalid choice"):
        session.open()
    with pytest.raises(SessionUnsupported, match="invalid choice"):
        session.open()  # decided once: no second login
    with pytest.raises(SessionFailed):
        session.request("echo")
    assert len(remote.commands()) == 1


def test_the_setting_off_starts_nothing(tmp_path: Path, remote: Remote) -> None:
    bundle = fake_remote(_project(tmp_path), transfer_session="off")
    with pytest.raises(SessionUnsupported, match="transfer_session is off"):
        TransferSession(bundle, timeout=60).open()
    assert remote.commands() == []

    bogus = fake_remote(_project(tmp_path / "other"), transfer_session="sometimes")
    with pytest.raises(ValueError, match="auto or off"):
        TransferSession(bogus, timeout=60).open()
    assert remote.commands() == []


def test_ssh_status_255_is_a_connection_failure_at_every_stage_and_only_for_ssh(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    hello = json.dumps({"format": TRANSFER_SESSION_FORMAT, "format_version": 1, "ops": ["bye", "echo"]})
    at_echo = f"""
    import os, sys
    print({hello!r}, flush=True)
    sys.stdin.buffer.read(1)
    os._exit(255)
    """
    with pytest.raises(RuntimeError, match="ssh connection failed") as raised:
        TransferSession(_script_remote(tmp_path, "echo255", at_echo, template="ssh"), timeout=60).open()
    assert not isinstance(raised.value, SessionUnsupported)
    with pytest.raises(SessionUnsupported, match="exit status 255"):
        TransferSession(_script_remote(tmp_path, "local255", at_echo), timeout=60).open()

    project = _project(tmp_path)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "someone@fake.example.test: Permission denied (publickey).")
    with pytest.raises(RuntimeError, match="Permission denied") as raised:
        TransferSession(fake_remote(project), timeout=60).open()
    assert not isinstance(raised.value, SessionUnsupported)
    with pytest.raises(SessionUnsupported, match="'torque' is not implemented"):
        TransferSession(_unrecognized(project), timeout=60).open()


def test_a_missing_needed_op_is_unsupported(tmp_path: Path, remote: Remote) -> None:
    bundle = fake_remote(_project(tmp_path), template="local")
    with pytest.raises(SessionUnsupported, match="lacks the ops frobnicate"):
        TransferSession(bundle, needs=["frobnicate", "adopt", "echo"], timeout=60).open()


def test_no_hello_another_format_or_another_version_is_unsupported(tmp_path: Path) -> None:
    other = json.dumps({"format": "httk-workflow-session", "format_version": 1})
    newer = json.dumps({"format": TRANSFER_SESSION_FORMAT, "format_version": 2, "ops": ["bye", "echo"]})
    cases = {
        "silent": ("print('welcome to the cluster')\n", "exit status 0"),
        "other": (f"import sys\nprint({other!r}, flush=True)\nsys.stdin.read()\n", "httk-workflow-session"),
        "newer": (f"import sys\nprint({newer!r}, flush=True)\nsys.stdin.read()\n", "version: 2"),
    }
    for name, (source, message) in cases.items():
        started = time.monotonic()
        with pytest.raises(SessionUnsupported, match=message):
            TransferSession(_script_remote(tmp_path, name, source), timeout=60).open()
        assert time.monotonic() - started < 30  # decided at once, not by the hello timeout


def test_a_mangling_channel_is_unsupported(tmp_path: Path, remote: Remote) -> None:
    body = """
    def mangle(request):
        data = request.upload.read()
        request.upload.finish()
        with request.download() as stream:
            stream.write(data.replace(b"\\r", b"\\n"))  # what a text-mode channel might do
        return {}

    OPS["echo"] = mangle
    """
    with pytest.raises(SessionUnsupported, match="binary data intact"):
        TransferSession(_far_side(tmp_path, body), timeout=60).open()


def test_close_from_another_thread_while_opening_is_not_a_fallback(tmp_path: Path) -> None:
    hello = json.dumps({"format": TRANSFER_SESSION_FORMAT, "format_version": 1, "ops": ["bye", "echo"]})
    slow = f"import sys, time\ntime.sleep(3)\nprint({hello!r}, flush=True)\nsys.stdin.read()\n"
    session = TransferSession(_script_remote(tmp_path, "slow", slow), timeout=30)
    raised: list[BaseException] = []

    def opening() -> None:
        try:
            session.open()
        except BaseException as exc:
            raised.append(exc)

    thread = threading.Thread(target=opening)
    thread.start()
    deadline = time.monotonic() + 10
    while session._process is None and time.monotonic() < deadline:
        time.sleep(0.02)
    session.close()
    thread.join(timeout=30)
    (error,) = raised
    assert isinstance(error, RuntimeError) and not isinstance(error, SessionUnsupported), error
    assert "closed" in str(error)


def test_the_opening_echo_has_an_absolute_deadline(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = """
    def busy_echo(request):
        for _ in range(150):
            time.sleep(0.1)
            request.advance()
        return session.OPS["echo"](request)

    OPS["echo"] = busy_echo
    """
    monkeypatch.setattr(adapters, "_ECHO_BOUND", 1.0)
    started = time.monotonic()
    with pytest.raises(SessionUnsupported, match="did not answer in time"):
        TransferSession(_far_side(tmp_path, body, tick=0.1), timeout=60).open()
    assert time.monotonic() - started < 12


_HELLO = {"format": TRANSFER_SESSION_FORMAT, "format_version": 1, "ops": ["bye", "echo"]}


@pytest.mark.parametrize(
    ("branch", "printed"),
    [
        ("refused", {"format": "httk-computer-result", "format_version": 2, "operation": "session", "ok": False}),
        ("foreign", {"format": "httk-workflow-session", "format_version": 1}),
        ("version", {**_HELLO, "format_version": 2}),
        ("malformed ops", {**_HELLO, "ops": "echo bye"}),
        ("missing ops", {**_HELLO, "ops": ["bye"]}),
        ("echo garbage", _HELLO),
    ],
)
def test_a_known_ssh_255_is_a_connection_failure_on_every_branch(
    tmp_path: Path, remote: Remote, branch: str, printed: dict[str, Any]
) -> None:
    source = f"""
    import os, sys
    print({json.dumps(printed)!r}, flush=True)
    if {branch == "echo garbage"!r}:
        sys.stdin.buffer.read(1)
        sys.stdout.buffer.write(b"Z" * 20)
        sys.stdout.buffer.flush()
    os._exit(255)
    """
    bundle = _script_remote(tmp_path, "ssh255", source, template="ssh")
    with pytest.raises(RuntimeError, match="ssh connection failed") as raised:
        TransferSession(bundle, timeout=60).open()
    assert not isinstance(raised.value, SessionUnsupported)


def test_the_bound_is_at_least_three_progress_intervals(tmp_path: Path) -> None:
    session = TransferSession(tmp_path, timeout=0.01)
    session._limit = 0.01
    assert session._bound() == 3 * _frames.TICK == 30.0
    session._limit = 100.0
    assert session._bound() == 100.0


# Requests over a working session.


def test_an_unknown_op_changes_nothing_and_its_upload_is_drained(tmp_path: Path, remote: Remote) -> None:
    bundle = fake_remote(_project(tmp_path), template="local")
    with _opened(bundle) as session:
        assert session.request("nope", {"x": 1}) == {"ok": False, "error": "unknown op 'nope'"}
        uploaded = session.request("nope", upload=_writer(bytes(3 * MiB), b"tail"))
        assert uploaded == {"ok": False, "error": "unknown op 'nope'"}
        payload = bytes(range(256)) * 100
        assert _echo(session, payload)[1] == payload


def test_serve_discards_the_unread_upload_of_a_refusing_op(tmp_path: Path, remote: Remote) -> None:
    with _opened(_far_side(tmp_path)) as session:
        assert session.request("refuse", upload=_writer(bytes(3 * MiB))) == {"ok": False, "error": "refused"}
        assert _echo(session, b"after")[1] == b"after"


def test_an_op_answering_ok_over_an_unread_upload_is_fatal(tmp_path: Path, remote: Remote) -> None:
    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(SessionFailed, match="did not read to a successful terminal") as raised:
            session.request("unread", upload=_writer(b"data"))
        assert raised.value.response is not None and raised.value.response["fatal"] is True


def test_an_upload_abort_reaches_the_far_side_and_its_response_is_kept(tmp_path: Path, remote: Remote) -> None:
    def failing(stream: BinaryIO) -> None:
        stream.write(b"start")
        raise OSError("source vanished")

    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(RequestAborted, match="source vanished") as raised:
            session.request("keep", upload=failing)
        terminal = {"ok": False, "abort": "OSError: source vanished"}
        assert raised.value.response == {"ok": False, "error": "aborted", "terminal": terminal}
        assert isinstance(raised.value.__cause__, OSError)
        assert session.request("keep", upload=_writer(b"whole")) == {"ok": True, "terminal": {"ok": True}}


def test_a_failing_download_consumer_keeps_the_response(tmp_path: Path, remote: Remote) -> None:
    def picky(stream: BinaryIO) -> None:
        stream.read(10)
        raise ValueError("not what I wanted")

    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(RequestAborted, match="not what I wanted") as raised:
            session.request("big", {"mib": 3}, download=picky)
        assert raised.value.response == {"ok": True} and isinstance(raised.value.__cause__, ValueError)
        assert _echo(session, b"after")[1] == b"after"


def test_an_aborted_download_raises_request_aborted(tmp_path: Path, remote: Remote) -> None:
    read, received = _collector()
    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(RequestAborted, match="the source went away") as raised:
            session.request("half", download=read)
        assert isinstance(raised.value.__cause__, StreamAborted) and raised.value.response == {
            "ok": False,
            "error": "half",
        }
        assert received == b"half"
        assert _echo(session, b"after")[1] == b"after"


def test_an_ok_answer_must_carry_its_download(tmp_path: Path, remote: Remote) -> None:
    read, _ = _collector()
    with _opened(_far_side(tmp_path)) as session:
        assert session.request("nothing", {"ok": False}, download=read) == {
            "ok": False,
            "error": "nothing",
        }  # a refusal needs none
        with pytest.raises(SessionFailed, match="answered ok without its download"):
            session.request("nothing", download=read)


@pytest.mark.parametrize(
    ("how", "message"),
    [
        ("foreign id", "of request 9 during request 2"),
        ("response inside download", "R frame inside a download"),
        ("data after terminal", "unexpected D frame"),
        ("second terminal", "unexpected E frame"),
        ("malformed progress", "malformed progress"),
    ],
)
def test_a_grammar_violation_fails_the_session(tmp_path: Path, remote: Remote, how: str, message: str) -> None:
    read, _ = _collector()
    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(SessionFailed, match=message):
            session.request("violate", {"how": how}, download=read)
        with pytest.raises(SessionFailed):
            session.request("nothing")


def test_a_response_during_the_upload_breaks_the_protocol(tmp_path: Path, remote: Remote) -> None:
    with (
        _opened(_far_side(tmp_path)) as session,
        pytest.raises(SessionFailed, match="R frame before the upload ended"),
    ):
        session.request("answer_early", upload=_writer(*[bytes(MiB)] * 8))


def test_a_stale_frame_fails_the_session_before_the_next_request_is_sent(tmp_path: Path, remote: Remote) -> None:
    marker = tmp_path / "marker"
    with _opened(_far_side(tmp_path)) as session:
        assert session.request("stale") == {"ok": True}
        deadline = time.monotonic() + 15
        while not session._events._items and time.monotonic() < deadline:
            time.sleep(0.02)
        with pytest.raises(SessionFailed, match="unsolicited D frame of request 2"):
            session.request("mark", {"marker": str(marker)})
    assert not marker.exists()  # the next request was never sent


def test_a_nested_request_or_an_unsendable_header_leaves_the_session_usable(tmp_path: Path, remote: Remote) -> None:
    with _opened(_far_side(tmp_path)) as session:

        def nested(stream: BinaryIO) -> None:
            session.request("nothing")

        with pytest.raises(RequestAborted) as raised:
            session.request("big", {"mib": 1}, download=nested)
        assert isinstance(raised.value.__cause__, RuntimeError) and "in flight" in str(raised.value.__cause__)
        with pytest.raises(TypeError):
            session.request("nothing", {"x": object()})
        assert _echo(session, b"after")[1] == b"after"


def test_a_fatal_op_fails_the_session_for_good(tmp_path: Path, remote: Remote) -> None:
    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(SessionFailed, match="lost owner") as raised:
            session.request("boom")
        assert raised.value.response is not None and raised.value.response["fatal"] is True
        assert session._process is None
        with pytest.raises(SessionFailed):
            session.request("nothing")
        with pytest.raises(SessionFailed):
            session.open()
    assert _starts(tmp_path) == ["start"]


def test_a_fatal_op_takes_its_whole_process_group_along(tmp_path: Path, remote: Remote) -> None:
    pidfile = tmp_path / "grandchild"
    with _opened(_far_side(tmp_path)) as session:
        with pytest.raises(SessionFailed, match="lost owner"):
            session.request("boom", {"pidfile": str(pidfile)})
        assert _wait_dead(int(pidfile.read_text()))
        assert session._reader is not None and not session._reader.is_alive()


def test_a_vanished_leader_still_has_its_group_killed(tmp_path: Path, remote: Remote, quick: None) -> None:
    # The leader exits without answering; its child keeps stdout open, so only the bound ends the request.
    pidfile = tmp_path / "grandchild"
    with _opened(_far_side(tmp_path), bound=1.0) as session:
        with pytest.raises(SessionFailed, match="sent nothing"):
            session.request("vanish", {"pidfile": str(pidfile)})
        assert session._status == 0  # the leader had exited on its own
        assert _wait_dead(int(pidfile.read_text()))
        assert session._reader is not None and not session._reader.is_alive()


# Bounds.


def test_a_slow_op_reporting_progress_outlives_the_idle_bound(tmp_path: Path, remote: Remote, quick: None) -> None:
    with _opened(_far_side(tmp_path, tick=0.1), bound=1.0) as session:
        started = time.monotonic()
        assert session.request("slow", {"steps": 30}) == {"ok": True, "steps": 30}
        assert time.monotonic() - started > 2.5


def test_a_silent_far_side_fails_at_the_idle_bound_with_its_group(tmp_path: Path, remote: Remote, quick: None) -> None:
    pidfile = tmp_path / "grandchild"
    with _opened(_far_side(tmp_path, tick=1000.0), bound=0.5) as session:
        started = time.monotonic()
        with pytest.raises(SessionFailed, match="sent nothing and took no data for 0.5 seconds"):
            session.request("silent", {"pidfile": str(pidfile)})
        assert time.monotonic() - started < 10
        assert _wait_dead(int(pidfile.read_text()))
        with pytest.raises(SessionFailed, match="sent nothing"):
            session.request("nothing")
    assert _starts(tmp_path) == ["start"]


def test_a_stalled_count_fails_outside_sync_and_build_only(tmp_path: Path, remote: Remote, quick: None) -> None:
    with _opened(_far_side(tmp_path, tick=0.1), bound=0.8) as session:
        for phase in ("sync", "build"):
            assert session.request("stand", {"phase": phase, "seconds": 2.5})["ok"] is True
        started = time.monotonic()
        with pytest.raises(SessionFailed, match="extract progress stood at 3"):
            session.request("stand", {"phase": "extract", "seconds": 60})
        assert time.monotonic() - started < 10


def test_arriving_download_bytes_count_as_movement(tmp_path: Path, remote: Remote, quick: None) -> None:
    read, received = _collector()
    with _opened(_far_side(tmp_path, tick=0.1), bound=1.0) as session:
        assert session.request("trickle", {"pieces": 12, "gap": 0.25}, download=read)["ok"] is True
    assert received == b"x" * 1024 * 12


def test_an_upload_the_far_side_stops_reading_stalls(tmp_path: Path, remote: Remote, quick: None) -> None:
    with _opened(_far_side(tmp_path, tick=0.1), bound=1.0) as session:
        started = time.monotonic()
        with pytest.raises(SessionFailed, match="extract progress stood at 3"):
            session.request("stand", {"phase": "extract", "seconds": 60}, upload=_writer(*[bytes(MiB)] * 8))
        assert time.monotonic() - started < 10


def test_a_far_side_that_stops_reading_and_reporting_fails_a_blocked_write(
    tmp_path: Path, remote: Remote, quick: None
) -> None:
    with _opened(_far_side(tmp_path, tick=1000.0), bound=0.5) as session:
        started = time.monotonic()
        with pytest.raises(SessionFailed, match="sent nothing and took no data for 0.5 seconds"):
            session.request("silent", upload=_writer(*[bytes(MiB)] * 8))
        assert time.monotonic() - started < 10


def test_a_late_frame_fails_even_when_it_is_already_queued(tmp_path: Path, remote: Remote, quick: None) -> None:
    def dawdle(stream: BinaryIO) -> None:
        assert stream.read(1) == b"a"
        time.sleep(3.0)  # meanwhile b arrives, 2 s after a: later than the bound allows
        stream.read(1)

    with (
        _opened(_far_side(tmp_path, tick=1000.0), bound=0.8) as session,
        pytest.raises(SessionFailed, match="sent nothing"),
    ):
        session.request("pause", {"gap": 2.0}, download=dawdle)


def test_a_refusal_discarding_a_slow_upload_keeps_the_session(tmp_path: Path, remote: Remote, quick: None) -> None:
    def slowly(stream: BinaryIO) -> None:
        for _ in range(16):
            stream.write(bytes(MiB))
            time.sleep(0.1)

    with _opened(_far_side(tmp_path, tick=0.05), bound=1.0) as session:
        assert session.request("refuse", upload=slowly) == {"ok": False, "error": "refused"}
        assert session.request("nope", upload=slowly)["error"] == "unknown op 'nope'"
        assert _echo(session, b"after")[1] == b"after"


def test_accepted_upload_bytes_count_as_activity(tmp_path: Path, remote: Remote, quick: None) -> None:
    # No progress at all: only the bytes the far side keeps taking show that it is alive, up to its terminal.
    def slowly(stream: BinaryIO) -> None:
        for _ in range(16):
            stream.write(bytes(MiB))
            time.sleep(0.1)

    with _opened(_far_side(tmp_path, tick=1000.0), bound=1.0) as session:
        assert session.request("refuse", upload=slowly) == {"ok": False, "error": "refused"}


def test_a_slow_consumer_is_not_the_far_side_idling(tmp_path: Path, remote: Remote, quick: None) -> None:
    # The reader waits on the full queue longer than the bound: this side's own back-pressure, not the far side's.
    received = [0]

    def slow(stream: BinaryIO) -> None:
        while chunk := stream.read(MiB):
            received[0] += len(chunk)
            time.sleep(0.6)

    with _opened(_far_side(tmp_path, tick=1000.0), bound=0.5) as session:
        assert session.request("big", {"mib": 10}, download=slow)["ok"] is True
    assert received[0] == 10 * MiB


def test_close_bounds_its_bye(tmp_path: Path, remote: Remote, quick: None) -> None:
    body = """
    def bye(request):
        for _ in range(600):
            time.sleep(0.1)
            request.advance()
        return {}

    OPS["bye"] = bye
    """
    session = _opened(_far_side(tmp_path, body, tick=0.1), bound=30.0)
    started = time.monotonic()
    session.close()
    assert time.monotonic() - started < 15
    assert session._process is None


def test_empty_frames_count_against_the_queue_bound() -> None:
    hello = json.dumps({"format": TRANSFER_SESSION_FORMAT, "format_version": 1, "ops": []}).encode() + b"\n"
    stream = io.BytesIO(hello + encode_frame(DATA, 1, b"") * 10_000)
    events = adapters._ByteQueue(100 * adapters._FRAME_OVERHEAD)
    reader = threading.Thread(target=adapters._read_transfer_session, args=(stream, events), daemon=True)
    reader.start()
    reader.join(timeout=2)
    assert reader.is_alive()  # blocked on the full queue, not done reading
    assert events.high_water <= 100 * adapters._FRAME_OVERHEAD
    events.close()
    reader.join(timeout=10)
    assert not reader.is_alive()


@pytest.mark.parametrize("counting", [False, True])
def test_a_source_pause_is_not_charged_to_the_far_side(
    tmp_path: Path, remote: Remote, quick: None, counting: bool
) -> None:
    # Pauses of 2 s (bound 0.8 s) before the first write, between writes and before the terminal, while the far
    # side ticks a count that stands still (fixed) or moves only with the bytes it reads (counting).
    def pausing(stream: BinaryIO) -> None:
        time.sleep(2.0)
        stream.write(bytes(MiB))
        time.sleep(2.0)
        stream.write(bytes(MiB))
        time.sleep(2.0)

    with _opened(_far_side(tmp_path, tick=0.1), bound=0.8) as session:
        assert session.request("take", {"counting": counting}, upload=pausing) == {"ok": True, "bytes": 2 * MiB}


def test_a_steady_upload_is_movement_under_a_stuck_count(tmp_path: Path, remote: Remote, quick: None) -> None:
    # One 16 MiB write the far side reads slowly (2.4 s, bound 0.8 s) under a stuck count: the bytes it accepts move.
    with _opened(_far_side(tmp_path, tick=0.1), bound=0.8) as session:
        assert session.request("sip", upload=_writer(bytes(16 * MiB))) == {"ok": True, "bytes": 16 * MiB}


def test_empty_download_frames_do_not_move_a_stuck_count(tmp_path: Path, remote: Remote, quick: None) -> None:
    read, _ = _collector()
    with _opened(_far_side(tmp_path, tick=0.1), bound=1.0) as session:
        started = time.monotonic()
        with pytest.raises(SessionFailed, match="extract progress stood at 3"):
            session.request("empties", download=read)
        assert time.monotonic() - started < 6


def test_a_frame_cut_short_is_the_end_of_the_session(tmp_path: Path, remote: Remote) -> None:
    read, _ = _collector()
    with _opened(_far_side(tmp_path)) as session, pytest.raises(SessionFailed, match="transfer session ended"):
        session.request("partial", download=read)


def test_a_consumer_pause_past_the_bound_is_back_pressure_not_a_stall(
    tmp_path: Path, remote: Remote, quick: None
) -> None:
    # 80 MiB to a consumer that pauses 2 s (bound 0.5 s) with a stuck progress count, then drains: the full queue
    # held the far side back, which neither the idle nor the stall clock charges to it.
    received = [0]

    def pausing(stream: BinaryIO) -> None:
        received[0] += len(stream.read(MiB))
        time.sleep(2.0)
        while chunk := stream.read(MiB):
            received[0] += len(chunk)

    with _opened(_far_side(tmp_path, tick=0.1), bound=0.5) as session:
        assert session.request("big", {"mib": 80, "stuck": True}, download=pausing)["ok"] is True
        assert received[0] == 80 * MiB
        assert 32 * MiB < session._events.high_water <= adapters._TRANSFER_QUEUE_BYTES == (64 << 20) + 64


def test_a_blocked_reader_is_never_charged_to_the_far_side() -> None:
    # Deterministic: the reader is blocked on the full queue; the consumer judges before the reader can run again.
    events = adapters._ByteQueue(1000)
    events.put(adapters._Event("frame", None, 0.0, 0.0), 1000)
    producer = threading.Thread(target=events.put, args=(adapters._Event("frame", None, 0.0, 0.0), 500), daemon=True)
    producer.start()
    deadline = time.monotonic() + 10
    while events._blocked_since is None and time.monotonic() < deadline:
        time.sleep(0.01)
    watch = adapters._Watch(0.5)
    time.sleep(0.7)  # past the bound, all of it with the reader held back
    assert watch.remaining(events.held_back())[0] > 0.4
    events.take()
    assert watch.remaining(events.held_back())[0] > 0.4
    producer.join(timeout=10)
    with pytest.raises(ValueError, match="exceeds the queue bound"):
        events.put(adapters._Event("frame", None, 0.0, 0.0), 1001)
    events.close()


# The in-process serve loop, on pipes.


class _Peer:
    """The client end of an in-process :func:`serve` running on two pipes."""

    def __init__(self, ops: dict[str, Op]) -> None:
        requests_read, requests_write = os.pipe()
        responses_read, responses_write = os.pipe()
        self.requests: IO[bytes] = os.fdopen(requests_write, "wb")
        self.responses: IO[bytes] = os.fdopen(responses_read, "rb")
        self.status: list[int] = []
        far_in, far_out = os.fdopen(requests_read, "rb"), os.fdopen(responses_write, "wb")

        def run() -> None:
            with far_in, far_out:
                self.status.append(serve(far_in, far_out, ops=ops))

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        self.hello = json.loads(self.responses.readline())
        self.frames = FrameReader(self.responses)
        self.writer = FrameWriter(self.requests.write, self.requests.flush)

    def end(self) -> int:
        self.requests.close()
        return self.join()

    def join(self) -> int:
        self.thread.join(timeout=30)
        assert not self.thread.is_alive()
        (status,) = self.status
        return status

    def until_response(self) -> list[Frame]:
        seen = []
        while (frame := self.frames.read_frame()) is not None:
            seen.append(frame)
            if frame.kind == RESPONSE:
                return seen
        raise AssertionError(f"no response after {seen}")

    def answer(self) -> dict[str, Any]:
        document = self.until_response()[-1].document
        assert document is not None
        return document


def test_serve_says_hello_and_ends_cleanly_between_requests() -> None:
    peer = _Peer(dict(OPS))
    ops = sorted(
        ["adopt", "adopt_staged", "bye", "echo", "hold_page", "holds_page", "install", "release", "select", "send"]
        + ["stage", "workflows"]
    )
    assert peer.hello == {"format": TRANSFER_SESSION_FORMAT, "format_version": 1, "ops": ops}
    peer.writer.send_json(REQUEST, 1, {"op": "nope"})
    assert peer.answer() == {"ok": False, "error": "unknown op 'nope'"}
    assert peer.end() == 0


def test_serve_honours_bye_and_refuses_one_with_an_upload() -> None:
    peer = _Peer(dict(OPS))
    peer.writer.send_json(REQUEST, 1, {"op": "bye", "upload": True})
    peer.writer.send(DATA, 1, b"x")
    peer.writer.send_json(END, 1, {"ok": True})
    assert peer.answer() == {"ok": False, "error": "bye takes no upload"}
    peer.writer.send_json(REQUEST, 2, {"op": "bye"})
    peer.writer.send_json(REQUEST, 3, {"op": "nope"})
    assert peer.answer() == {"ok": True}
    assert peer.join() == 0
    assert peer.frames.read_frame() is None  # request 3 was never answered


@pytest.mark.parametrize("error", [RuntimeError("half adopted"), SystemExit(0)])
def test_serve_answers_a_fatal_op_and_exits_nonzero(error: BaseException) -> None:
    def boom(request: Request) -> dict[str, Any]:
        raise error

    peer = _Peer({"boom": boom, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "boom"})
    response = peer.answer()
    assert response["fatal"] is True and response["ok"] is False and type(error).__name__ in response["error"]
    assert peer.join() == 1
    assert peer.frames.read_frame() is None  # nothing follows the fatal answer
    peer.requests.close()


def test_serve_treats_an_unsendable_response_as_fatal() -> None:
    peer = _Peer({"odd": lambda request: {"value": object()}, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "odd"})
    assert peer.answer()["fatal"] is True
    assert peer.join() == 1


def test_serve_fail_stops_when_an_op_swallows_a_broken_frame() -> None:
    def swallow(request: Request) -> dict[str, Any]:
        assert request.upload is not None
        with contextlib.suppress(Exception):
            request.upload.finish()
        raise _transfer_session.Refused("swallowed")

    peer = _Peer({"swallow": swallow, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "swallow", "upload": True})
    peer.writer.send_json(PROGRESS, 1, {"phase": "x", "count": 1})  # a client never sends progress
    assert peer.answer()["fatal"] is True
    assert peer.join() == 1


_NESTED = {"deep": json.loads("[" * 50 + "]" * 50)}
_TOO_DEEP = b'{"deep":' + b"[" * 200_000 + b"]" * 200_000 + b"}"


def test_a_deeply_nested_terminal_swallowed_by_an_op_is_fatal() -> None:
    def swallow(request: Request) -> dict[str, Any]:
        assert request.upload is not None
        with contextlib.suppress(Exception):
            request.upload.finish()
        raise _transfer_session.Refused("swallowed")

    peer = _Peer({"swallow": swallow, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "swallow", "upload": True})
    peer.writer.send(END, 1, _TOO_DEEP)
    response = peer.answer()
    assert response["fatal"] is True and "RecursionError" in response["error"]
    assert peer.join() == 1


@pytest.mark.parametrize("result", [{"ok": True}, {"ok": False}, {"fatal": False}, {"x": 1, "ok": True}])
def test_an_op_setting_a_reserved_member_is_fatal(result: dict[str, Any]) -> None:
    peer = _Peer({"reserved": lambda request: result, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "reserved"})
    response = peer.answer()
    assert response["fatal"] is True and "reserved response members" in response["error"]
    assert peer.join() == 1


def test_a_refusal_carries_its_members_and_keeps_the_session() -> None:
    def refuse(request: Request) -> dict[str, Any]:
        raise _transfer_session.Refused("not today", detail=_NESTED)

    def refuse_reserved(request: Request) -> dict[str, Any]:
        raise _transfer_session.Refused("no", ok=True)

    peer = _Peer({"refuse": refuse, "refuse_reserved": refuse_reserved, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "refuse"})
    assert peer.answer() == {"ok": False, "error": "not today", "detail": _NESTED}
    peer.writer.send_json(REQUEST, 2, {"op": "refuse_reserved"})
    assert peer.answer()["fatal"] is True
    assert peer.join() == 1


# The far-side command itself, in a process of its own.

_COMMAND = """
import sys, threading, time
from httk.workflow.workflow_cli import _transfer_session as session

def linger(request):
    threading.Thread(target=time.sleep, args=(600,)).start()  # a thread that would keep the process alive
    return {}

def interrupt(request):
    raise KeyboardInterrupt

session.OPS.update(linger=linger, interrupt=interrupt)
sys.exit(session.handle_transfer_session(None, None))
"""


@pytest.mark.parametrize(
    ("ending", "frame"),
    [
        ("a deeply nested request", b"Q" + (2).to_bytes(4, "big") + len(_TOO_DEEP).to_bytes(4, "big") + _TOO_DEEP),
        ("an exception escaping serve", encode_json(REQUEST, 2, {"op": "interrupt"})),
    ],
    ids=["nested request", "escaping exception"],
)
def test_a_failed_far_side_exits_at_once(tmp_path: Path, ending: str, frame: bytes) -> None:
    import httk.core.cli

    import httk.workflow

    roots = [str(Path(module.__file__ or "").resolve().parents[2]) for module in (httk.workflow, httk.core.cli)]
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join([*roots, os.environ.get("PYTHONPATH", "")])}
    process = subprocess.Popen(
        [sys.executable, "-c", _COMMAND], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=environment
    )
    assert process.stdin is not None and process.stdout is not None
    try:
        assert json.loads(process.stdout.readline())["format"] == TRANSFER_SESSION_FORMAT
        process.stdin.write(encode_json(REQUEST, 1, {"op": "linger"}))
        process.stdin.flush()
        frames = FrameReader(process.stdout)
        answer = frames.read_frame()
        assert answer is not None and answer.document == {"ok": True}
        process.stdin.write(frame)
        process.stdin.flush()
        assert process.wait(timeout=30) == 1, ending
    finally:
        process.kill()
        process.wait()


def test_end_of_input_inside_a_request_ends_serve() -> None:
    # An op that tolerates the cut (echo refuses its upload) still ends the session after its answer.
    peer = _Peer(dict(OPS))
    peer.writer.send_json(REQUEST, 1, {"op": "echo", "upload": True})
    peer.writer.send(DATA, 1, b"abc")
    assert peer.end() == 1
    response = peer.answer()
    assert response["ok"] is False and "fatal" not in response and "input ended" in response["error"]

    # An op that reads into the cut unguarded is fatal.
    def strict(request: Request) -> dict[str, Any]:
        assert request.upload is not None
        request.upload.finish()
        return {}

    peer = _Peer({"strict": strict, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "strict", "upload": True})
    peer.writer.send(DATA, 1, b"abc")
    assert peer.end() == 1
    response = peer.answer()
    assert response["fatal"] is True and "input ended" in response["error"]


@pytest.mark.parametrize(
    "wire",
    [
        encode_frame(DATA, 1, b"stray"),
        b"Q\x00\x00\x00\x01\x00\x00\x00\x02[]",
    ],
)
def test_serve_ends_at_a_protocol_error(wire: bytes) -> None:
    peer = _Peer(dict(OPS))
    peer.requests.write(wire)
    assert peer.end() == 1


def test_serve_refuses_a_request_id_that_does_not_increase() -> None:
    peer = _Peer(dict(OPS))
    peer.writer.send_json(REQUEST, 5, {"op": "nope"})
    assert peer.answer()
    peer.writer.send_json(REQUEST, 5, {"op": "nope"})
    assert peer.end() == 1


def test_a_download_before_the_upload_terminal_is_fatal() -> None:
    def eager(request: Request) -> dict[str, Any]:
        request.download()
        return {}

    peer = _Peer({"eager": eager, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "eager", "upload": True})
    peer.writer.send(DATA, 1, b"ignored")
    peer.writer.send_json(END, 1, {"ok": True})
    response = peer.answer()
    assert response["fatal"] is True and "after the upload" in response["error"]
    assert peer.join() == 1


def test_serve_aborts_a_download_its_op_did_not_close() -> None:
    def sloppy(request: Request) -> dict[str, Any]:
        stream = request.download()
        stream.write(b"partial")
        stream.flush()
        return {}

    peer = _Peer({"sloppy": sloppy, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "sloppy"})
    seen = [(frame.kind, frame.data if frame.kind == DATA else frame.document) for frame in peer.until_response()]
    assert seen == [
        (DATA, b"partial"),
        (END, {"ok": False, "abort": "the op returned without completing its download"}),
        (RESPONSE, {"ok": True}),
    ]
    assert peer.end() == 0


def test_the_ticker_covers_a_discard_until_the_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_frames, "TICK", 0.02)
    peer = _Peer(dict(OPS))
    peer.writer.send_json(REQUEST, 1, {"op": "nope", "upload": True})
    for _ in range(3):
        time.sleep(0.2)
        peer.writer.send(DATA, 1, b"x" * 100)
    peer.writer.send_json(END, 1, {"ok": True})
    seen = peer.until_response()
    progress = [frame.document for frame in seen if frame.kind == PROGRESS]
    assert progress and all(document is not None and document["phase"] == "discard" for document in progress)
    assert {document["count"] for document in progress if document is not None} >= {100, 200}
    assert seen[-1].document == {"ok": False, "error": "unknown op 'nope'"}
    assert peer.end() == 0


def test_progress_frames_stop_before_the_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_frames, "TICK", 0.02)

    def busy(request: Request) -> dict[str, Any]:
        request.progress("adopt", 0)
        for _ in range(10):
            time.sleep(0.02)
            request.advance()
        return {}

    peer = _Peer({"busy": busy, **OPS})
    peer.writer.send_json(REQUEST, 1, {"op": "busy"})
    peer.writer.send_json(REQUEST, 2, {"op": "nope"})
    first, second = peer.until_response(), peer.until_response()
    assert any(frame.kind == PROGRESS for frame in first)
    assert all(frame.id == 1 for frame in first) and all(frame.id == 2 for frame in second)
    assert peer.end() == 0


def test_a_stopped_ticker_never_sends_even_when_it_already_woke(monkeypatch: pytest.MonkeyPatch) -> None:
    # Deterministic: the ticker wakes and waits for the writer lock the test holds while it stops it and answers.
    monkeypatch.setattr(_frames, "TICK", 0.01)
    sink = io.BytesIO()
    frames = FrameWriter(sink.write)
    request = Request(1, {"op": "busy"}, None, frames)
    ticker = _transfer_session._Ticker(request, frames)
    with frames.lock:
        ticker.__enter__()
        time.sleep(0.3)  # the ticker woke long ago and now waits for the lock
        ticker.stop()
        frames.send_json(RESPONSE, 1, {"ok": True})
    ticker._thread.join(timeout=10)
    reader = FrameReader(io.BytesIO(sink.getvalue()))
    kinds = []
    while (frame := reader.read_frame()) is not None:
        kinds.append(frame.kind)
    assert kinds == [RESPONSE]
