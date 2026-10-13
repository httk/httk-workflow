"""Test the daemon execution-scope packet and its descriptor lifetime."""

import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from httk.workflow import _daemon_scope
from httk.workflow._death import ProcessIdentity

_HOST = {"hostname": "compute-4", "boot_id": "11111111-2222-3333-4444-555555555555"}


def _packet(value: object) -> int:
    descriptor = os.memfd_create("daemon-scope-test", os.MFD_CLOEXEC)
    os.write(descriptor, json.dumps(value).encode())
    return descriptor


def test_execution_scope_uses_bwrap_child_and_namespace_pid_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_daemon_scope, "_pid_one_start_ticks", lambda: 9182)
    scope_fd = _packet({"child-pid": 27645, "other": "ignored"})
    identity = _daemon_scope.read_execution_scope(scope_fd, json.dumps(_HOST))
    assert identity == ProcessIdentity("compute-4", _HOST["boot_id"], 27645, 9182)
    with pytest.raises(OSError):
        os.fstat(scope_fd)


@pytest.mark.parametrize(
    ("scope", "host"),
    [
        ({}, _HOST),
        ({"child-pid": "2"}, _HOST),
        ({"child-pid": 1}, _HOST),
        ({"child-pid": 2}, {**_HOST, "extra": 1}),
        ({"child-pid": 2}, {"hostname": _HOST["hostname"]}),
        ({"child-pid": 2}, []),
        ({"child-pid": 2}, {**_HOST, "hostname": "bad/name"}),
        ({"child-pid": 2}, {**_HOST, "boot_id": ""}),
    ],
)
def test_execution_scope_refuses_incomplete_or_malformed_identity(scope: object, host: object) -> None:
    scope_fd = _packet(scope)
    with pytest.raises(ValueError):
        _daemon_scope.read_execution_scope(scope_fd, json.dumps(host))
    with pytest.raises(OSError):
        os.fstat(scope_fd)


def test_execution_scope_requires_valid_json_and_closes_descriptor() -> None:
    scope_fd = os.memfd_create("bad-scope", os.MFD_CLOEXEC)
    os.write(scope_fd, b"{} trailing")
    with pytest.raises(ValueError):
        _daemon_scope.read_execution_scope(scope_fd, json.dumps(_HOST))
    with pytest.raises(OSError):
        os.fstat(scope_fd)


def test_service_scope_reader_survives_bwrap_info_writer_close() -> None:
    writer_fd = os.memfd_create("bwrap-info-writer", os.MFD_CLOEXEC)
    reader_fd = os.open(f"/proc/self/fd/{writer_fd}", os.O_RDONLY | os.O_CLOEXEC)
    fake_bwrap = Path(__file__).with_name("fake_bwrap.py")
    child = """
import json, os, sys
reader, writer = map(int, sys.argv[1:])
packet = json.loads(os.pread(reader, 4096, 0))
assert packet['child-pid'] == os.getpid()
assert os.readlink(f'/proc/self/fd/{reader}').startswith('/memfd:')
assert not os.path.exists(f'/proc/self/fd/{writer}')
try:
    os.write(reader, b'x')
except OSError:
    pass
else:
    raise AssertionError('service scope descriptor is writable')
"""
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(fake_bwrap),
                "--info-fd",
                str(writer_fd),
                "--",
                sys.executable,
                "-c",
                child,
                str(reader_fd),
                str(writer_fd),
            ],
            pass_fds=(writer_fd, reader_fd),
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    finally:
        os.close(writer_fd)
        os.close(reader_fd)
    assert result.returncode == 0, result.stderr


def test_execution_scope_reads_pid_one_ticks_from_proc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _daemon_scope.Path,
        "open",
        lambda *_args, **_kwargs: io.BytesIO(("1 (daemon (child)) S " + " ".join(["0"] * 18 + ["67890"])).encode()),
    )
    assert _daemon_scope._pid_one_start_ticks() == 67890


def test_execution_scope_refuses_non_pid_one_stat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_daemon_scope.Path, "open", lambda *_args, **_kwargs: io.BytesIO(b"2 (daemon) S 0 0\n"))
    with pytest.raises(ValueError):
        _daemon_scope._pid_one_start_ticks()
