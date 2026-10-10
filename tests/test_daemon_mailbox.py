"""Focused tests for the descriptor-anchored daemon mailbox primitive."""

import errno
import os
import re
import socket
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from httk.workflow import _daemon_mailbox as mailbox_module
from httk.workflow import _fs
from httk.workflow._daemon_mailbox import MailboxDirectory, read_regular

_NAME = re.compile(r"[0-9a-f]{32}\.json\Z")


def _mailbox(tmp_path: Path) -> Path:
    path = tmp_path / "mailbox"
    path.mkdir()
    return path


def test_publish_read_mode_and_temporary_cleanup(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)

    with MailboxDirectory(path) as mailbox:
        name = mailbox.publish(b"hello")
        assert _NAME.fullmatch(name)
        assert mailbox.read(name) == b"hello"
        assert mailbox.names() == (name,)
        with pytest.raises(ValueError, match="bytes"):
            mailbox.publish(bytearray(b"hello"))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="exceeds"):
            mailbox.publish(b"x" * (mailbox_module.MAX_DOCUMENT_BYTES + 1))

    assert stat.S_IMODE((path / name).stat().st_mode) == 0o600
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_replace_replaces_symlink_entry_without_touching_target(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    name = "9" * 32 + ".json"
    target = tmp_path / "outside.json"
    target.write_bytes(b"outside")
    (path / name).symlink_to(target)

    with MailboxDirectory(path) as mailbox:
        mailbox.replace(name, b"replacement")
        assert mailbox.read(name) == b"replacement"

    assert not (path / name).is_symlink()
    assert target.read_bytes() == b"outside"


def test_remove_validates_flat_names_and_unlinks_symlink_entry(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    name = "a" * 32 + ".json"
    target = tmp_path / "outside.json"
    target.write_bytes(b"outside")
    (path / name).symlink_to(target)

    with MailboxDirectory(path) as mailbox:
        with pytest.raises(ValueError, match="name"):
            mailbox.remove("../outside.json")
        mailbox.remove(name)
        mailbox.remove(name)  # an absent publication counts as removed

    assert not (path / name).exists()
    assert target.read_bytes() == b"outside"


def test_failed_replace_before_rename_preserves_old_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _mailbox(tmp_path)
    name = "b" * 32 + ".json"
    old = path / name
    old.write_bytes(b"old")

    def fail_rename(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected rename failure")

    monkeypatch.setattr(mailbox_module.os, "replace", fail_rename)
    with MailboxDirectory(path) as mailbox, pytest.raises(OSError):
        mailbox.replace(name, b"new")
    assert old.read_bytes() == b"old"
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_replace_enforces_name_and_size_bounds(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    with MailboxDirectory(path) as mailbox:
        with pytest.raises(ValueError, match="name"):
            mailbox.replace("../escape", b"data")
        with pytest.raises(ValueError, match="bytes"):
            mailbox.replace("c" * 32 + ".json", bytearray(b"data"))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="exceeds"):
            mailbox.replace("c" * 32 + ".json", b"x" * (mailbox_module.MAX_DOCUMENT_BYTES + 1))


def test_path_is_absolute_and_symlink_components_are_refused(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(path, target_is_directory=True)

    with pytest.raises(ValueError, match="absolute"):
        MailboxDirectory("relative/mailbox")
    with pytest.raises(OSError):
        MailboxDirectory(link)
    with pytest.raises(OSError):
        MailboxDirectory(link / "child")


def test_read_validates_flat_names_and_entry_types() -> None:
    # Parallel pytest temporary paths can exceed the Unix socket address limit.
    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        path = _mailbox(Path(directory))
        name = "0" * 32 + ".json"
        (path / name).mkdir()
        socket_name = "2" * 32 + ".json"
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.bind(str(path / socket_name))
            with MailboxDirectory(path) as mailbox:
                with pytest.raises(ValueError, match="name"):
                    mailbox.read("../secret")
                with pytest.raises(ValueError, match="regular"):
                    mailbox.read(name)
                with pytest.raises(OSError):
                    mailbox.read(socket_name)


def test_fifo_read_is_nonblocking_in_a_subprocess(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    name = "3" * 32 + ".json"
    os.mkfifo(path / name)
    script = f"""
from pathlib import Path
from httk.workflow._daemon_mailbox import MailboxDirectory

mailbox = MailboxDirectory(Path({str(path)!r}))
try:
    mailbox.read({name!r})
except ValueError:
    raise SystemExit(0)
except BaseException:
    raise SystemExit(3)
raise SystemExit(4)
"""
    completed = subprocess.run([sys.executable, "-c", script], timeout=15, check=False)
    assert completed.returncode == 0


def test_read_refuses_final_symlink(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    name = "7" * 32 + ".json"
    target = tmp_path / "outside.json"
    target.write_bytes(b"outside")
    (path / name).symlink_to(target)

    with MailboxDirectory(path) as mailbox, pytest.raises(OSError):
        mailbox.read(name)


def test_scans_skip_unrelated_entries_and_return_bounded_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _mailbox(tmp_path)
    for index in range(mailbox_module.MAX_DIRECTORY_ENTRIES + 1):
        (path / f"unrelated-{index}").touch()
        (path / f".hidden-{index}").touch()
    valid = "4" * 32 + ".json"
    (path / valid).write_bytes(b"ok")

    with MailboxDirectory(path) as mailbox:
        assert mailbox.scan() == ((valid,), False)
        monkeypatch.setattr(mailbox_module, "MAX_SCANNED_ENTRIES", 10)
        names, truncated = mailbox.scan()
        assert truncated and len(names) <= 1
        monkeypatch.undo()
        monkeypatch.setattr(mailbox_module, "MAX_DIRECTORY_ENTRIES", 1)
        (path / ("5" * 32 + ".json")).write_bytes(b"ok")
        names, truncated = mailbox.scan()
        assert truncated and len(names) == 1


def test_a_child_mailbox_is_opened_without_following_a_symlink(tmp_path: Path) -> None:
    parent = tmp_path / "exchange"
    (parent / "requests").mkdir(parents=True)
    (parent / "linked").symlink_to(parent / "requests", target_is_directory=True)
    descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with MailboxDirectory.child(descriptor, "requests") as mailbox:
            name = mailbox.publish(b"ok")
        assert (parent / "requests" / name).read_bytes() == b"ok"
        with pytest.raises(OSError):
            MailboxDirectory.child(descriptor, "linked")
        for bad in ("", "..", "a/b"):
            with pytest.raises(ValueError):
                MailboxDirectory.child(descriptor, bad)
    finally:
        os.close(descriptor)


def test_names_filters_temporary_and_unrelated_entries_and_sorts(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    names = ("f" * 32 + ".json", "0" * 32 + ".json", "a" * 32 + ".json")
    for name in names:
        (path / name).write_bytes(b"ok")
    (path / ("b" * 32 + ".tmp")).write_bytes(b"temporary")
    (path / "request.json").write_bytes(b"unrelated")

    with MailboxDirectory(path) as mailbox:
        assert mailbox.names() == tuple(sorted(names))


def test_read_rejects_oversize_and_detects_growth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)
    oversize_name = "5" * 32 + ".json"
    (path / oversize_name).write_bytes(b"x" * (mailbox_module.MAX_DOCUMENT_BYTES + 1))

    growing_name = "6" * 32 + ".json"
    growing_path = path / growing_name
    growing_path.write_bytes(b"x" * mailbox_module.MAX_DOCUMENT_BYTES)
    writer = os.open(growing_path, os.O_WRONLY | os.O_CLOEXEC)
    original_read = os.read
    first = True

    def partial_read(descriptor: int, size: int) -> bytes:
        nonlocal first
        chunk = original_read(descriptor, min(size, 1))
        if first:
            first = False
            os.lseek(writer, 0, os.SEEK_END)
            os.write(writer, b"y")
        return chunk

    monkeypatch.setattr(mailbox_module.os, "read", partial_read)
    try:
        with MailboxDirectory(path) as mailbox:
            with pytest.raises(ValueError, match="exceeds"):
                mailbox.read(oversize_name)
            with pytest.raises(ValueError, match="exceeds"):
                mailbox.read(growing_name)
    finally:
        os.close(writer)


def test_descriptor_remains_anchored_after_ancestor_rename(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    path = parent / "mailbox"
    path.mkdir(parents=True)
    with MailboxDirectory(path) as mailbox:
        renamed = tmp_path / "old-parent"
        parent.rename(renamed)
        (parent / "mailbox").mkdir(parents=True)
        old_name = mailbox.publish(b"old")
        assert mailbox.read(old_name) == b"old"
        assert (renamed / "mailbox" / old_name).read_bytes() == b"old"
        assert not (parent / "mailbox" / old_name).exists()


def test_close_is_idempotent_and_operations_reject_use_after_close(tmp_path: Path) -> None:
    mailbox = MailboxDirectory(_mailbox(tmp_path))
    mailbox.close()
    mailbox.close()
    with pytest.raises(ValueError, match="closed"):
        mailbox.names()
    with pytest.raises(ValueError, match="closed"):
        mailbox.publish(b"x")
    with pytest.raises(ValueError, match="closed"):
        mailbox.read("0" * 32 + ".json")


def test_short_writes_are_completed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)
    original_write = os.write

    def short_write(descriptor: int, data: bytes) -> int:
        return original_write(descriptor, data[:1])

    monkeypatch.setattr(mailbox_module.os, "write", short_write)
    with MailboxDirectory(path) as mailbox:
        name = mailbox.publish(b"a long enough document")
        assert mailbox.read(name) == b"a long enough document"


def test_partial_write_interrupted_error_cleans_temporary_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)
    original_write = os.write

    def partial_then_interrupt(descriptor: int, data: bytes) -> int:
        original_write(descriptor, data[:1])
        raise InterruptedError("injected interrupted write")

    monkeypatch.setattr(mailbox_module.os, "write", partial_then_interrupt)
    with MailboxDirectory(path) as mailbox, pytest.raises(InterruptedError):
        mailbox.publish(b"data")
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_close_error_does_not_retry_consumed_descriptor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)
    original_close = os.close
    calls: list[int] = []
    mailbox = MailboxDirectory(path)

    def consume_then_raise(descriptor: int) -> None:
        calls.append(descriptor)
        original_close(descriptor)
        if len(calls) == 1:
            raise OSError("injected close failure")

    monkeypatch.setattr(mailbox_module.os, "close", consume_then_raise)
    with mailbox, pytest.raises(OSError):
        mailbox.publish(b"data")
    assert len(calls) == 2
    assert calls[0] != calls[1]
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


@pytest.mark.parametrize("failure", ["error"])
def test_write_failures_clean_up_temporary_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    path = _mailbox(tmp_path)

    def broken_write(descriptor: int, data: bytes) -> int:
        if failure == "zero":
            return 0
        raise OSError("injected write failure")

    monkeypatch.setattr(mailbox_module.os, "write", broken_write)
    with MailboxDirectory(path) as mailbox, pytest.raises(OSError):
        mailbox.publish(b"data")
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_file_fsync_failure_cleans_temporary_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("injected file fsync failure")

    monkeypatch.setattr(mailbox_module.os, "fsync", fail_fsync)
    with MailboxDirectory(path) as mailbox, pytest.raises(OSError):
        mailbox.publish(b"data")
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_rename_failure_cleans_temporary_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)

    def fail_rename(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected rename failure")

    monkeypatch.setattr(mailbox_module.os, "replace", fail_rename)
    with MailboxDirectory(path) as mailbox, pytest.raises(OSError):
        mailbox.publish(b"data")
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_directory_fsync_failure_leaves_ambiguous_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)
    original_fsync = os.fsync
    calls = 0

    def fail_directory_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(mailbox_module.os, "fsync", fail_directory_fsync)
    with MailboxDirectory(path) as mailbox:
        pytest.raises(OSError, mailbox.publish, b"data")
        names = mailbox.names()
        assert len(names) == 1
        assert mailbox.read(names[0]) == b"data"
    assert not [item for item in path.iterdir() if item.name.endswith(".tmp")]


def test_failed_operations_close_descriptors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _mailbox(tmp_path)
    invalid_name = "8" * 32 + ".json"
    (path / invalid_name).mkdir()
    baseline = len(os.listdir("/proc/self/fd"))

    for _ in range(4):
        with pytest.raises(OSError):
            MailboxDirectory(path / "missing")

    def fail_write(_descriptor: int, _data: bytes) -> int:
        raise OSError("injected write failure")

    monkeypatch.setattr(mailbox_module.os, "write", fail_write)
    with MailboxDirectory(path) as mailbox:
        for _ in range(4):
            with pytest.raises(ValueError):
                mailbox.read(invalid_name)
            with pytest.raises(OSError):
                mailbox.publish(b"data")

    assert len(os.listdir("/proc/self/fd")) == baseline


def test_set_aside_moves_any_publication_named_entry_out_of_scans(tmp_path: Path) -> None:
    path = _mailbox(tmp_path)
    name = "7" * 32 + ".json"
    (path / name).mkdir()
    (path / name / "content").write_bytes(b"x")
    with MailboxDirectory(path) as mailbox:
        identity = mailbox.identity()
        assert mailbox.scan() == ((name,), False)
        assert mailbox.scan(skip={name}) == ((), False)
        aside = mailbox.set_aside(name)
        assert aside.startswith(f".invalid-{name}-") and (path / aside / "content").exists()
        assert mailbox.scan() == ((), False) and mailbox.identity() == identity
        with pytest.raises(FileNotFoundError):
            mailbox.set_aside(name)
        with pytest.raises(ValueError):
            mailbox.set_aside("../x")


def test_read_regular_tells_a_symlink_from_a_special_file_by_the_unsafe_kind(tmp_path: Path) -> None:
    (tmp_path / "target").write_text("x")
    (tmp_path / "link").symlink_to("target")
    os.mkfifo(tmp_path / "fifo")
    with pytest.raises(OSError) as raised:
        read_regular(_fs.loc(tmp_path / "link"), 16)
    assert raised.value.errno == errno.ELOOP
    with pytest.raises(ValueError, match="not a regular file"):
        read_regular(_fs.loc(tmp_path / "fifo"), 16)
