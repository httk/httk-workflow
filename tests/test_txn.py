"""The lock-free primitives of the transfer protocol (plan 4.3): outcomes decided by identity, not error codes."""

import errno
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import _txn
from httk.workflow.errors import FormatError

_REAL_RENAME = os.rename
_REAL_LINK = os.link


@pytest.fixture
def control(tmp_path: Path) -> Path:
    """A bare workspace control directory with the ``tmp/`` the primitives use."""

    directory = tmp_path / ".httk-workspace"
    (directory / "tmp").mkdir(parents=True)
    (directory / "managers").mkdir()
    return directory


@pytest.fixture
def hook() -> Iterator[list[str]]:
    """Record every protocol step the primitives report."""

    steps: list[str] = []
    _txn._HOOK = steps.append
    try:
        yield steps
    finally:
        _txn._HOOK = None


def _then_raise(real: Callable[..., None], error: OSError) -> Callable[..., None]:
    """Perform the real operation, then report an error anyway (an NFS retransmission reply)."""

    def operation(*args: Any, **kwargs: Any) -> None:
        real(*args, **kwargs)
        raise error

    return operation


# ---------------------------------------------------------------------------
# rename_verified
# ---------------------------------------------------------------------------


def test_rename_verified_moves_a_directory(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    assert _txn.rename_verified(tmp_path / "a", tmp_path / "b")
    assert (tmp_path / "b").is_dir() and not (tmp_path / "a").exists()


@pytest.mark.parametrize("error", [FileNotFoundError(errno.ENOENT, "lost reply"), FileExistsError(errno.EEXIST, "dup")])
def test_rename_verified_reports_a_retransmitted_success_as_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: OSError
) -> None:
    (tmp_path / "a").mkdir()
    monkeypatch.setattr(_txn.os, "rename", _then_raise(_REAL_RENAME, error))
    assert _txn.rename_verified(tmp_path / "a", tmp_path / "b") is True
    assert (tmp_path / "b").is_dir()


def test_rename_verified_reports_a_genuine_loss(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "a").mkdir()

    def another_actor_wins(src: Any, dst: Any, **kwargs: Any) -> None:
        _REAL_RENAME(src, tmp_path / "elsewhere")
        raise FileNotFoundError(errno.ENOENT, "source vanished")

    monkeypatch.setattr(_txn.os, "rename", another_actor_wins)
    assert _txn.rename_verified(tmp_path / "a", tmp_path / "b") is False
    assert (tmp_path / "elsewhere").is_dir() and not (tmp_path / "b").exists()


def test_rename_verified_loses_when_the_source_is_already_gone(tmp_path: Path) -> None:
    assert _txn.rename_verified(tmp_path / "absent", tmp_path / "b") is False


def test_rename_verified_does_not_mistake_another_entry_at_the_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "x").write_text("someone else's")

    def another_actor_wins(src: Any, dst: Any, **kwargs: Any) -> None:
        _REAL_RENAME(src, tmp_path / "elsewhere")
        raise OSError(errno.ENOTEMPTY, "not empty")

    monkeypatch.setattr(_txn.os, "rename", another_actor_wins)
    assert _txn.rename_verified(tmp_path / "a", tmp_path / "b") is False


def test_rename_verified_raises_when_the_source_stays(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "x").write_text("occupied")
    with pytest.raises(OSError) as raised:
        _txn.rename_verified(tmp_path / "a", tmp_path / "b")
    assert raised.value.errno in {errno.ENOTEMPTY, errno.EEXIST}
    assert (tmp_path / "a").is_dir()


def test_rename_verified_never_translates_exdev(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "a").mkdir()
    monkeypatch.setattr(_txn.os, "rename", _then_raise(lambda *a, **k: None, OSError(errno.EXDEV, "cross")))
    with pytest.raises(OSError) as raised:
        _txn.rename_verified(tmp_path / "a", tmp_path / "b")
    assert raised.value.errno == errno.EXDEV


def test_rename_verified_works_relative_to_directory_descriptors(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "out").mkdir()
    (tmp_path / "in" / "entry").mkdir()
    source = os.open(tmp_path / "in", os.O_RDONLY | os.O_DIRECTORY)
    target = os.open(tmp_path / "out", os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert _txn.rename_verified("entry", "moved", src_dir_fd=source, dst_dir_fd=target)
        assert not _txn.rename_verified("entry", "again", src_dir_fd=source, dst_dir_fd=target)
    finally:
        os.close(source)
        os.close(target)
    assert (tmp_path / "out" / "moved").is_dir()


# ---------------------------------------------------------------------------
# link_new
# ---------------------------------------------------------------------------


def test_link_new_creates_the_file_and_leaves_no_temporary(tmp_path: Path, hook: list[str]) -> None:
    assert _txn.link_new(tmp_path, "intent.json", b'{"a": 1}', mode=0o600)
    assert (tmp_path / "intent.json").read_bytes() == b'{"a": 1}'
    assert (tmp_path / "intent.json").stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "intent.json").stat().st_nlink == 1
    assert os.listdir(tmp_path) == ["intent.json"]
    assert hook == ["link_new.written"]


def test_link_new_never_replaces_different_content(tmp_path: Path) -> None:
    (tmp_path / "name").write_bytes(b"theirs")
    assert _txn.link_new(tmp_path, "name", b"mine") is False
    assert _txn.link_new(tmp_path, "name", b"mine", nonce=True) is False
    assert (tmp_path / "name").read_bytes() == b"theirs"
    assert os.listdir(tmp_path) == ["name"]


def test_link_new_accepts_identical_bytes_only_with_a_nonce(tmp_path: Path) -> None:
    (tmp_path / "name").write_bytes(b"same")
    assert _txn.link_new(tmp_path, "name", b"same") is False
    assert _txn.link_new(tmp_path, "name", b"same", nonce=True) is True


@pytest.mark.parametrize("error", [FileExistsError(errno.EEXIST, "dup"), FileNotFoundError(errno.ENOENT, "lost")])
def test_link_new_reports_a_retransmitted_link_as_its_own(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: OSError
) -> None:
    monkeypatch.setattr(_txn.os, "link", _then_raise(_REAL_LINK, error))
    # No nonce: the inode is this call's own temporary, which decides it.
    assert _txn.link_new(tmp_path, "manifest.json", b"deterministic") is True
    assert (tmp_path / "manifest.json").read_bytes() == b"deterministic"
    assert os.listdir(tmp_path) == ["manifest.json"]


def test_link_new_raises_when_the_link_failed_and_nothing_is_there(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_txn.os, "link", _then_raise(lambda *a, **k: None, OSError(errno.EIO, "io")))
    with pytest.raises(OSError) as raised:
        _txn.link_new(tmp_path, "name", b"data")
    assert raised.value.errno == errno.EIO
    assert os.listdir(tmp_path) == []


def test_link_new_works_through_a_directory_descriptor(tmp_path: Path) -> None:
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert _txn.link_new(descriptor, "reason.json", b"why")
        assert not _txn.link_new(descriptor, "reason.json", b"other")
    finally:
        os.close(descriptor)
    assert (tmp_path / "reason.json").read_bytes() == b"why"


def test_link_new_refuses_a_path_as_name(tmp_path: Path) -> None:
    for name in ("", ".", "..", "a/b"):
        with pytest.raises(ValueError):
            _txn.link_new(tmp_path, name, b"")


def test_link_new_temporaries_are_recognizable(tmp_path: Path) -> None:
    def crash(step: str) -> None:
        raise RuntimeError(step)

    _txn._HOOK = crash
    try:
        with pytest.raises(RuntimeError):
            _txn.link_new(tmp_path, "manifest.json", b"x")
    finally:
        _txn._HOOK = None
    # The temporary was removed on the way out; one a hard crash leaves is recognizable.
    assert os.listdir(tmp_path) == []
    assert _txn.is_link_temporary(f".manifest.json.link-{uuid.uuid4().hex}")
    assert not _txn.is_link_temporary("manifest.json")


# ---------------------------------------------------------------------------
# trash and remove_tree
# ---------------------------------------------------------------------------


def test_trash_removes_a_tree_including_read_only_directories(control: Path, hook: list[str]) -> None:
    victim = control / "tmp" / "abort.x"
    (victim / "runners" / "tool").mkdir(parents=True)
    (victim / "runners" / "tool" / "run").write_text("#!/bin/sh\n")
    (victim / "runners" / "tool").chmod(0o555)
    assert _txn.trash(victim, control=control, holds_payload=_txn.holds_job_payload)
    assert os.listdir(control / "tmp") == []
    assert hook == ["trash.renamed"]


def test_trash_never_follows_a_symlink(control: Path, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "precious").write_text("keep")
    victim = control / "tmp" / "eject.y"
    victim.mkdir()
    (victim / "link").symlink_to(outside)
    assert _txn.trash(victim, control=control, holds_payload=None)
    assert (outside / "precious").read_text() == "keep"


def test_trash_of_an_absent_tree_is_someone_elses(control: Path) -> None:
    assert _txn.trash(control / "tmp" / "gone", control=control, holds_payload=None) is False


def test_trash_quarantines_a_payload_instead_of_removing_it(control: Path, caplog: pytest.LogCaptureFixture) -> None:
    victim = control / "tmp" / "eject.z"
    (victim / "payload").mkdir(parents=True)
    (victim / "payload" / "job.json").write_text("{}")
    with caplog.at_level(logging.ERROR, logger="httk.workflow._txn"):
        assert _txn.trash(victim, control=control, holds_payload=_txn.holds_job_payload)
    assert not victim.exists()
    [quarantined] = list((control / "quarantine").iterdir())
    assert (quarantined / "entry" / "payload" / "job.json").read_text() == "{}"
    report = json.loads((quarantined / "report.json").read_text())
    assert report["original_path"] == str(victim)
    assert [getattr(record, "event", None) for record in caplog.records] == ["trash_quarantined"]
    assert os.listdir(control / "tmp") == []


def test_trash_without_a_payload_check_removes_everything(control: Path) -> None:
    victim = control / "tmp" / "export.a"
    (victim / "bundle").mkdir(parents=True)
    (victim / "bundle" / "job.json").write_text("{}")
    assert _txn.trash(victim, control=control, holds_payload=None)
    assert not (control / "quarantine").exists() and os.listdir(control / "tmp") == []


def test_trash_loses_cleanly_to_a_concurrent_trasher(control: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    victim = control / "tmp" / "abort.b"
    victim.mkdir()

    def other_trasher(src: Any, dst: Any, **kwargs: Any) -> None:
        _REAL_RENAME(src, control / "tmp" / "trash.other")
        raise FileNotFoundError(errno.ENOENT, "gone")

    monkeypatch.setattr(_txn.os, "rename", other_trasher)
    assert _txn.trash(victim, control=control, holds_payload=_txn.holds_job_payload) is False
    assert (control / "tmp" / "trash.other").is_dir()


def test_remove_tree_is_iterative_and_tolerates_absence(tmp_path: Path) -> None:
    deep = tmp_path / "deep"
    deep.mkdir()
    descriptor = os.open(deep, os.O_RDONLY | os.O_DIRECTORY)
    for _ in range(1200):  # deeper than the interpreter's recursion limit
        os.mkdir("d", dir_fd=descriptor)
        inner = os.open("d", os.O_RDONLY | os.O_DIRECTORY, dir_fd=descriptor)
        os.close(descriptor)
        descriptor = inner
    os.close(os.open("leaf", os.O_WRONLY | os.O_CREAT, 0o644, dir_fd=descriptor))
    os.close(descriptor)
    _txn.remove_tree(deep)
    assert not deep.exists()
    _txn.remove_tree(deep)
    _txn.remove_tree(tmp_path / "never" / "existed")
    file = tmp_path / "file"
    file.write_text("x")
    _txn.remove_tree(file)
    assert not file.exists()


def test_holds_job_payload(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    assert not _txn.holds_job_payload(tmp_path / "empty")
    (tmp_path / "deep" / "tree" / "k").mkdir(parents=True)
    (tmp_path / "deep" / "tree" / "k" / "job.json").write_text("{}")
    assert _txn.holds_job_payload(tmp_path / "deep")
    (tmp_path / "link").mkdir()
    (tmp_path / "link" / "to").symlink_to(tmp_path / "deep")
    assert not _txn.holds_job_payload(tmp_path / "link")
    assert not _txn.holds_job_payload(tmp_path / "absent")


# ---------------------------------------------------------------------------
# birth
# ---------------------------------------------------------------------------


def _skeleton(directory: Path) -> None:
    (directory / "envelope.partial" / "markers").mkdir(parents=True)


def test_birth_publishes_a_complete_directory(control: Path, hook: list[str]) -> None:
    tmp = control / "tmp"
    assert _txn.birth(tmp, "eject.t", _skeleton)
    assert (tmp / "eject.t" / "envelope.partial" / "markers").is_dir()
    assert sorted(os.listdir(tmp)) == ["eject.t"]
    assert hook == ["birth.built"]


def test_birth_onto_a_non_empty_existing_target_fails(control: Path) -> None:
    tmp = control / "tmp"
    (tmp / "eject.t" / "payload").mkdir(parents=True)
    assert _txn.birth(tmp, "eject.t", _skeleton) is False
    assert sorted(os.listdir(tmp)) == ["eject.t"]
    assert os.listdir(tmp / "eject.t") == ["payload"]
    (tmp / "file").write_text("x")
    assert _txn.birth(tmp, "file", _skeleton) is False
    assert sorted(os.listdir(tmp)) == ["eject.t", "file"]


def test_birth_cleans_up_a_failed_build(control: Path) -> None:
    def broken(directory: Path) -> None:
        (directory / "half").mkdir()
        raise RuntimeError("crash while building")

    with pytest.raises(RuntimeError):
        _txn.birth(control / "tmp", "eject.u", broken)
    assert os.listdir(control / "tmp") == []


# ---------------------------------------------------------------------------
# times and owners
# ---------------------------------------------------------------------------


def test_name_encoded_times_are_strict() -> None:
    now = time.time_ns()
    assert _txn.parse_ns(_txn.format_ns(now)) == now
    assert _txn.parse_ns("0") == 0
    for text in ("", "01", "-1", "+1", "1.0", " 1", "1e3", str(1 << 63)):
        with pytest.raises(FormatError):
            _txn.parse_ns(text)
    for value in (-1, 1 << 63, True, 1.0):
        with pytest.raises(ValueError):
            _txn.format_ns(value)  # type: ignore[arg-type]


def test_owner_tokens_round_trip() -> None:
    manager_id = str(uuid.uuid4())
    token = _txn.owner_token(manager_id)
    assert token == "m" + uuid.UUID(manager_id).hex
    assert _txn.parse_owner_token(token) == _txn.OwnerToken("manager", manager_id=manager_id)
    cli = _txn.owner_token()
    assert cli.isalnum() and cli == cli.lower() and cli.startswith("c")
    parsed = _txn.parse_owner_token(cli)
    assert parsed.kind == "cli" and parsed.pid == os.getpid()
    for junk in ("", "m1234", "x" + "0" * 32, "c" + "0" * 8 + "01" + "0" * 8, "M" + uuid.uuid4().hex, "c1.2"):
        with pytest.raises(FormatError):
            _txn.parse_owner_token(junk)


def _heartbeat(control: Path, manager_id: str, age_seconds: float) -> None:
    directory = control / "managers" / manager_id
    directory.mkdir(parents=True, exist_ok=True)
    from datetime import UTC, datetime, timedelta

    stamp = (datetime.now(UTC) - timedelta(seconds=age_seconds)).isoformat().replace("+00:00", "Z")
    (directory / "heartbeat.json").write_text(json.dumps({"manager_id": manager_id, "updated_at": stamp}))


def test_a_manager_owner_is_gone_after_the_takeover_grace_or_without_its_directory(control: Path) -> None:
    now = time.time_ns()
    lease = 10.0  # grace = lease * DEFAULT_TAKEOVER_GRACE_FACTOR = 20 s
    manager_id = str(uuid.uuid4())
    token = _txn.owner_token(manager_id)
    assert _txn.owner_gone(control, token, since=now, now=now, lease_seconds=lease)  # no directory
    _heartbeat(control, manager_id, 15.0)
    assert not _txn.owner_gone(control, token, since=now, now=now, lease_seconds=lease)
    _heartbeat(control, manager_id, 25.0)
    assert _txn.owner_gone(control, token, since=now, now=now, lease_seconds=lease)
    # An unreadable heartbeat is aged from when the work started instead.
    (control / "managers" / manager_id / "heartbeat.json").write_text("not json")
    assert not _txn.owner_gone(control, token, since=now - 5 * 10**9, now=now, lease_seconds=lease)
    assert _txn.owner_gone(control, token, since=now - 30 * 10**9, now=now, lease_seconds=lease)


def test_a_cli_owner_is_gone_after_a_day_or_when_its_process_died_here(control: Path) -> None:
    now = time.time_ns()
    alive = _txn.owner_token()
    day = _txn.CLI_OWNER_SECONDS * 10**9
    assert not _txn.owner_gone(control, alive, since=now, now=now, lease_seconds=900.0)
    assert _txn.owner_gone(control, alive, since=now - day - 1, now=now, lease_seconds=900.0)
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    parsed = _txn.parse_owner_token(alive)
    dead = f"c{parsed.host}{child.pid}{parsed.boot}"
    if parsed.boot != "00000000":
        assert _txn.owner_gone(control, dead, since=now, now=now, lease_seconds=900.0)
    # On another host (or boot) a dead-looking pid proves nothing: only the day counts.
    elsewhere = f"c{'f' * 8 if parsed.host != 'f' * 8 else 'e' * 8}{child.pid}{parsed.boot}"
    assert not _txn.owner_gone(control, elsewhere, since=now, now=now, lease_seconds=900.0)
    assert _txn.owner_gone(control, elsewhere, since=now - day - 1, now=now, lease_seconds=900.0)


def test_link_new_handles_a_name_near_name_max(tmp_path: Path) -> None:
    name = "n" * 250
    assert _txn.link_new(tmp_path, name, b"data", durable=False)
    assert (tmp_path / name).read_bytes() == b"data" and os.listdir(tmp_path) == [name]
