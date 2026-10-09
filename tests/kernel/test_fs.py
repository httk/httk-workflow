"""Unit tests of :mod:`httk.workflow._fs`: every decision branch, with injected rename errors."""

import errno
import os
import re
import shutil
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import _fs
from httk.workflow._fs import (
    CrossDevice,
    Delivered,
    DeliveryUncertain,
    Entry,
    FilesystemUnsupported,
    Loc,
    Moved,
    MoveFailed,
    TooLarge,
    UnsafePath,
    UntrustedContentError,
    WalkLimits,
    anchored,
    append_file,
    create_exclusive,
    deliver,
    discard,
    exists,
    fresh_token,
    loc,
    move_once,
    move_owned,
    open_dir,
    publish_dir,
    read_bounded,
    remove_empty_dir,
    remove_file,
    rename_probe,
    set_fault_injector,
    walk_untrusted,
    write_file,
)

REAL_RENAME = os.rename
REAL_REPLACE = os.replace

type Rename = Callable[..., None]


@pytest.fixture(autouse=True)
def _no_fault_injector() -> Iterator[None]:
    yield
    set_fault_injector(None)


def _tree(root: Path, name: str = "payload") -> Path:
    root.mkdir(parents=True)
    (root / name).write_text("content")
    return root


def _fail(code: int, *, perform: bool = False, times: int | None = None) -> tuple[Rename, list[int]]:
    """Return an os.rename stand-in raising *code* (after renaming when *perform*) and its call counter."""

    calls: list[int] = []

    def rename(src: object, dst: object, **kwargs: object) -> None:
        calls.append(1)
        if times is not None and len(calls) > times:
            REAL_RENAME(src, dst, **kwargs)  # type: ignore[arg-type]
            return
        if perform:
            REAL_RENAME(src, dst, **kwargs)  # type: ignore[arg-type]
        raise OSError(code, os.strerror(code))

    return rename, calls


def _count_fsync(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    real = os.fsync

    def fsync(descriptor: int) -> None:
        calls.append(descriptor)
        real(descriptor)

    monkeypatch.setattr(os, "fsync", fsync)
    return calls


# Locations


def test_loc_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        loc(Path("relative"))
    descriptor = open_dir(tmp_path)
    try:
        for name in ("", ".", "..", "a/b", "/abs", "nul\0"):
            with pytest.raises(ValueError):
                anchored(descriptor, name)
        named = anchored(descriptor, "x")
        assert named.name() == "x"
        with pytest.raises(ValueError):
            named.parent()
    finally:
        os.close(descriptor)
    assert loc(tmp_path / "x").parent() == Loc(tmp_path)


def test_open_dir_refuses_symlink_and_file(tmp_path: Path) -> None:
    (tmp_path / "real").mkdir()
    (tmp_path / "link").symlink_to("real")
    (tmp_path / "file").write_text("")
    for name in ("link", "file"):
        with pytest.raises(UnsafePath):
            open_dir(tmp_path / name)


def test_exists_never_follows(tmp_path: Path) -> None:
    (tmp_path / "dangling").symlink_to("nowhere")
    (tmp_path / "file").write_text("")
    assert exists(loc(tmp_path / "dangling"))
    assert not exists(loc(tmp_path / "absent"))
    assert not exists(loc(tmp_path / "file" / "below"))


# move_once


def test_move_once_won(tmp_path: Path) -> None:
    src = _tree(tmp_path / "src")
    dst = tmp_path / "a" / "b" / "dst"
    assert move_once(loc(src), loc(dst), durable=False) is Moved.WON
    assert (dst / "payload").read_text() == "content"
    assert not src.exists()


def test_move_once_won_when_rename_performs_then_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    rename, calls = _fail(errno.EIO, perform=True)
    monkeypatch.setattr(os, "rename", rename)
    assert move_once(loc(src), loc(tmp_path / "dst"), durable=False) is Moved.WON
    assert calls == [1]


def test_move_once_lost_when_another_actor_moved_src(tmp_path: Path) -> None:
    src = _tree(tmp_path / "src")
    REAL_RENAME(src, tmp_path / "theirs")
    assert move_once(loc(src), loc(tmp_path / "dst"), durable=False) is Moved.LOST
    assert not (tmp_path / "dst").exists()


def test_move_once_lost_after_settle(tmp_path: Path) -> None:
    started = time.monotonic()
    assert move_once(loc(tmp_path / "gone"), loc(tmp_path / "dst"), durable=False, settle=0.2) is Moved.LOST
    assert time.monotonic() - started >= 0.2


def test_move_once_won_when_dst_appears_during_settle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    dst = tmp_path / "dst"
    hidden = tmp_path / "in-flight"
    late = threading.Timer(0.15, REAL_RENAME, (hidden, dst))

    def rename(source: object, destination: object, **kwargs: object) -> None:
        # The rename happened but is not yet visible: dst only appears later.
        REAL_RENAME(src, hidden)
        late.start()
        raise OSError(errno.ENOENT, "stale")

    monkeypatch.setattr(os, "rename", rename)
    try:
        assert move_once(loc(src), loc(dst), durable=True, settle=5.0) is Moved.WON
    finally:
        late.join()
    assert (dst / "payload").exists()


def test_move_once_retries_when_dst_parent_is_pruned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    dst = tmp_path / "parent" / "dst"
    calls: list[int] = []

    def rename(source: object, destination: object, **kwargs: object) -> None:
        calls.append(1)
        if len(calls) == 1:
            os.rmdir(dst.parent)  # a pruner removes the parent _ensure_parent just made
        REAL_RENAME(source, destination, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", rename)
    assert move_once(loc(src), loc(dst), durable=False) is Moved.WON
    assert calls == [1, 1]
    assert (dst / "payload").exists()


def test_move_once_fails_after_eight_attempts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    rename, calls = _fail(errno.EIO)
    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(MoveFailed) as raised:
        move_once(loc(src), loc(tmp_path / "dst"), durable=False)
    assert raised.value.errno == errno.EIO
    assert len(calls) == 8
    assert src.exists()


def test_move_once_cross_device(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    rename, _ = _fail(errno.EXDEV)
    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(CrossDevice):
        move_once(loc(src), loc(tmp_path / "dst"), durable=False)


def test_move_once_durable_fsyncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    calls = _count_fsync(monkeypatch)
    assert move_once(loc(src), loc(tmp_path / "new" / "dst"), durable=True) is Moved.WON
    # The new parent's entry, then both parent directories.
    assert len(calls) == 3


def test_move_once_refuses_symlink_component(tmp_path: Path) -> None:
    src = _tree(tmp_path / "src")
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "link").symlink_to("elsewhere")
    for dst in (tmp_path / "link" / "sub" / "dst", tmp_path / "link" / "dst"):
        with pytest.raises(UnsafePath):
            move_once(loc(src), loc(dst), durable=False)
    assert list((tmp_path / "elsewhere").iterdir()) == []
    assert src.exists()


def test_move_once_tolerates_concurrent_mkdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    real_mkdir = os.mkdir

    def mkdir(path: Path, *args: object, **kwargs: object) -> None:
        real_mkdir(path)  # another actor wins the mkdir
        real_mkdir(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "mkdir", mkdir)
    assert move_once(loc(src), loc(tmp_path / "a" / "dst"), durable=False) is Moved.WON


def test_ensure_parent_refuses_concurrent_symlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    (tmp_path / "elsewhere").mkdir()

    def mkdir(path: Path, *args: object, **kwargs: object) -> None:
        os.symlink(tmp_path / "elsewhere", path)  # an attacker wins the race with a symlink
        raise FileExistsError(errno.EEXIST, "exists")

    monkeypatch.setattr(os, "mkdir", mkdir)
    with pytest.raises(UnsafePath):
        move_once(loc(src), loc(tmp_path / "a" / "dst"), durable=False)
    assert list((tmp_path / "elsewhere").iterdir()) == []


def test_ensure_parent_rounds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    real_mkdir = os.mkdir
    calls: list[int] = []

    def mkdir(path: Path, *args: object, **kwargs: object) -> None:
        # An ancestor is pruned between mkdirs: the first round fails, the next succeeds.
        calls.append(1)
        if len(calls) == 1:
            raise FileNotFoundError(errno.ENOENT, "pruned")
        real_mkdir(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "mkdir", mkdir)
    assert move_once(loc(src), loc(tmp_path / "a" / "b" / "dst"), durable=False) is Moved.WON

    def always_pruned(path: Path, *args: object, **kwargs: object) -> None:
        raise FileNotFoundError(errno.ENOENT, "pruned")

    monkeypatch.setattr(os, "mkdir", always_pruned)
    with pytest.raises(MoveFailed) as raised:
        move_once(loc(tmp_path / "a" / "b" / "dst"), loc(tmp_path / "c" / "dst"), durable=False)
    assert raised.value.errno == errno.ENOENT


def test_move_once_mixed_anchors(tmp_path: Path) -> None:
    _tree(tmp_path / "inbox" / "item")
    inbox = open_dir(tmp_path / "inbox")
    try:
        assert move_once(anchored(inbox, "item"), loc(tmp_path / "taken"), durable=True) is Moved.WON
        assert move_once(loc(tmp_path / "taken"), anchored(inbox, "back"), durable=True) is Moved.WON
    finally:
        os.close(inbox)
    assert (tmp_path / "inbox" / "back" / "payload").exists()


# move_owned


def test_move_owned_moves(tmp_path: Path) -> None:
    src = _tree(tmp_path / "src")
    move_owned(loc(src), loc(tmp_path / "x" / "dst"), durable=True)
    assert (tmp_path / "x" / "dst" / "payload").exists()


def test_move_owned_done_when_rename_performs_then_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    rename, calls = _fail(errno.EIO, perform=True)
    monkeypatch.setattr(os, "rename", rename)
    move_owned(loc(src), loc(tmp_path / "dst"), durable=False)
    assert calls == [1]


def test_move_owned_done_when_successor_moved_dst_on(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")

    def rename(source: object, destination: object, **kwargs: object) -> None:
        REAL_RENAME(source, destination, **kwargs)  # type: ignore[arg-type]
        REAL_RENAME(tmp_path / "p" / "dst", tmp_path / "successor")
        os.rmdir(tmp_path / "p")  # and a pruner removes the emptied parent before our fsync

    monkeypatch.setattr(os, "rename", rename)
    move_owned(loc(src), loc(tmp_path / "p" / "dst"), durable=True)
    assert (tmp_path / "successor" / "payload").exists()


def test_move_owned_replaces_existing_file(tmp_path: Path) -> None:
    (tmp_path / "staged").write_text("new")
    (tmp_path / "target").write_text("old")
    (tmp_path / "link").symlink_to("target")
    move_owned(loc(tmp_path / "staged"), loc(tmp_path / "target"), durable=False)
    assert (tmp_path / "target").read_text() == "new"
    (tmp_path / "staged").write_text("over link")
    move_owned(loc(tmp_path / "staged"), loc(tmp_path / "link"), durable=False)
    assert not (tmp_path / "link").is_symlink()
    assert (tmp_path / "target").read_text() == "new"


def test_move_owned_retries_then_fails_when_src_persists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "staged").write_text("new")
    (tmp_path / "target").write_text("old")
    rename, calls = _fail(errno.EIO)
    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(MoveFailed) as raised:
        move_owned(loc(tmp_path / "staged"), loc(tmp_path / "target"), durable=False)
    assert raised.value.errno == errno.EIO
    assert len(calls) == 8
    assert (tmp_path / "target").read_text() == "old"


def test_move_owned_retry_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _tree(tmp_path / "src")
    rename, calls = _fail(errno.ENOENT, times=2)
    monkeypatch.setattr(os, "rename", rename)
    move_owned(loc(src), loc(tmp_path / "dst"), durable=False)
    assert len(calls) == 3


def test_move_owned_file_over_directory_reports_errno(tmp_path: Path) -> None:
    (tmp_path / "staged").write_text("new")
    (tmp_path / "target").mkdir()
    with pytest.raises(MoveFailed) as raised:
        move_owned(loc(tmp_path / "staged"), loc(tmp_path / "target"), durable=False)
    assert raised.value.errno == errno.EISDIR


# deliver


def _bundle(path: Path, token: bytes) -> Path:
    path.mkdir(parents=True)
    (path / ".token").write_bytes(token)
    (path / "data").write_text("data")
    return path


def test_deliver_done(tmp_path: Path) -> None:
    src = _bundle(tmp_path / "out" / "b", b"t1")
    dst = tmp_path / "inbox" / "b"
    assert deliver(loc(src), loc(dst), token_name=".token", token=b"t1", durable=True) is Delivered.DONE
    assert (dst / "data").exists()


def test_deliver_done_when_rename_performs_then_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _bundle(tmp_path / "b", b"t1")
    rename, _ = _fail(errno.EIO, perform=True)
    monkeypatch.setattr(os, "rename", rename)
    assert deliver(loc(src), loc(tmp_path / "dst"), token_name=".token", token=b"t1", durable=False) is Delivered.DONE


@pytest.mark.parametrize("occupant", ["dir", "file", "symlink"])
def test_deliver_occupied(tmp_path: Path, occupant: str) -> None:
    src = _bundle(tmp_path / "b", b"t1")
    dst = tmp_path / "dst"
    if occupant == "dir":
        _tree(dst, "theirs")
    elif occupant == "file":
        dst.write_text("theirs")
    else:
        (tmp_path / "elsewhere").mkdir()
        dst.symlink_to("elsewhere")
    assert deliver(loc(src), loc(dst), token_name=".token", token=b"t1", durable=False) is Delivered.OCCUPIED
    assert (src / "data").exists()
    assert not (tmp_path / "elsewhere" / "data").exists()


def test_deliver_other_error_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _bundle(tmp_path / "b", b"t1")
    rename, _ = _fail(errno.EACCES)
    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(PermissionError):
        deliver(loc(src), loc(tmp_path / "dst"), token_name=".token", token=b"t1", durable=False)
    assert src.exists()


def test_deliver_source_present_without_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _bundle(tmp_path / "b", b"t1")
    monkeypatch.setattr(os, "rename", lambda source, destination, **kwargs: None)
    with pytest.raises(MoveFailed):
        deliver(loc(src), loc(tmp_path / "dst"), token_name=".token", token=b"t1", durable=False)


def test_deliver_uncertain_on_token_mismatch(tmp_path: Path) -> None:
    src = _bundle(tmp_path / "b", b"someone else")
    with pytest.raises(DeliveryUncertain):
        deliver(loc(src), loc(tmp_path / "dst"), token_name=".token", token=b"t1", durable=False)


def test_deliver_uncertain_when_src_vanished(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = _bundle(tmp_path / "b", b"t1")

    def rename(source: object, destination: object, **kwargs: object) -> None:
        REAL_RENAME(src, tmp_path / "taken")
        raise OSError(errno.ENOENT, "gone")

    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(DeliveryUncertain):
        deliver(loc(src), loc(tmp_path / "dst"), token_name=".token", token=b"t1", durable=False)


def test_deliver_anchored(tmp_path: Path) -> None:
    _bundle(tmp_path / "outbox" / "b", b"t1")
    (tmp_path / "inbox").mkdir()
    outbox, inbox = open_dir(tmp_path / "outbox"), open_dir(tmp_path / "inbox")
    try:
        delivered = deliver(anchored(outbox, "b"), anchored(inbox, "b"), token_name=".token", token=b"t1", durable=True)
        assert delivered is Delivered.DONE
        _bundle(tmp_path / "outbox" / "c", b"t2")
        occupied = deliver(anchored(outbox, "c"), anchored(inbox, "b"), token_name=".token", token=b"t2", durable=True)
        assert occupied is Delivered.OCCUPIED
    finally:
        os.close(outbox)
        os.close(inbox)


# publish_dir


def _staging(path: Path, nonce: bytes) -> Path:
    path.mkdir(parents=True)
    (path / ".nonce").write_bytes(nonce)
    return path


def test_publish_dir_winner_and_loser(tmp_path: Path) -> None:
    first = _staging(tmp_path / "s1", b"n1")
    second = _staging(tmp_path / "s2", b"n2")
    dst = tmp_path / "ledger" / "entry"
    assert publish_dir(loc(first), loc(dst), nonce=b"n1", durable=True)
    assert not publish_dir(loc(second), loc(dst), nonce=b"n2", durable=True)
    assert (dst / ".nonce").read_bytes() == b"n1"
    assert not first.exists()
    assert not second.exists()


def test_publish_dir_won_when_rename_performs_then_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging = _staging(tmp_path / "s", b"n")
    rename, _ = _fail(errno.EIO, perform=True)
    monkeypatch.setattr(os, "rename", rename)
    assert publish_dir(loc(staging), loc(tmp_path / "dst"), nonce=b"n", durable=False)


def test_publish_dir_loses_when_rename_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging = _staging(tmp_path / "s", b"n")
    rename, _ = _fail(errno.ENOTEMPTY)
    monkeypatch.setattr(os, "rename", rename)
    assert not publish_dir(loc(staging), loc(tmp_path / "dst"), nonce=b"n", durable=False)
    assert not staging.exists()


def test_publish_dir_precondition(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    with pytest.raises(ValueError):
        publish_dir(loc(tmp_path / "empty"), loc(tmp_path / "dst"), nonce=b"n", durable=False)
    staging = _staging(tmp_path / "s", b"other")
    with pytest.raises(ValueError):
        publish_dir(loc(staging), loc(tmp_path / "dst"), nonce=b"n", durable=False)
    assert staging.exists()


# write_file


def test_write_file_replaces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "f.json"
    target.write_text("old")
    calls = _count_fsync(monkeypatch)
    write_file(loc(target), b"new", durable=True)
    assert target.read_bytes() == b"new"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["f.json"]
    assert len(calls) == 2


def test_write_file_mode(tmp_path: Path) -> None:
    write_file(loc(tmp_path / "f"), b"", durable=False, mode=0o600)
    assert (tmp_path / "f").stat().st_mode & 0o777 == 0o600


def test_write_file_refuses_planted_temporary_symlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    outside = tmp_path / "outside"
    outside.write_text("precious")
    (tmp_path / "dir").mkdir()
    planted = tmp_path / "dir" / f".f.{'a' * 16}.tmp"
    planted.symlink_to(outside)
    monkeypatch.setattr(_fs, "fresh_token", lambda: "a" * 16)
    with pytest.raises(FileExistsError):
        write_file(loc(tmp_path / "dir" / "f"), b"attack", durable=False)
    assert outside.read_text() == "precious"
    assert planted.is_symlink()


def test_write_file_removes_temporary_on_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def replace(source: object, destination: object, **kwargs: object) -> None:
        raise OSError(errno.EIO, "io")

    monkeypatch.setattr(os, "replace", replace)
    with pytest.raises(OSError):
        write_file(loc(tmp_path / "f"), b"x", durable=False)
    assert list(tmp_path.iterdir()) == []


def test_write_file_cleanup_tolerates_vanished_temporary(tmp_path: Path) -> None:
    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        assert src is not None
        os.unlink(src.path)
        raise OSError(errno.EIO, "io")

    set_fault_injector(fault)
    with pytest.raises(OSError):
        write_file(loc(tmp_path / "f"), b"x", durable=False)
    assert list(tmp_path.iterdir()) == []


def test_write_file_replace_performed_then_raised(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def replace(source: object, destination: object, **kwargs: object) -> None:
        REAL_REPLACE(source, destination, **kwargs)  # type: ignore[arg-type]
        raise OSError(errno.ENOENT, "retransmitted")

    monkeypatch.setattr(os, "replace", replace)
    write_file(loc(tmp_path / "f"), b"x", durable=False)
    assert (tmp_path / "f").read_bytes() == b"x"


def test_write_file_raises_when_directory_moved_away(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "job").mkdir()

    def replace(source: object, destination: object, **kwargs: object) -> None:
        # A recoverer moves the job directory, temporary included, before our replace.
        REAL_RENAME(tmp_path / "job", tmp_path / "moved")
        raise OSError(errno.ENOENT, "gone")

    monkeypatch.setattr(os, "replace", replace)
    with pytest.raises(FileNotFoundError):
        write_file(loc(tmp_path / "job" / "f"), b"x", durable=False)
    assert not (tmp_path / "job").exists()
    assert not (tmp_path / "moved" / "f").exists()


def test_write_file_anchored(tmp_path: Path) -> None:
    descriptor = open_dir(tmp_path)
    try:
        write_file(anchored(descriptor, "progress.json"), b"{}", durable=True)
    finally:
        os.close(descriptor)
    assert (tmp_path / "progress.json").read_bytes() == b"{}"
    assert [path.name for path in tmp_path.iterdir()] == ["progress.json"]


# create_exclusive


def test_create_exclusive(tmp_path: Path) -> None:
    descriptor = create_exclusive(loc(tmp_path / "out"), b"head", durable=True)
    try:
        os.write(descriptor, b"tail")
    finally:
        os.close(descriptor)
    assert (tmp_path / "out").read_bytes() == b"headtail"
    assert (tmp_path / "out").stat().st_mode & 0o777 == 0o600


def test_create_exclusive_refuses_existing_file_and_symlink(tmp_path: Path) -> None:
    (tmp_path / "file").write_text("theirs")
    (tmp_path / "target").write_text("precious")
    (tmp_path / "link").symlink_to("target")
    (tmp_path / "dangling").symlink_to("nowhere")
    for name in ("file", "link", "dangling"):
        with pytest.raises(FileExistsError):
            create_exclusive(loc(tmp_path / name), b"x", durable=False)
    assert (tmp_path / "target").read_text() == "precious"
    assert not (tmp_path / "nowhere").exists()


# append_file


def test_append_file_creates_then_appends(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _count_fsync(monkeypatch)
    append_file(loc(tmp_path / "log"), b"one\n", durable=True, mode=0o600)
    # The file, and its directory because this call created it.
    assert len(calls) == 2
    append_file(loc(tmp_path / "log"), b"two\n", durable=True)
    assert len(calls) == 3
    append_file(loc(tmp_path / "log"), b"three\n", durable=False)
    assert len(calls) == 3
    assert (tmp_path / "log").read_bytes() == b"one\ntwo\nthree\n"
    assert (tmp_path / "log").stat().st_mode & 0o777 == 0o600


def test_append_file_refuses_symlink(tmp_path: Path) -> None:
    (tmp_path / "target").write_text("precious")
    (tmp_path / "link").symlink_to("target")
    (tmp_path / "dangling").symlink_to("nowhere")
    for name in ("link", "dangling"):
        with pytest.raises(UnsafePath):
            append_file(loc(tmp_path / name), b"x", durable=False)
    assert (tmp_path / "target").read_text() == "precious"
    assert not (tmp_path / "nowhere").exists()


def test_append_file_anchored(tmp_path: Path) -> None:
    descriptor = open_dir(tmp_path)
    try:
        append_file(anchored(descriptor, "log"), b"a", durable=True)
        append_file(anchored(descriptor, "log"), b"b", durable=True)
    finally:
        os.close(descriptor)
    assert (tmp_path / "log").read_bytes() == b"ab"


# read_bounded


def test_read_bounded(tmp_path: Path) -> None:
    (tmp_path / "f").write_bytes(b"12345")
    assert read_bounded(loc(tmp_path / "f"), 5) == b"12345"
    assert read_bounded(loc(tmp_path / "absent"), 5) is None
    with pytest.raises(TooLarge):
        read_bounded(loc(tmp_path / "f"), 4)
    descriptor = open_dir(tmp_path)
    try:
        assert read_bounded(anchored(descriptor, "f"), 10) == b"12345"
    finally:
        os.close(descriptor)


def test_read_bounded_refuses_non_regular(tmp_path: Path) -> None:
    (tmp_path / "f").write_text("x")
    (tmp_path / "link").symlink_to("f")
    (tmp_path / "dir").mkdir()
    os.mkfifo(tmp_path / "fifo")
    for name in ("link", "dir", "fifo"):
        with pytest.raises(UnsafePath):
            read_bounded(loc(tmp_path / name), 10, nonblock=True)
    with pytest.raises(UnsafePath):
        read_bounded(loc(tmp_path / "link"), 10)
    with pytest.raises(NotADirectoryError):
        read_bounded(loc(tmp_path / "f" / "below"), 10)


# discard and remove_empty_dir


def test_discard_removes_tree_without_following_symlinks(tmp_path: Path) -> None:
    outside = _tree(tmp_path / "outside", "precious")
    target = tmp_path / "job"
    (target / "a" / "b" / "c").mkdir(parents=True)
    (target / "a" / "b" / "c" / "f").write_text("x")
    (target / "a" / "g").write_text("y")
    (target / "a" / "dirlink").symlink_to(outside)
    (target / "filelink").symlink_to(outside / "precious")
    (tmp_path / "trash").mkdir()
    discard(loc(target), trash_dir=tmp_path / "trash", durable=True)
    assert not target.exists()
    assert list((tmp_path / "trash").iterdir()) == []
    assert (outside / "precious").read_text() == "content"


def test_discard_single_file_and_absent(tmp_path: Path) -> None:
    (tmp_path / "f").write_text("x")
    discard(loc(tmp_path / "f"), trash_dir=tmp_path / "trash", durable=False)
    discard(loc(tmp_path / "absent"), trash_dir=tmp_path / "trash", durable=False)
    assert list((tmp_path / "trash").iterdir()) == []
    assert not (tmp_path / "f").exists()


def test_discard_tolerates_concurrent_removal_and_refill(tmp_path: Path) -> None:
    target = tmp_path / "job"
    (target / "sub").mkdir(parents=True)
    for name in ("a", "b", "c"):
        (target / "sub" / name).write_text(name)
    state = {"self": False, "removed": False, "refilled": False}

    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        if op != "remove" or src is None or src.at is None:
            return
        listing = os.listdir(src.at)
        if not state["self"]:
            # Another actor removes this very entry first: our unlink sees ENOENT.
            os.unlink(src.name(), dir_fd=src.at)
            state["self"] = True
        elif not state["removed"] and len(listing) > 1:
            # Another actor removes a sibling first: its unlink must see ENOENT and carry on.
            other = next(name for name in listing if name != src.name())
            os.unlink(other, dir_fd=src.at)
            state["removed"] = True
        elif src.name() == "sub" and not state["refilled"]:
            # The directory refills during the walk: rmdir sees ENOTEMPTY, the next pass removes it.
            sub = os.open("sub", os.O_RDONLY | os.O_DIRECTORY, dir_fd=src.at)
            os.close(os.open("late", os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=sub))
            os.close(sub)
            state["refilled"] = True

    set_fault_injector(fault)
    discard(loc(target), trash_dir=tmp_path / "trash", durable=False)
    assert state == {"self": True, "removed": True, "refilled": True}
    assert not target.exists()
    assert list((tmp_path / "trash").iterdir()) == []


def test_discard_gives_up_when_a_directory_keeps_refilling(tmp_path: Path) -> None:
    (tmp_path / "job" / "sub").mkdir(parents=True)

    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        if src is not None and src.at is not None and src.name() == "sub":
            sub = os.open("sub", os.O_RDONLY | os.O_DIRECTORY, dir_fd=src.at)
            os.close(os.open(fresh_token(), os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=sub))
            os.close(sub)

    set_fault_injector(fault)
    with pytest.raises(OSError) as raised:
        discard(loc(tmp_path / "job"), trash_dir=tmp_path / "trash", durable=False)
    assert raised.value.errno == errno.ENOTEMPTY


def test_discard_never_follows_a_directory_swapped_for_a_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outside = _tree(tmp_path / "outside", "precious")
    _tree(tmp_path / "job" / "swap")
    real_remove_leaf = _fs._remove_leaf
    swapped: list[int] = []

    def remove_leaf(at: int | None, name: str) -> bool:
        removed = real_remove_leaf(at, name)
        if name == "swap" and not removed and not swapped:
            # Between the lstat and the open, the directory becomes a symlink to precious data.
            os.rename("swap", tmp_path / "aside", src_dir_fd=at)
            os.symlink(outside, "swap", dir_fd=at)
            swapped.append(1)
        return removed

    monkeypatch.setattr(_fs, "_remove_leaf", remove_leaf)
    discard(loc(tmp_path / "job"), trash_dir=tmp_path / "trash", durable=False)
    assert swapped == [1]
    assert (outside / "precious").read_text() == "content"
    assert list((tmp_path / "trash").iterdir()) == []


def test_discard_raises_unexpected_rmdir_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "job" / "sub").mkdir(parents=True)

    def rmdir(path: object, **kwargs: object) -> None:
        raise PermissionError(errno.EPERM, "denied")

    monkeypatch.setattr(os, "rmdir", rmdir)
    with pytest.raises(PermissionError):
        discard(loc(tmp_path / "job"), trash_dir=tmp_path / "trash", durable=False)


def test_discard_refuses_anchored(tmp_path: Path) -> None:
    descriptor = open_dir(tmp_path)
    try:
        with pytest.raises(ValueError):
            discard(anchored(descriptor, "x"), trash_dir=tmp_path, durable=False)
    finally:
        os.close(descriptor)


def test_remove_empty_dir(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    _tree(tmp_path / "full")
    assert remove_empty_dir(loc(tmp_path / "empty"))
    assert not remove_empty_dir(loc(tmp_path / "full"))
    assert not remove_empty_dir(loc(tmp_path / "absent"))
    with pytest.raises(NotADirectoryError):
        remove_empty_dir(loc(tmp_path / "full" / "payload"))
    assert (tmp_path / "full" / "payload").exists()


# remove_file


def test_remove_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "f").write_text("x")
    (tmp_path / "target").write_text("precious")
    (tmp_path / "link").symlink_to("target")
    (tmp_path / "dir").mkdir()
    calls = _count_fsync(monkeypatch)
    remove_file(loc(tmp_path / "f"), durable=True)
    assert len(calls) == 1
    remove_file(loc(tmp_path / "f"), durable=False)
    remove_file(loc(tmp_path / "link"), durable=False)
    assert (tmp_path / "target").read_text() == "precious"
    with pytest.raises(IsADirectoryError):
        remove_file(loc(tmp_path / "dir"), durable=False)
    remove_file(loc(tmp_path / "pruned" / "f"), durable=True)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["dir", "target"]


def test_remove_file_anchored(tmp_path: Path) -> None:
    (tmp_path / "f").write_text("x")
    descriptor = open_dir(tmp_path)
    try:
        remove_file(anchored(descriptor, "f"), durable=True)
        remove_file(anchored(descriptor, "f"), durable=True)
    finally:
        os.close(descriptor)
    assert list(tmp_path.iterdir()) == []


# walk_untrusted


def test_walk_untrusted_lists_sorted_top_down(tmp_path: Path) -> None:
    root = tmp_path / "root"
    (root / "a" / "b").mkdir(parents=True)
    (root / "a" / "b" / "f").write_text("12")
    (root / "a.txt").write_text("123")
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "hidden").write_text("")
    (root / "z").symlink_to(tmp_path / "outside")
    assert walk_untrusted(root) == (
        Entry(PurePosixPath("a"), "dir", 0),
        Entry(PurePosixPath("a/b"), "dir", 0),
        Entry(PurePosixPath("a/b/f"), "file", 2),
        Entry(PurePosixPath("a.txt"), "file", 3),
        Entry(PurePosixPath("z"), "symlink", 0),
    )


def test_walk_untrusted_refusals(tmp_path: Path) -> None:
    def refused(root: Path, limits: WalkLimits = _fs.DEFAULT_LIMITS) -> UntrustedContentError:
        with pytest.raises(UntrustedContentError) as raised:
            walk_untrusted(root, limits=limits)
        return raised.value

    fifo = tmp_path / "fifo"
    (fifo / "d").mkdir(parents=True)
    os.mkfifo(fifo / "d" / "pipe")
    assert refused(fifo).relative_path == PurePosixPath("d/pipe")

    linked = _tree(tmp_path / "linked", "f")
    os.link(linked / "f", tmp_path / "second-name")
    assert refused(linked).relative_path == PurePosixPath("f")

    deep = tmp_path / "deep"
    (deep / "a" / "b" / "c").mkdir(parents=True)
    assert walk_untrusted(deep, limits=WalkLimits(depth=3))
    assert refused(deep, WalkLimits(depth=2)).relative_path == PurePosixPath("a/b/c")

    many = tmp_path / "many"
    many.mkdir()
    for name in "abc":
        (many / name).write_text("")
    assert len(walk_untrusted(many, limits=WalkLimits(entries=3))) == 3
    refused(many, WalkLimits(entries=2))

    (tmp_path / "rootlink").symlink_to(many)
    assert refused(tmp_path / "rootlink").relative_path == PurePosixPath(".")
    assert "not a directory" in refused(many / "a").reason


# rename_probe and fresh_token


def test_rename_probe_passes_and_cleans_up(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    rename_probe(tmp_path / "a", tmp_path / "b")
    rename_probe(tmp_path / "a", tmp_path / "a")
    assert list((tmp_path / "a").iterdir()) == []
    assert list((tmp_path / "b").iterdir()) == []


def test_rename_probe_detects_copying_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def rename(source: object, destination: object, **kwargs: object) -> None:
        shutil.copytree(str(source), str(destination))
        shutil.rmtree(str(source))

    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(FilesystemUnsupported, match="inode"):
        rename_probe(tmp_path, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_rename_probe_cross_device(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rename, _ = _fail(errno.EXDEV)
    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(FilesystemUnsupported):
        rename_probe(tmp_path, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_rename_probe_detects_replacing_directory_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def rename(source: object, destination: object, **kwargs: object) -> None:
        if Path(str(destination)).exists():
            shutil.rmtree(Path(str(destination)))
        REAL_RENAME(source, destination, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", rename)
    with pytest.raises(FilesystemUnsupported, match="onto a non-empty"):
        rename_probe(tmp_path, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_fresh_token() -> None:
    tokens = {fresh_token() for _ in range(1000)}
    assert len(tokens) == 1000
    assert all(re.fullmatch(r"[a-z2-7]{16}", token) for token in tokens)


# Fault injection


def test_fault_injector_points(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []
    set_fault_injector(lambda op, phase, src, dst: calls.append((op, phase)))

    _tree(tmp_path / "src")
    move_once(loc(tmp_path / "src"), loc(tmp_path / "dst"), durable=False)
    assert calls == [("rename", "before"), ("rename", "after")]

    calls.clear()
    write_file(loc(tmp_path / "f"), b"x", durable=False)
    assert calls == [("write", "before_replace")]

    calls.clear()
    _staging(tmp_path / "s1", b"1")
    _staging(tmp_path / "s2", b"2")
    publish_dir(loc(tmp_path / "s1"), loc(tmp_path / "p"), nonce=b"1", durable=False)
    assert calls == [("rename", "before"), ("rename", "after"), ("publish", "after")]
    calls.clear()
    publish_dir(loc(tmp_path / "s2"), loc(tmp_path / "p"), nonce=b"2", durable=False)
    # The loser's staging: its .nonce, then the directory itself.
    assert calls == [("rename", "before"), ("rename", "after"), ("publish", "after")] + [("remove", "before")] * 2

    calls.clear()
    discard(loc(tmp_path / "dst"), trash_dir=tmp_path / "trash", durable=False)
    assert calls == [("rename", "before"), ("rename", "after"), ("remove", "before"), ("remove", "before")]

    calls.clear()
    (tmp_path / "empty").mkdir()
    remove_empty_dir(loc(tmp_path / "empty"))
    remove_file(loc(tmp_path / "empty"), durable=False)
    append_file(loc(tmp_path / "log"), b"x", durable=False)
    read_bounded(loc(tmp_path / "f"), 10)
    assert exists(loc(tmp_path / "f"))
    os.close(create_exclusive(loc(tmp_path / "g"), durable=False))
    assert calls == [("remove", "before"), ("remove", "before")]


def test_fault_injector_crash_before_rename_leaves_src(tmp_path: Path) -> None:
    def crash(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        raise SystemExit(op + "/" + phase)

    _tree(tmp_path / "src")
    set_fault_injector(crash)
    with pytest.raises(SystemExit):
        move_once(loc(tmp_path / "src"), loc(tmp_path / "dst"), durable=False)
    assert (tmp_path / "src").exists()
    assert not (tmp_path / "dst").exists()
    with pytest.raises(SystemExit):
        write_file(loc(tmp_path / "f"), b"x", durable=False)
    # A simulated crash leaves the temporary behind for its owner's reconcile, as a real crash would.
    assert [path.name.endswith(".tmp") for path in tmp_path.iterdir() if path.name != "src"] == [True]
