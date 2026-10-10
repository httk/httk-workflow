"""The kernel and ``_fs`` surface added for moving: ``submit(priority=)``, ``Owner.discard_scratch``, ``copy_tree``."""

import json
import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _fs
from httk.workflow._kernel import Owner, claim, register_owner, submit


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@pytest.fixture(params=[False, True], ids=["fast", "durable"])
def ws(tmp_path: Path, request: pytest.FixtureRequest) -> FakeWorkspace:
    root = tmp_path / "ws"
    return FakeWorkspace(root, root / ".httk-workspace", root / "jobs", request.param, 0.05)


def new_owner(ws: FakeWorkspace) -> Owner:
    return register_owner(ws, kind="cli", label=None, allocation=None, advertised={})


def staged(owner: Owner, priority: int = 500) -> Path:
    staging = owner.scratch("build") / "job"
    staging.mkdir()
    document = {"format": "httk-workflow-job", "format_version": 3, "id": str(uuid.uuid4()), "tag": "t"}
    (staging / "job.json").write_text(json.dumps(document | {"placement": "p", "priority": priority}))
    return staging


def test_submit_takes_an_explicit_priority(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    assert submit(ws, owner, staged(owner, 500)).priority == 500
    ref = submit(ws, owner, staged(owner, 500), state="paused", priority=7)
    assert (ref.priority, ref.state) == (7, "paused") and ref.path.name.split("~")[1] == "p007"
    job = claim(ws, owner, ref)
    assert job is not None and job.from_priority == 7
    job.give_back()
    with pytest.raises(ValueError):
        submit(ws, owner, staged(owner), priority=1000)
    owner.close()


def test_discard_scratch_only_discards_own_scratch(ws: FakeWorkspace) -> None:
    owner, other = new_owner(ws), new_owner(ws)
    scratch = owner.scratch("eject")
    (scratch / "deep" / "er").mkdir(parents=True)
    (scratch / "deep" / "er" / "file").write_text("x")
    foreign = other.scratch("eject")
    for path in (foreign, scratch / "deep", ws.root, ws.control / "tmp" / f"{owner.owner_id}.Bad.x"):
        with pytest.raises(ValueError):
            owner.discard_scratch(path)
    owner.discard_scratch(scratch)
    assert not scratch.exists() and foreign.exists()
    owner.discard_scratch(scratch)  # absent: already discarded
    owner.close()
    other.close()


def test_copy_tree(tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "a" / "b").mkdir(parents=True)
    (source / "a" / "b" / "file").write_bytes(b"data")
    (source / "link").symlink_to("a/b/file")
    for durable in (False, True):
        target = tmp_path / f"out-{durable}" / "copy"
        _fs.copy_tree(source, target, durable=durable)
        assert (target / "a" / "b" / "file").read_bytes() == b"data"
        assert os.readlink(target / "link") == "a/b/file"
        with pytest.raises(FileExistsError):
            _fs.copy_tree(source, target, durable=durable)
    os.mkfifo(source / "pipe")
    with pytest.raises(_fs.UnsafePath):
        _fs.copy_tree(source, tmp_path / "fifo", durable=False)


def test_remove_write_temporaries(tmp_path: Path) -> None:
    (tmp_path / ".job.json.abcdefghijklmnop.tmp").write_text("x")
    (tmp_path / ".job.json.ABCDEFGHIJKLMNOP.tmp").write_text("x")
    (tmp_path / ".other.json.abcdefghijklmnop.tmp").write_text("x")
    (tmp_path / ".job.json.qrstuvwxyz234567.tmp").mkdir()
    assert _fs.remove_write_temporaries(tmp_path, "job.json", durable=False) == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        ".job.json.ABCDEFGHIJKLMNOP.tmp",
        ".job.json.qrstuvwxyz234567.tmp",
        ".other.json.abcdefghijklmnop.tmp",
    ]


def test_copy_tree_keeps_modes_and_times_and_reads_anchored(tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "bin").mkdir(parents=True)
    (source / "bin" / "run").write_bytes(b"#!/bin/sh\n")
    (source / "bin" / "run").chmod(0o750)
    os.utime(source / "bin" / "run", ns=(1_000_000_000, 2_000_000_000))
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        _fs.copy_tree(_fs.anchored(descriptor, "src"), tmp_path / "copy", durable=False)
    finally:
        os.close(descriptor)
    copied = (tmp_path / "copy" / "bin" / "run").stat()
    assert stat.S_IMODE(copied.st_mode) == 0o750 and copied.st_mtime_ns == 2_000_000_000
    (tmp_path / "link").symlink_to(source, target_is_directory=True)
    with pytest.raises(_fs.UnsafePath):
        _fs.copy_tree(tmp_path / "link", tmp_path / "via-link", durable=False)


def test_copy_tree_with_limits_applies_the_untrusted_walk_rules(tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "a" / "b").mkdir(parents=True)
    (source / "a" / "f").write_bytes(b"x")
    limits = _fs.WalkLimits(entries=3, depth=2)
    _fs.copy_tree(source, tmp_path / "ok", durable=False, limits=limits)
    with pytest.raises(_fs.UntrustedContentError, match="deeper"):
        _fs.copy_tree(source, tmp_path / "deep", durable=False, limits=_fs.WalkLimits(depth=1))
    with pytest.raises(_fs.UntrustedContentError, match="entries"):
        _fs.copy_tree(source, tmp_path / "many", durable=False, limits=_fs.WalkLimits(entries=2))
    os.link(source / "a" / "f", source / "hard")
    with pytest.raises(_fs.UntrustedContentError, match="hard-linked"):
        _fs.copy_tree(source, tmp_path / "linked", durable=False, limits=_fs.DEFAULT_LIMITS)
    _fs.copy_tree(source, tmp_path / "trusted", durable=False)  # a trusted copy breaks the link instead
    assert (tmp_path / "trusted" / "hard").stat().st_nlink == 1


def test_publish_record_creates_a_record_at_most_once(tmp_path: Path) -> None:
    assert _fs.publish_record(tmp_path, "r", b"first", nonce=b"one", durable=True)
    assert not _fs.publish_record(tmp_path, "r", b"second", nonce=b"two", durable=False)
    assert (tmp_path / "r" / "record").read_bytes() == b"first"
    assert sorted(os.listdir(tmp_path)) == ["r"]  # no staging left by the loser
    with pytest.raises(FileNotFoundError):
        _fs.publish_record(tmp_path / "missing", "r", b"x", nonce=b"three", durable=False)
    with pytest.raises(ValueError):
        _fs.publish_record(tmp_path, "../r", b"x", nonce=b"four", durable=False)


def test_open_dir_under_an_open_descriptor_and_lstat(tmp_path: Path) -> None:
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "link").symlink_to(tmp_path / "a")
    root = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.close(_fs.open_dir_under(root, "a/b"))
        os.close(_fs.open_dir_under(root, "a/c", create=True))
        with pytest.raises(_fs.UnsafePath):
            _fs.open_dir_under(root, "link/b")
        os.fstat(root)  # the root descriptor stays open
    finally:
        os.close(root)
    assert (tmp_path / "a" / "c").is_dir()
    assert _fs.lstat(_fs.loc(tmp_path / "missing")) is None
    info = _fs.lstat(_fs.loc(tmp_path / "link"))
    assert info is not None and stat.S_ISLNK(info.st_mode)
