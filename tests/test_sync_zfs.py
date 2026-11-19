"""``syncfs`` on gated ZFS, and the barrier's device coverage (plan WORKFLOW_LOCAL_COMMANDS L2, §4 amendments 4-5).

1. Under ``io.sync auto`` ZFS gives ``syncfs`` only on Linux 5.17 or later with an OpenZFS module of at least 2.4.0
   (``/sys/module/zfs/version``, a downstream suffix ignored); unknown, unreadable or older gives ``fsync``. A
   forced ``syncfs`` is unchanged. The prime still flushes on ZFS.
2. On ZFS a ``syncfs`` barrier always fails on a commit error (``io.sync_errors ignore`` does not apply, and job
   creation does not suggest it), and a tree written before httk ran is synced file by file under ``auto``.
3. A ``syncfs`` covers only the barrier's filesystem: what is mounted inside a tree written before its barrier
   (found in the mount table, or by listing the tree when it cannot be read), directory or file, is fsynced file by
   file; :func:`~httk.workflow._fs._off_device` groups the directories a barrier does not cover by filesystem.
"""

import errno
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import test_deferred_commit as deferred
from httk.workflow import Workspace, _fs
from httk.workflow import scaffold as scaffold_module
from test_deferred_commit import far_session, staged  # noqa: F401 (fixtures)
from test_deliver import hold_jobs
from test_job_transfer import jobs, outcomes, outgoing, run, selected
from test_moving import assert_in, find
from test_session_pull import durable, receptions, ssh  # noqa: F401 (ssh: fixture)
from test_staged_settle import cluster  # noqa: F401 (fixture)
from test_sync_errors_retry import Syncfs

WORKERS = pytest.mark.parametrize("workers", [1, 8], ids=["sequential", "pooled"])
TABLE = pytest.mark.parametrize("table", [True, False], ids=["mountinfo", "listing"])


@pytest.fixture(autouse=True)
def _fresh_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts with an empty per-process cache of probe results."""

    monkeypatch.setattr(_fs, "_syncfs_works", {})


def kernel(monkeypatch: pytest.MonkeyPatch, release: str) -> None:
    monkeypatch.setattr(_fs, "_uname", lambda: ("Linux", release))


def zfs_version(monkeypatch: pytest.MonkeyPatch, text: str | None) -> None:
    monkeypatch.setattr(_fs, "_read_zfs_version", lambda: text)


def mounted(monkeypatch: pytest.MonkeyPatch, path: Path, kind: str) -> None:
    """A fake ``/proc/self/mountinfo`` naming *path*'s filesystem *kind*."""

    device = os.stat(path).st_dev
    line = f"20 1 {os.major(device)}:{os.minor(device)} / / rw,relatime shared:1 - {kind} source rw\n"
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: line)


# -- the gate -----------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("release", "version", "expected"),
    [
        ("7.0.0-42-generic", "2.4.1\n", ("syncfs", "syncfs (zfs 2.4.1, Linux 7.0)")),
        ("7.0.0-42-generic", "2.4.1-1ubuntu5.1\n", ("syncfs", "syncfs (zfs 2.4.1-1ubuntu5.1, Linux 7.0)")),
        ("5.15.0-125-generic", "2.4.1", ("fsync", "fsync (zfs on Linux 5.15 is below 5.17)")),
        ("5.16.20", "2.4.1", ("fsync", "fsync (zfs on Linux 5.16 is below 5.17)")),
        ("5.17.0", "2.4.1", ("syncfs", "syncfs (zfs 2.4.1, Linux 5.17)")),
        ("6.8.0-45-generic", "2.4.0", ("syncfs", "syncfs (zfs 2.4.0, Linux 6.8)")),
        ("6.8.0-45-generic", "2.4.0-rc1", ("syncfs", "syncfs (zfs 2.4.0-rc1, Linux 6.8)")),
        ("6.8.0-45-generic", "2.10.0", ("syncfs", "syncfs (zfs 2.10.0, Linux 6.8)")),
        ("6.8.0-45-generic", "3.0.0", ("syncfs", "syncfs (zfs 3.0.0, Linux 6.8)")),
        ("6.8.0-45-generic", "2.2.2-0ubuntu9.1", ("fsync", "fsync (zfs 2.2.2-0ubuntu9.1 is below 2.4.0)")),
        ("6.8.0-45-generic", "2.1.16", ("fsync", "fsync (zfs 2.1.16 is below 2.4.0)")),
        ("6.8.0-45-generic", "2.3.0", ("fsync", "fsync (zfs 2.3.0 is below 2.4.0)")),
        ("6.8.0-45-generic", "2.3.4", ("fsync", "fsync (zfs 2.3.4 is below 2.4.0)")),
        ("6.8.0-45-generic", "2.3.99", ("fsync", "fsync (zfs 2.3.99 is below 2.4.0)")),
        ("6.8.0-45-generic", None, ("fsync", "fsync (the zfs version is unknown)")),
        ("6.8.0-45-generic", "  \n", ("fsync", "fsync (the zfs version is unknown)")),
        ("6.8.0-45-generic", "garbage", ("fsync", "fsync (the zfs version 'garbage' is not recognized)")),
        ("6.8.0-45-generic", "2.4", ("fsync", "fsync (the zfs version '2.4' is not recognized)")),
        ("6.8.0-45-generic", "v2.4.1", ("fsync", "fsync (the zfs version 'v2.4.1' is not recognized)")),
        ("6.8.0-45-generic", "2.4.1234567890", ("fsync", "fsync (the zfs version '2.4.1234567890' is not recognized)")),
        ("4.18.0-553.el8", "2.4.1", ("fsync", "fsync (Linux 4.18 is older than 5.8)")),
    ],
)
def test_auto_trusts_syncfs_on_zfs_only_past_its_kernel_and_version_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    release: str,
    version: str | None,
    expected: tuple[str, str],
) -> None:
    kernel(monkeypatch, release)
    zfs_version(monkeypatch, version)
    mounted(monkeypatch, tmp_path, "zfs")
    with caplog.at_level(logging.WARNING, logger="httk.workflow._fs"):
        assert _fs.resolve_sync(tmp_path, "auto") == expected
    assert caplog.records == []  # auto falls back quietly
    # A forced syncfs is as before: the gate is auto's alone.
    linux = _fs._kernel_version(release)
    assert linux is not None
    if linux >= (5, 8):
        assert _fs.resolve_sync(tmp_path, "syncfs") == ("syncfs", f"syncfs (forced, zfs, Linux {linux[0]}.{linux[1]})")
    else:
        assert _fs.resolve_sync(tmp_path, "syncfs") == expected
    assert _fs.resolve_sync(tmp_path, "fsync") == ("fsync", "fsync (io.sync is fsync)")


def test_the_gate_is_zfs_s_alone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "5.8.0")
    zfs_version(monkeypatch, None)
    mounted(monkeypatch, tmp_path, "ext4")
    assert _fs.resolve_sync(tmp_path, "auto") == ("syncfs", "syncfs (ext4, Linux 5.8)")
    mounted(monkeypatch, tmp_path, "zfs_member")
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (zfs_member is not vetted for auto)")


def test_the_version_is_read_afresh_from_the_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "7.0.0-42-generic")
    mounted(monkeypatch, tmp_path, "zfs")
    version = tmp_path / "version"
    monkeypatch.setattr(_fs, "_ZFS_VERSION", version)
    assert _fs._read_zfs_version() is None
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (the zfs version is unknown)")
    version.write_text("2.4.1-1ubuntu5.1\n")
    assert _fs.resolve_sync(tmp_path, "auto") == ("syncfs", "syncfs (zfs 2.4.1-1ubuntu5.1, Linux 7.0)")
    version.write_text("2.2.2-0ubuntu9.4\n")  # a module reloaded: nothing is cached
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (zfs 2.2.2-0ubuntu9.4 is below 2.4.0)")


@pytest.mark.parametrize(
    ("text", "parsed"),
    [
        ("2.4.1-1ubuntu5.1", (2, 4, 1)),
        ("2.4.1", (2, 4, 1)),
        ("2.4.0-rc1", (2, 4, 0)),
        ("10.20.30.40", (10, 20, 30)),
        ("2.4", None),
        ("zfs-2.4.1", None),
        ("", None),
    ],
)
def test_the_version_parser_takes_the_numeric_prefix(text: str, parsed: tuple[int, int, int] | None) -> None:
    assert _fs._zfs_version(text) == parsed


def test_the_prime_still_flushes_on_gated_zfs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, "2.4.1-1ubuntu5.1")
    mounted(monkeypatch, tmp_path, "zfs")
    calls: list[int] = []
    monkeypatch.setattr(_fs, "_syncfs", calls.append)
    with _fs.Barrier(tmp_path, "auto", quiet=True) as barrier:
        assert (barrier.strategy, barrier.primed, barrier.device) == ("syncfs", True, os.stat(tmp_path).st_dev)
        assert len(calls) == 1  # the prime: one syncfs before anything lands
        barrier.commit()
    assert len(calls) == 2 and calls[0] == calls[1]


def test_a_suspended_pool_fails_the_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # OpenZFS 2.4's sync_fs answers a suspended pool with EIO (ZIL_COMMIT_NOW), where fsync under failmode=wait
    # blocks: the commit fails and nothing is published.
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, "2.4.1")
    mounted(monkeypatch, tmp_path, "zfs")
    outcomes = [None, errno.EIO]

    def syncfs(_descriptor: int) -> None:
        failure = outcomes.pop(0)
        if failure is not None:
            raise OSError(failure, os.strerror(failure))

    monkeypatch.setattr(_fs, "_syncfs", syncfs)
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow._fs"),
        _fs.Barrier(tmp_path, "auto", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed, match="not durable"),
    ):
        barrier.commit()
    assert [record.getMessage() for record in caplog.records] == [
        f"the sync barrier (syncfs (zfs 2.4.1, Linux 7.0)) of {tmp_path} failed: [Errno 5] Input/output error"
    ]


def failing_syncfs(monkeypatch: pytest.MonkeyPatch, outcomes: list[int | None]) -> list[int]:
    """``_fs._syncfs`` answering each call with the next of *outcomes* (an ``errno``, or ``None`` for success)."""

    calls: list[int] = []

    def syncfs(descriptor: int) -> None:
        calls.append(descriptor)
        failure = outcomes.pop(0) if outcomes else None
        if failure is not None:
            raise OSError(failure, os.strerror(failure))

    monkeypatch.setattr(_fs, "_syncfs", syncfs)
    return calls


@pytest.mark.parametrize(("setting", "version"), [("auto", "2.4.1"), ("syncfs", "2.2.2")], ids=["gated", "forced"])
def test_io_sync_errors_ignore_does_not_apply_on_zfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, setting: str, version: str
) -> None:
    # The EIO of a ZFS syncfs is this commit's own failure (its own commit of the intent log), never someone else's.
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, version)
    mounted(monkeypatch, tmp_path, "zfs")
    calls = failing_syncfs(monkeypatch, [None, errno.EIO])
    with (
        _fs.Barrier(tmp_path, setting, errors="ignore", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed, match="not durable"),
    ):
        assert (barrier.strategy, barrier.filesystem, barrier.errors) == ("syncfs", "zfs", "fail")
        barrier.commit()
    assert len(calls) == 2  # no second syncfs of the ignore path


def test_io_sync_errors_ignore_still_applies_elsewhere(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "7.0.0-42-generic")
    mounted(monkeypatch, tmp_path, "ext4")
    calls = failing_syncfs(monkeypatch, [None, errno.EIO, None])
    with _fs.Barrier(tmp_path, "auto", errors="ignore", quiet=True) as barrier:
        assert (barrier.filesystem, barrier.errors) == ("ext4", "ignore")
        barrier.commit()
    assert len(calls) == 3


@pytest.mark.parametrize(("kind", "offered"), [("zfs", False), ("ext4", True)])
def test_job_creation_offers_io_sync_errors_ignore_only_off_zfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, offered: bool
) -> None:
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, "2.4.1")
    mounted(monkeypatch, tmp_path, kind)
    failing_syncfs(monkeypatch, [None, errno.EIO])
    with (
        _fs.Barrier(tmp_path, "auto", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed) as raised,
    ):
        scaffold_module._commit_staged(barrier, "the job x")
    assert "nothing was published" in str(raised.value)
    assert ("io.sync_errors ignore" in str(raised.value)) is offered


def test_a_tree_written_before_httk_ran_is_walked_file_by_file_on_zfs_under_auto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A ZFS syncfs does not report a failed writeback of mmap-dirtied pages; fsync does.
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, "2.4.1")
    mounted(monkeypatch, tmp_path, "zfs")
    tree = tmp_path / "bundle"
    (tree / "a").mkdir(parents=True)
    (tree / "a" / "file").write_bytes(b"1")
    (tree / "top").write_bytes(b"2")
    calls = failing_syncfs(monkeypatch, [])
    synced = fsync_spy(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "auto") == [tree]
    assert calls == []
    expected = [tree / "a" / "file", tree / "top", tree / "a", tree]
    assert sorted(synced) == sorted(inode(path) for path in expected)
    with _fs.Barrier(tmp_path, "auto", prime=False, written=True, quiet=True) as barrier:
        assert (barrier.strategy, barrier.reason) == ("fsync", "fsync (zfs, for a tree written before httk ran)")
    # Forcing syncfs is as before L2; a barrier entered before httk writes keeps syncfs.
    synced.clear()
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]
    assert (len(calls), synced) == (1, [])
    with _fs.Barrier(tmp_path, "auto", quiet=True) as barrier:
        assert barrier.strategy == "syncfs"


# -- device coverage --------------------------------------------------------------------------------------------------


def shifted(info: os.stat_result) -> os.stat_result:
    """*info* on another device."""

    return os.stat_result(
        (
            info.st_mode,
            info.st_ino,
            info.st_dev + 1,
            info.st_nlink,
            info.st_uid,
            info.st_gid,
            info.st_size,
            int(info.st_atime),
            int(info.st_mtime),
            int(info.st_ctime),
        )
    )


def foreign(monkeypatch: pytest.MonkeyPatch, *entries: Path, table: bool = True) -> None:
    """Make *entries* look like mounts of another filesystem: ``os.lstat`` and ``os.fstat`` report another
    ``st_dev`` for them, and the mount table lists them (with *table*) or cannot be read (without)."""

    inodes = {os.lstat(entry).st_ino for entry in entries}
    real_fstat, real_lstat = os.fstat, os.lstat

    def fstat(descriptor: int) -> os.stat_result:
        info = real_fstat(descriptor)
        return shifted(info) if info.st_ino in inodes else info

    def lstat(*args: Any, **kwargs: Any) -> os.stat_result:
        info = real_lstat(*args, **kwargs)
        return shifted(info) if info.st_ino in inodes else info

    monkeypatch.setattr(os, "fstat", fstat)
    monkeypatch.setattr(os, "lstat", lstat)
    if table:
        here = _fs._device_name(real_lstat(entries[0]).st_dev)
        lines = [f"30 1 {here} / / rw - ext4 none rw"] + [
            f"{40 + index} 1 {_fs._device_name(real_lstat(entry).st_dev + 1)} / {entry} rw - tmpfs none rw"
            for index, entry in enumerate(entries)
        ]
        monkeypatch.setattr(_fs, "_read_mountinfo", lambda: "\n".join(lines) + "\n")
    else:
        monkeypatch.setattr(_fs, "_read_mountinfo", lambda: None)


def mount_table(monkeypatch: pytest.MonkeyPatch, root: Path, *entries: tuple[str, Path]) -> None:
    """A mount table: the filesystem of *root* mounted at ``/``, then ``(major:minor, mount point)`` entries."""

    lines = [f"30 1 {_fs._device_name(os.stat(root).st_dev)} / / rw - ext4 none rw"] + [
        f"{40 + index} 1 {number} / {point} rw - tmpfs none rw" for index, (number, point) in enumerate(entries)
    ]
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: "\n".join(lines) + "\n")


def listings(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The roots of every listing walk (the fallback when the mount table proves nothing)."""

    roots: list[int] = []
    real = _fs._walk_off_device

    def walk(root: int, device: int, **options: Any) -> int:
        roots.append(root)
        return real(root, device, **options)

    monkeypatch.setattr(_fs, "_walk_off_device", walk)
    return roots


def fsync_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The inode of every ``os.fsync``, in order."""

    synced: list[int] = []
    real_fsync, real_stat = os.fsync, os.fstat

    def spy(descriptor: int) -> None:
        synced.append(real_stat(descriptor).st_ino)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", spy)
    return synced


def inode(path: Path) -> int:
    return os.lstat(path).st_ino


def bundle(root: Path) -> dict[str, Path]:
    """A tree written before httk ran: ``mnt``, ``a/deep/mnt2`` and the file ``fmnt`` are the mounts tests fake."""

    paths = {
        "file": root / "file",
        "a": root / "a",
        "a/file": root / "a" / "file",
        "deep": root / "a" / "deep",
        "deep/file": root / "a" / "deep" / "file",
        "mnt": root / "mnt",
        "mnt/file": root / "mnt" / "file",
        "mnt/sub": root / "mnt" / "sub",
        "mnt/sub/file": root / "mnt" / "sub" / "file",
        "mnt2": root / "a" / "deep" / "mnt2",
        "mnt2/file": root / "a" / "deep" / "mnt2" / "file",
        "fmnt": root / "a" / "fmnt",
    }
    for name, path in paths.items():
        if name.endswith("file") or name == "fmnt":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(name.encode())
        else:
            path.mkdir(parents=True, exist_ok=True)
    (root / "a" / "link").symlink_to(paths["mnt"])
    return paths


#: What the faked mounts of :func:`bundle` hold.
MOUNTED = ["mnt/file", "mnt/sub/file", "mnt/sub", "mnt", "mnt2/file", "mnt2", "fmnt"]


def syncfs_calls(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    monkeypatch.setattr(_fs, "_syncfs", calls.append)
    return calls


@TABLE
@WORKERS
def test_what_is_mounted_inside_a_tree_is_fsynced_after_the_syncfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int, table: bool
) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    foreign(monkeypatch, paths["mnt"], paths["mnt2"], paths["fmnt"], table=table)
    calls = syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    listed = listings(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "syncfs", workers=workers) == [tree]
    assert len(listed) == (0 if table else 1)
    assert len(calls) == 1  # one syncfs covers the tree's own filesystem
    # Every file and directory of the mounted directories, at any depth, the mounted file, and nothing else.
    assert sorted(synced) == sorted(inode(paths[name]) for name in MOUNTED)
    assert synced.index(inode(paths["mnt/sub/file"])) < synced.index(inode(paths["mnt/sub"]))
    assert synced.index(inode(paths["mnt/sub"])) < synced.index(inode(paths["mnt"]))


@TABLE
def test_every_tree_of_a_batch_is_covered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, table: bool) -> None:
    trees = [tmp_path / "one", tmp_path / "two", tmp_path / "three"]
    bundles = [bundle(tree) for tree in trees]
    foreign(monkeypatch, *(paths["fmnt"] for paths in bundles), bundles[2]["mnt"], table=table)
    calls = syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    listed = listings(monkeypatch)
    assert _fs.sync_written(tmp_path, trees, "syncfs") == trees
    assert len(listed) == (0 if table else 3)
    expected = [paths["fmnt"] for paths in bundles] + [bundles[2][name] for name in MOUNTED[:4]]
    assert (len(calls), sorted(synced)) == (1, sorted(inode(path) for path in expected))


def test_a_tree_wholly_on_the_barrier_s_filesystem_takes_one_syncfs_and_no_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trees = [tmp_path / "one", tmp_path / "two"]
    for tree in trees:
        bundle(tree)
    calls = syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    assert _fs.sync_written(tmp_path, trees, "syncfs") == trees
    assert (len(calls), synced) == (1, [])


def test_mounts_of_the_barrier_s_filesystem_outside_the_tree_or_out_of_reach_need_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    elsewhere = _fs._device_name(os.stat(tmp_path).st_dev + 1)
    (tmp_path / "outside").mkdir()
    mount_table(
        monkeypatch,
        tmp_path,
        (_fs._device_name(os.stat(tmp_path).st_dev), paths["mnt"]),  # a bind mount of the barrier's filesystem
        (elsewhere, paths["a"]),  # listed on another device, but what is reachable there is on ours
        (elsewhere, tmp_path / "outside"),  # not below the tree
        (elsewhere, tree),  # the tree itself: its own st_dev is checked
        (elsewhere, tree / "gone" / "mnt"),  # hidden by a later mount, or gone
        (elsewhere, tree / "a" / "link" / "file"),  # reached only through a symlink
    )
    calls = syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]
    assert (len(calls), synced) == (1, [])


def test_a_tree_root_replaced_by_another_filesystem_is_walked_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    root = inode(tree)
    real = os.fstat

    def fstat(descriptor: int) -> os.stat_result:
        info = real(descriptor)
        return shifted(info) if info.st_ino == root else info  # the opened root, not the lstat before it

    monkeypatch.setattr(os, "fstat", fstat)
    syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]
    regular = [path for name, path in paths.items() if name.endswith("file") or name == "fmnt"]
    directories = [path for name, path in paths.items() if not (name.endswith("file") or name == "fmnt")]
    assert sorted(synced) == sorted(inode(path) for path in [*regular, *directories, tree])


def test_a_mounted_subtree_of_an_anchored_tree_is_fsynced_too(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    foreign(monkeypatch, paths["mnt2"])
    syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    anchor = _fs.open_dir(tmp_path)
    try:
        located = _fs.anchored(anchor, "bundle")
        assert _fs.sync_written(tmp_path, [located], "syncfs") == [located]
    finally:
        os.close(anchor)
    assert sorted(synced) == sorted([inode(paths["mnt2/file"]), inode(paths["mnt2"])])


def test_under_fsync_the_walk_covers_what_is_mounted_as_any_other(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    foreign(monkeypatch, paths["mnt"], paths["mnt2"], paths["fmnt"])
    calls = syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "fsync") == [tree]
    assert calls == []
    expected = [name for name in paths if name.endswith("file") or name == "fmnt"] + [
        "a",
        "deep",
        "mnt",
        "mnt/sub",
        "mnt2",
    ]
    assert sorted(synced) == sorted([inode(paths[name]) for name in expected] + [inode(tree)])


def test_without_a_mount_table_a_directory_that_cannot_be_listed_fails_the_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Whether a filesystem is mounted below it cannot be known: as under fsync, the entry is named.
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    syncfs_calls(monkeypatch)
    hidden = inode(paths["deep"])
    real = os.listdir

    def listing(path: Any = ".") -> list[str]:
        if isinstance(path, int) and os.fstat(path).st_ino == hidden:
            raise PermissionError(errno.EACCES, "Permission denied")
        return real(path)

    monkeypatch.setattr(os, "listdir", listing)
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: None)
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow._fs"),
        pytest.raises(_fs.SyncUnreadable, match="a/deep") as raised,
    ):
        _fs.sync_written(tmp_path, [tree], "syncfs")
    assert raised.value.errno == errno.EACCES
    assert len(caplog.records) == 1 and "failed" in caplog.records[0].getMessage()
    # With the mount table, nothing needs listing.
    mount_table(monkeypatch, tmp_path)
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]


def test_a_mount_below_a_directory_that_cannot_be_searched_fails_the_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root searches a directory of mode 000")
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    mount_table(monkeypatch, tmp_path, (_fs._device_name(os.stat(tmp_path).st_dev + 1), paths["mnt2"]))
    syncfs_calls(monkeypatch)
    paths["deep"].chmod(0)
    try:
        with pytest.raises(_fs.SyncUnreadable, match="a/deep") as raised:
            _fs.sync_written(tmp_path, [tree], "syncfs")
    finally:
        paths["deep"].chmod(0o755)
    assert raised.value.errno == errno.EACCES


def test_without_a_mount_table_a_tree_deeper_than_256_levels_fails_without_leaking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "bundle"
    deep = tree
    for _ in range(300):
        deep = deep / "d"
    deep.mkdir(parents=True)
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: None)
    syncfs_calls(monkeypatch)
    before = len(os.listdir("/proc/self/fd"))
    with pytest.raises(_fs.SyncUnreadable, match="deeper than 256 levels"):
        _fs.sync_written(tmp_path, [tree], "syncfs")
    assert len(os.listdir("/proc/self/fd")) == before
    mount_table(monkeypatch, tmp_path)  # the mount table needs no walk, so no depth limit
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]


@pytest.mark.parametrize("which", ["mnt", "fmnt"])
def test_a_failed_fsync_of_a_mounted_entry_fails_the_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, which: str
) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    foreign(monkeypatch, paths[which])
    syncfs_calls(monkeypatch)

    def broken(_descriptor: int) -> None:
        raise OSError(errno.EIO, "injected")

    monkeypatch.setattr(os, "fsync", broken)
    with pytest.raises(_fs.SyncFailed, match=which) as raised:
        _fs.sync_written(tmp_path, [tree], "syncfs", errors="ignore")  # ignore trusts a syncfs error, not an fsync's
    assert raised.value.errno == errno.EIO and not isinstance(raised.value, _fs.SyncUnreadable)


_REAL_MOUNTS = """
import json, os, subprocess, sys
from pathlib import Path
from httk.workflow import _fs

base = Path(sys.argv[1])
tree, other = base / "bundle", base / "other"
for path in (tree / "a" / "mnt", tree / "plain", other):
    path.mkdir(parents=True)
(tree / "fmnt").write_bytes(b"placeholder")
subprocess.run(["mount", "-t", "tmpfs", "none", str(tree / "a" / "mnt")], check=True)
(tree / "a" / "mnt" / "sub").mkdir()
(tree / "a" / "mnt" / "sub" / "f2").write_bytes(b"2")
(tree / "a" / "mnt" / "f1").write_bytes(b"1")
subprocess.run(["mount", "-t", "tmpfs", "none", str(other)], check=True)
(other / "payload").write_bytes(b"on another filesystem")
subprocess.run(["mount", "--bind", str(other / "payload"), str(tree / "fmnt")], check=True)
(tree / "plain" / "f").write_bytes(b"on the barrier's")
synced = []
real = os.fsync

def spy(descriptor):
    synced.append(os.readlink(f"/proc/self/fd/{descriptor}").removeprefix(str(tree)))
    real(descriptor)

os.fsync = spy
_fs._syncfs = lambda descriptor: None
result = {}
for table in (True, False):
    if not table:
        _fs._read_mountinfo = lambda: None
    synced.clear()
    assert _fs.sync_written(base, [tree], "syncfs") == [tree]
    result[str(table)] = sorted(synced)
print(json.dumps(result))
"""


def test_real_mounts_inside_a_tree_are_fsynced(tmp_path: Path) -> None:
    # A real tmpfs mount and a real file bind mount, in a user and mount namespace of their own.
    if shutil.which("unshare") is None or subprocess.run(["unshare", "-Urm", "true"], check=False).returncode != 0:
        pytest.skip("no unprivileged user and mount namespaces here")
    script = tmp_path / "mounts.py"
    script.write_text(_REAL_MOUNTS)
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join(entry for entry in sys.path if entry)}
    run = subprocess.run(
        ["unshare", "-Urm", sys.executable, str(script), str(tmp_path / "base")],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    assert run.returncode == 0, run.stderr
    expected = sorted(["/a/mnt/f1", "/a/mnt/sub/f2", "/a/mnt/sub", "/a/mnt", "/fmnt"])
    assert json.loads(run.stdout) == {"True": expected, "False": expected}


# -- what a barrier covers --------------------------------------------------------------------------------------------


def test_off_device_groups_the_directories_a_barrier_does_not_cover(tmp_path: Path) -> None:
    (tmp_path / "here").mkdir()
    (tmp_path / "file").write_bytes(b"")
    proc, sys_device = os.stat("/proc").st_dev, os.stat("/sys").st_dev
    assert len({proc, sys_device, os.stat(tmp_path).st_dev}) == 3
    with _fs.Barrier(tmp_path, "fsync", quiet=True) as barrier:
        assert barrier.device == os.stat(tmp_path).st_dev
        anchor = _fs.open_dir(tmp_path)
        elsewhere = os.open("/proc", os.O_RDONLY | os.O_DIRECTORY)
        try:
            paths: list[Path | _fs.Loc | int] = [
                tmp_path / "here",
                Path("/proc"),
                tmp_path / "missing",
                _fs.anchored(anchor, "here"),
                _fs.anchored(anchor, "missing"),
                tmp_path / "file",
                Path("/sys"),
                anchor,
                elsewhere,
                tmp_path,
            ]
            assert _fs._off_device(paths, barrier) == {
                proc: [Path("/proc"), elsewhere],
                None: [tmp_path / "missing", paths[4], tmp_path / "file"],
                sys_device: [Path("/sys")],
            }
            covered: list[Path | int] = [tmp_path, anchor]
            assert _fs._off_device(covered, barrier) == {}
            assert _fs._off_device([], barrier) == {}
        finally:
            os.close(elsewhere)
            os.close(anchor)


def test_off_device_needs_an_entered_barrier(tmp_path: Path) -> None:
    barrier = _fs.Barrier(tmp_path, "fsync", quiet=True)
    with pytest.raises(ValueError, match="inside its context"):
        _fs._off_device([tmp_path], barrier)
    with barrier:
        assert _fs._off_device([tmp_path], barrier) == {}
    assert barrier.device is None
    with pytest.raises(ValueError, match="inside its context"):
        _fs._off_device([tmp_path], barrier)


def test_a_symlinked_anchor_and_symlinked_directories_are_followed_alike(tmp_path: Path) -> None:
    # Barrier opens its anchor through a symlink; _off_device examines paths the same way.
    (tmp_path / "anchor").symlink_to(tmp_path)
    (tmp_path / "proc").symlink_to("/proc")
    with _fs.Barrier(tmp_path / "anchor", "fsync", quiet=True) as barrier:
        assert barrier.device == os.stat(tmp_path).st_dev
        assert _fs._off_device([tmp_path / "anchor", tmp_path], barrier) == {}
        assert _fs._off_device([tmp_path / "proc"], barrier) == {os.stat("/proc").st_dev: [tmp_path / "proc"]}


@pytest.mark.parametrize(
    ("setting", "kind", "expected"),
    [
        ("auto", "ext4", ("syncfs", "syncfs (ext4, Linux 7.0)", "ext4")),
        ("auto", "zfs", ("fsync", "fsync (zfs 2.2.2 is below 2.4.0)", "zfs")),
        ("auto", "lustre", ("syncfs", "syncfs (lustre, Linux 7.0)", "lustre")),
        ("syncfs", "nfs4", ("syncfs", "syncfs (forced, nfs4, Linux 7.0)", "nfs4")),
        ("auto", "nfs4", ("fsync", "fsync (nfs4 is not vetted for auto)", "nfs4")),
        ("fsync", "ext4", ("fsync", "fsync (io.sync is fsync)", None)),
    ],
)
def test_the_private_resolution_also_names_the_filesystem_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, setting: str, kind: str, expected: tuple[str, str, str | None]
) -> None:
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, "2.2.2")
    mounted(monkeypatch, tmp_path, kind)
    assert _fs._resolve_sync(tmp_path, setting) == expected
    assert _fs.resolve_sync(tmp_path, setting) == expected[:2]


# -- when the mount table proves nothing ------------------------------------------------------------------------------


def moved_after_first_read(monkeypatch: pytest.MonkeyPatch, tree: Path) -> None:
    """``/proc/self/fd`` names *tree*'s descriptor by its path once, then by another path (moved meanwhile)."""

    real = os.readlink
    seen: list[str] = []

    def readlink(path: Any, *args: Any, **kwargs: Any) -> str:
        found = real(path, *args, **kwargs)
        if str(path).startswith("/proc/self/fd/") and found == str(tree):
            seen.append(found)
            if len(seen) > 1:
                return f"{found}-moved"
        return found

    monkeypatch.setattr(os, "readlink", readlink)


@pytest.mark.parametrize("uncertain", ["moved", "elsewhere", "deleted", "uncontained", "other-device", "malformed"])
def test_a_mount_table_that_cannot_be_proven_to_describe_the_tree_falls_back_to_listing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, uncertain: str
) -> None:
    # The table lists none of the tree's mounts (stale, or describing another tree): only the listing finds it.
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    (tmp_path / "other").mkdir()
    foreign(monkeypatch, paths["mnt"], paths["fmnt"])
    here = _fs._device_name(os.stat(tmp_path).st_dev)
    table = {
        "uncontained": f"30 1 {here} / /nowhere rw - ext4 none rw\n",
        "other-device": f"30 1 {_fs._device_name(os.stat(tmp_path).st_dev + 5)} / / rw - ext4 none rw\n",
        "malformed": f"30 1 {here} / / rw - ext4 none rw\n31 1 {here} / {tree}/x rw\n",
    }.get(uncertain, f"30 1 {here} / / rw - ext4 none rw\n")
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: table)
    if uncertain == "moved":
        moved_after_first_read(monkeypatch, tree)
    elif uncertain in ("elsewhere", "deleted"):
        real = os.readlink
        replacement = str(tmp_path / "other") if uncertain == "elsewhere" else f"{tree} (deleted)"

        def readlink(path: Any, *args: Any, **kwargs: Any) -> str:
            found = real(path, *args, **kwargs)
            return replacement if str(path).startswith("/proc/self/fd/") and found == str(tree) else found

        monkeypatch.setattr(os, "readlink", readlink)
    syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    listed = listings(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]
    assert len(listed) == 1
    assert sorted(synced) == sorted(
        inode(paths[name]) for name in ["mnt/file", "mnt/sub/file", "mnt/sub", "mnt", "fmnt"]
    )


def test_a_proven_table_is_used_without_listing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tree = tmp_path / "bundle"
    paths = bundle(tree)
    foreign(monkeypatch, paths["fmnt"])
    syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    listed = listings(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]
    assert (listed, synced) == ([], [inode(paths["fmnt"])])


@pytest.mark.parametrize("character", ["\v", "\r", " ", "\x1c", "\x85"], ids=["VT", "CR", "U+2028", "FS", "NEL"])
def test_a_mount_point_holding_a_line_breaking_character_is_read_from_the_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, character: str
) -> None:
    # The kernel escapes only space, tab, LF and backslash: anything else is literal in a mount point.
    tree = tmp_path / "bundle"
    bundle(tree)
    mounted_there = tree / f"a{character}b"
    mounted_there.mkdir()
    (mounted_there / "file").write_bytes(b"x")
    foreign(monkeypatch, mounted_there)
    syncfs_calls(monkeypatch)
    synced = fsync_spy(monkeypatch)
    listed = listings(monkeypatch)
    assert _fs.sync_written(tmp_path, [tree], "syncfs") == [tree]
    assert listed == []
    assert sorted(synced) == sorted([inode(mounted_there / "file"), inode(mounted_there)])


def test_the_mount_table_is_read_as_bytes_without_newline_translation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = tmp_path / "mountinfo"
    table.write_bytes(b"30 1 0:5 / / rw - ext4 none rw\n31 1 0:6 / /a\rb\xff rw - tmpfs none rw\n")
    monkeypatch.setattr(_fs, "_MOUNTINFO", table)
    text = _fs._read_mountinfo()
    assert text == "30 1 0:5 / / rw - ext4 none rw\n31 1 0:6 / /a\rb\udcff rw - tmpfs none rw\n"
    assert _fs._mount_entries(text) == [
        ("0:5", Path("/"), "ext4"),
        ("0:6", Path("/a\rb\udcff"), "tmpfs"),
    ]


@pytest.mark.parametrize(
    "record",
    ["31 1 0:6 / /a rw", "31 1 0:6 / /a rw - tmpfs", "31 1 six / /a rw - tmpfs none rw", "31 1 0:6 / a rw - x y z"],
)
def test_one_record_that_does_not_parse_makes_the_whole_table_uncertain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, record: str
) -> None:
    text = f"30 1 {_fs._device_name(os.stat(tmp_path).st_dev)} / / rw - ext4 none rw\n{record}\n"
    assert _fs._mount_entries(text) is None
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: text)
    assert _fs._filesystem_type(tmp_path, os.stat(tmp_path).st_dev) is None  # so auto gives fsync


# -- ZFS in a transfer: io.sync_errors ignore does not apply, and the fsync retry adopts ---------------------------------


def test_on_zfs_an_immediate_commit_failure_under_ignore_is_refused_then_an_fsync_stage_is_adopted(
    far_session: tuple[Path, Workspace, Workspace, str],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle_path, near, far, far_name = far_session
    far.configure_settings({"io.sync": "auto", "io.sync_errors": "ignore"})
    monkeypatch.setenv("HTTK_TEST_ZFS", "1")
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first stage's commit, after its prime
    holds, plan = hold_jobs(near, 1)
    with deferred.opened(bundle_path) as session:
        refused = deferred.stage(session, far_name, near, holds)
        assert refused["ok"] is False and refused["document"]["sync_failed"] is True and "token" not in refused
        assert find(far, plan[0][0]["id"]) is None  # nothing published
        retried = deferred.stage(session, far_name, near, holds, sync="fsync")
        assert retried["ok"] is True
        assert deferred.adopt(session, far_name, retried)["ok"] is True
    assert find(far, plan[0][0]["id"]) is not None


def test_on_zfs_a_deferred_commit_failure_under_ignore_is_retried_with_fsync_and_adopted(
    staged: deferred.Cluster,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = staged
    plan = deferred.deferred_push(staged, monkeypatch, "2", 1)  # the deferred commit of the one stage fails
    far.configure_settings({"io.sync": "auto", "io.sync_errors": "ignore"})
    monkeypatch.setenv("HTTK_TEST_ZFS", "1")
    requests = deferred.recorded(monkeypatch)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"]
    assert len(deferred.assert_retried_in_place(requests)) == 1  # refused (sync_failed), then delivered with fsync
    assert captured.err.count("could not make a staged segment durable") == 1
    deferred._once(far, plan)
    assert outgoing(near) == []


def test_on_zfs_a_pulled_landing_s_failed_commit_under_ignore_is_fetched_again_with_fsync(
    ssh: tuple[Path, Workspace, str, Workspace, str],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    near.configure_settings({"io.sync": "auto", "io.sync_errors": "ignore"})
    kernel(monkeypatch, "7.0.0-42-generic")
    zfs_version(monkeypatch, "2.4.1")
    monkeypatch.setattr(_fs, "_filesystem_type", lambda path, device: "zfs")
    Syncfs(monkeypatch, [None, errno.EIO, None])
    fsyncs = receptions(monkeypatch)
    plan = jobs(far, 2)
    assert durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    assert fsyncs == [False, True]  # refused, then fetched again with an fsync of every file
    assert_in(near, plan)
