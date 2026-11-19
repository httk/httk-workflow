"""The sync barrier of :mod:`httk.workflow._fs` (plan WORKFLOW_SYNC_BARRIER §8): strategy resolution, the barrier's
prime and commit, the late walk, and the durable mode of :func:`~httk.workflow._fs.extract_tar`."""

import errno
import io
import logging
import os
import stat
import sys
import tarfile
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import _fs

WORKERS = pytest.mark.parametrize("workers", [1, 8], ids=["sequential", "pooled"])


@pytest.fixture(autouse=True)
def _fresh_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts with an empty per-process cache of probe results, and no OpenZFS version is readable (so
    ``auto`` gives ``fsync`` on ZFS whatever this host runs; ``tests/test_sync_zfs.py`` covers the ZFS gate)."""

    monkeypatch.setattr(_fs, "_syncfs_works", {})
    monkeypatch.setattr(_fs, "_read_zfs_version", lambda: None)


def kernel(monkeypatch: pytest.MonkeyPatch, release: str, system: str = "Linux") -> None:
    monkeypatch.setattr(_fs, "_uname", lambda: (system, release))


def mounts(monkeypatch: pytest.MonkeyPatch, *entries: tuple[str, str, str]) -> None:
    """A fake ``/proc/self/mountinfo`` of ``(major:minor, mount point as written, type)`` entries."""

    lines = [
        f"{index + 20} 1 {device} / {point} rw,relatime shared:{index} - {kind} source rw"
        for index, (device, point, kind) in enumerate(entries)
    ]
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: "\n".join(lines) + "\n")


def device_of(path: Path) -> str:
    device = os.stat(path).st_dev
    return f"{os.major(device)}:{os.minor(device)}"


# -- the real libc call -----------------------------------------------------------------------------------------------


def test_the_real_syncfs_refuses_a_bad_descriptor_and_syncs_the_test_directory(tmp_path: Path) -> None:
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    os.close(descriptor)
    with pytest.raises(OSError) as raised:
        _fs._syncfs(descriptor)
    assert raised.value.errno == errno.EBADF
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        _fs._syncfs(descriptor)  # returns, raising nothing
    finally:
        os.close(descriptor)
    # Whatever the strategy here resolves to, a forced barrier on this directory commits.
    with _fs.Barrier(tmp_path, "syncfs", quiet=True) as barrier:
        (tmp_path / "file").write_bytes(b"x")
        barrier.commit()


def test_a_missing_libc_symbol_resolves_to_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, (device_of(tmp_path), "/", "ext4"))
    monkeypatch.setattr(_fs, "_syncfs_function", lambda: None)
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (libc has no syncfs)")
    with pytest.raises(OSError) as raised:
        _fs._syncfs(0)
    assert raised.value.errno == errno.ENOSYS


def test_a_python_without_ctypes_resolves_to_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, (device_of(tmp_path), "/", "ext4"))
    _fs._syncfs_function.cache_clear()
    try:
        with monkeypatch.context() as patch:
            patch.setitem(sys.modules, "ctypes", None)  # import ctypes raises ImportError
            assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (libc has no syncfs)")
            with _fs.Barrier(tmp_path, "syncfs", quiet=True) as barrier:
                assert barrier.strategy == "fsync"
    finally:
        _fs._syncfs_function.cache_clear()
    assert _fs._syncfs_function() is not None


# -- resolution -------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("release", "expected"),
    [
        ("4.18.0-553.el8", ("fsync", "fsync (Linux 4.18 is older than 5.8)")),
        ("5.7.0", ("fsync", "fsync (Linux 5.7 is older than 5.8)")),
        ("5.8.0", ("syncfs", "syncfs (ext4, Linux 5.8)")),
        ("5.14.0-362.el9.x86_64", ("syncfs", "syncfs (ext4, Linux 5.14)")),
        ("6.8.0-45-generic", ("syncfs", "syncfs (ext4, Linux 6.8)")),
        ("5.80", ("syncfs", "syncfs (ext4, Linux 5.80)")),
        ("garbage", ("fsync", "fsync (the kernel release 'garbage' is not recognized)")),
        ("5", ("fsync", "fsync (the kernel release '5' is not recognized)")),
        ("", ("fsync", "fsync (the kernel release '' is not recognized)")),
    ],
)
def test_the_kernel_release_decides_auto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: str, expected: tuple[str, str]
) -> None:
    kernel(monkeypatch, release)
    mounts(monkeypatch, (device_of(tmp_path), "/", "ext4"))
    assert _fs.resolve_sync(tmp_path, "auto") == expected


def test_a_host_that_is_not_linux_resolves_to_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "23.1.0", system="Darwin")
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (Darwin is not Linux)")


@pytest.mark.parametrize(
    ("kind", "strategy"),
    [
        ("lustre", "syncfs"),
        ("ext4", "syncfs"),
        ("xfs", "syncfs"),
        ("tmpfs", "syncfs"),
        ("nfs4", "fsync"),
        ("fuse.sshfs", "fsync"),
        ("overlay", "fsync"),
        ("btrfs", "fsync"),
        ("gpfs", "fsync"),
    ],
)
def test_only_vetted_filesystem_types_give_syncfs_under_auto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, strategy: str
) -> None:
    kernel(monkeypatch, "5.14.0-362.el9.x86_64")
    mounts(monkeypatch, (device_of(tmp_path), "/", kind))
    expected = f"syncfs ({kind}, Linux 5.14)" if strategy == "syncfs" else f"fsync ({kind} is not vetted for auto)"
    assert _fs.resolve_sync(tmp_path, "auto") == (strategy, expected)
    # Forcing overrides only the vetting.
    assert _fs.resolve_sync(tmp_path, "syncfs") == ("syncfs", f"syncfs (forced, {kind}, Linux 5.14)")
    assert _fs.resolve_sync(tmp_path, "fsync") == ("fsync", "fsync (io.sync is fsync)")


def test_a_device_match_wins_over_a_longer_mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, ("0:999", str(tmp_path), "zfs"), (device_of(tmp_path), "/elsewhere", "lustre"))
    assert _fs.resolve_sync(tmp_path, "auto") == ("syncfs", "syncfs (lustre, Linux 6.8)")


def test_a_btrfs_subvolume_without_a_device_match_takes_the_longest_mount_point(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, ("0:998", "/", "xfs"), ("0:999", str(tmp_path.parent), "btrfs"), ("0:997", "/x", "ext4"))
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (btrfs is not vetted for auto)")


def test_an_escaped_mount_point_is_decoded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    landing = tmp_path / "with space\tand\\slash"
    landing.mkdir()
    kernel(monkeypatch, "6.8.0-45-generic")
    escaped = str(landing).replace("\\", "\\134").replace(" ", "\\040").replace("\t", "\\011")
    mounts(monkeypatch, ("0:998", "/", "zfs"), ("0:999", escaped, "xfs"))
    assert _fs.resolve_sync(landing, "auto") == ("syncfs", "syncfs (xfs, Linux 6.8)")


def test_a_mount_point_must_be_a_whole_component_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    landing = tmp_path / "database"
    landing.mkdir()
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, ("0:998", "/", "btrfs"), ("0:999", str(tmp_path / "data"), "lustre"))
    assert _fs.resolve_sync(landing, "auto") == ("fsync", "fsync (btrfs is not vetted for auto)")


def test_an_unreadable_or_unmatched_mount_table_is_an_unknown_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: None)
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (the filesystem type is unknown)")
    assert _fs.resolve_sync(tmp_path, "syncfs") == ("syncfs", "syncfs (forced, unknown filesystem, Linux 6.8)")
    # Malformed lines are skipped; no entry matches the device or is a prefix of the path.
    text = "bad line\n20 1 0:999 / /nowhere rw - ext4 src rw\n21 1 0:998 / /x rw shared:1 -\n\n"
    monkeypatch.setattr(_fs, "_read_mountinfo", lambda: text)
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (the filesystem type is unknown)")


def test_the_filesystem_type_is_read_afresh_each_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # An anonymous device number is reused after an unmount: the type is never cached by st_dev.
    kernel(monkeypatch, "6.8.0-45-generic")
    kinds = ["ext4", "fuse.sshfs"]

    def read() -> str:
        return f"20 1 {device_of(tmp_path)} / / rw - {kinds.pop(0)} source rw\n"

    monkeypatch.setattr(_fs, "_read_mountinfo", read)
    assert _fs.resolve_sync(tmp_path, "auto") == ("syncfs", "syncfs (ext4, Linux 6.8)")
    assert _fs.resolve_sync(tmp_path, "auto") == ("fsync", "fsync (fuse.sshfs is not vetted for auto)")
    assert kinds == []


def test_forced_syncfs_below_5_8_or_off_linux_is_fsync_with_one_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    mounts(monkeypatch, (device_of(tmp_path), "/", "lustre"))
    kernel(monkeypatch, "4.18.0-553.el8")
    with caplog.at_level(logging.WARNING, logger="httk.workflow._fs"):
        assert _fs.resolve_sync(tmp_path, "syncfs") == ("fsync", "fsync (Linux 4.18 is older than 5.8)")
    assert [record.getMessage() for record in caplog.records] == [
        "io.sync is syncfs, but Linux 4.18 is older than 5.8: using fsync"
    ]
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="httk.workflow._fs"):
        assert _fs.resolve_sync(tmp_path, "auto")[0] == "fsync"
    assert caplog.records == []  # auto falls back quietly


@pytest.mark.parametrize("value", ["SYNCFS", "", "none", 1, None, True])
def test_an_invalid_setting_is_fsync_with_one_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, value: object
) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, (device_of(tmp_path), "/", "lustre"))
    with caplog.at_level(logging.WARNING, logger="httk.workflow._fs"):
        strategy, reason = _fs.resolve_sync(tmp_path, value)
    assert strategy == "fsync" and reason.startswith("fsync (io.sync ")
    assert len(caplog.records) == 1


# -- the barrier ------------------------------------------------------------------------------------------------------


class Recorder:
    """Records ``syncfs`` calls (descriptor, the inode it names) and anchor opens, in one sequence."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, anchor: Path, failures: Sequence[int | None] = ()) -> None:
        self.events: list[tuple[str, object]] = []
        self.failures = list(failures)
        real_syncfs, real_open = _fs._syncfs, os.open
        anchor_inode = os.stat(anchor).st_ino

        def syncfs(descriptor: int) -> None:
            self.events.append(("syncfs", descriptor))
            failure = self.failures.pop(0) if self.failures else None
            if failure is not None:
                raise OSError(failure, os.strerror(failure))
            real_syncfs(descriptor)

        def opening(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
            descriptor = real_open(path, flags, *args, **kwargs)
            if kwargs.get("dir_fd") is None and os.fstat(descriptor).st_ino == anchor_inode:
                self.events.append(("open", descriptor))
            return descriptor

        monkeypatch.setattr(_fs, "_syncfs", syncfs)
        monkeypatch.setattr(os, "open", opening)

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.events]


def syncfs_strategy(monkeypatch: pytest.MonkeyPatch, path: Path) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, (device_of(path), "/", "lustre"))


def test_the_barrier_primes_and_commits_on_one_descriptor_opened_before_anything_lands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    syncfs_strategy(monkeypatch, tmp_path)
    recorder = Recorder(monkeypatch, tmp_path)
    with _fs.Barrier(tmp_path, "auto") as barrier:
        assert (barrier.strategy, barrier.reason) == ("syncfs", "syncfs (lustre, Linux 6.8)")
        assert recorder.kinds() == ["open", "syncfs"]
        recorder.events.append(("write", None))
        barrier.commit(3)
    assert recorder.kinds() == ["open", "syncfs", "write", "syncfs"]
    opened = recorder.events[0][1]
    assert recorder.events[1][1] == opened and recorder.events[3][1] == opened
    assert capsys.readouterr().err == "syncing 3 landed file(s) to disk (syncfs: lustre, Linux 6.8)\n"


def test_a_quiet_barrier_prints_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with _fs.Barrier(tmp_path, "syncfs", quiet=True) as barrier:
        barrier.commit()
    assert capsys.readouterr().err == ""


def test_an_error_at_the_prime_is_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    syncfs_strategy(monkeypatch, tmp_path)
    recorder = Recorder(monkeypatch, tmp_path, [errno.EIO])
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow._fs"),
        _fs.Barrier(tmp_path, "auto", quiet=True) as barrier,
    ):
        assert barrier.strategy == "syncfs"
        barrier.commit()
    assert recorder.kinds() == ["open", "syncfs", "syncfs"]
    assert len(caplog.records) == 1 and "before anything was landed" in caplog.records[0].getMessage()
    assert _fs._syncfs_works == {os.stat(tmp_path).st_dev: True}


@pytest.mark.parametrize("error", [errno.EIO, errno.EDQUOT, errno.ENOSPC])
def test_an_error_at_the_commit_fails_it_without_any_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, error: int
) -> None:
    syncfs_strategy(monkeypatch, tmp_path)
    (tmp_path / "landed").write_bytes(b"x")
    recorder = Recorder(monkeypatch, tmp_path, [None, error])
    fsyncs: list[int] = []
    monkeypatch.setattr(os, "fsync", fsyncs.append)
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow._fs"),
        _fs.Barrier(tmp_path, "auto", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed) as raised,
    ):
        barrier.commit()
    assert isinstance(raised.value, OSError) and raised.value.errno == error
    assert "not durable" in str(raised.value)
    assert recorder.kinds() == ["open", "syncfs", "syncfs"]
    assert fsyncs == []  # never attributed: no per-file walk decides the error was someone else's
    assert len(caplog.records) == 1 and "syncfs (lustre, Linux 6.8)" in caplog.records[0].getMessage()


@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EPERM, errno.EINVAL])
def test_an_unsupported_syncfs_at_the_prime_makes_the_filesystem_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: int
) -> None:
    syncfs_strategy(monkeypatch, tmp_path)
    recorder = Recorder(monkeypatch, tmp_path, [error])
    with _fs.Barrier(tmp_path, "auto", inline=True, quiet=True) as barrier:
        assert (barrier.strategy, barrier.fsync_inline) == ("fsync", True)
        barrier.commit()
    assert recorder.kinds() == ["open", "syncfs"]  # the commit of an inline landing has nothing left to do
    assert _fs.resolve_sync(tmp_path, "auto") == (
        "fsync",
        "fsync (syncfs is not supported on this filesystem here)",
    )


def test_an_explicit_probe_records_eperm(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    syncfs_strategy(monkeypatch, tmp_path)
    Recorder(monkeypatch, tmp_path, [errno.EPERM])
    assert _fs.resolve_sync(tmp_path, "syncfs")[0] == "syncfs"
    assert _fs._probe_syncfs(tmp_path) is False
    assert _fs.resolve_sync(tmp_path, "syncfs")[0] == "fsync"
    assert _fs.resolve_sync(tmp_path, "auto")[0] == "fsync"


def test_a_barrier_needs_an_existing_anchor_and_a_context(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError), _fs.Barrier(tmp_path / "missing", "fsync"):
        pass
    barrier = _fs.Barrier(tmp_path, "fsync")
    with pytest.raises(ValueError, match="inside its context"):
        barrier.commit()


def test_a_barrier_closes_its_descriptor(tmp_path: Path) -> None:
    before = len(os.listdir("/proc/self/fd"))
    for setting in ("syncfs", "fsync"):
        with _fs.Barrier(tmp_path, setting, quiet=True) as barrier:
            barrier.commit()
        with pytest.raises(RuntimeError), _fs.Barrier(tmp_path, setting, quiet=True):
            raise RuntimeError
    assert len(os.listdir("/proc/self/fd")) == before


# -- the late walk (strategy fsync, a landing another process wrote) ---------------------------------------------


def landed_tree(root: Path) -> None:
    (root / "a" / "b").mkdir(parents=True)
    (root / "a" / "b" / "deep").write_bytes(b"1")
    (root / "a" / "file").write_bytes(b"2")
    (root / "top").write_bytes(b"3")
    (root / "empty").mkdir()
    (root / "link").symlink_to("/etc/passwd")
    (root / "a" / "dirlink").symlink_to(root / "a" / "b")
    os.mkfifo(root / "fifo")


def fsync_spy(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, bool]]:
    """Every ``os.fsync``: the inode it names and whether it is a directory."""

    synced: list[tuple[int, bool]] = []
    real = os.fsync

    def spy(descriptor: int) -> None:
        info = os.fstat(descriptor)
        synced.append((info.st_ino, stat.S_ISDIR(info.st_mode)))
        real(descriptor)

    monkeypatch.setattr(os, "fsync", spy)
    return synced


def test_a_walking_commit_fsyncs_every_file_then_every_directory_bottom_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    landed_tree(tmp_path)
    synced = fsync_spy(monkeypatch)
    with _fs.Barrier(tmp_path, "fsync") as barrier:
        assert not barrier.inline
        barrier.commit()
    inode = {name: os.lstat(tmp_path / name).st_ino for name in ("a/b/deep", "a/file", "top", "a/b", "a", "empty")}
    assert sorted(synced) == sorted(
        [(inode[name], False) for name in ("a/b/deep", "a/file", "top")]
        + [(inode[name], True) for name in ("a/b", "a", "empty")]
        + [(os.stat(tmp_path).st_ino, True)]
    )
    order = [ino for ino, _ in synced]
    assert order.index(inode["a/b/deep"]) < order.index(inode["a/b"]) < order.index(inode["a"])
    assert order[-1] == os.stat(tmp_path).st_ino
    assert capsys.readouterr().err == "syncing the landed files to disk (fsync: io.sync is fsync)\n"


def test_an_inline_commit_under_fsync_says_what_was_made_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    kernel(monkeypatch, "6.8.0-45-generic")
    mounts(monkeypatch, (device_of(tmp_path), "/", "nfs4"))
    walks: list[int] = []

    def walk(anchor: int, **_options: Any) -> int:
        walks.append(anchor)
        return 0

    monkeypatch.setattr(_fs, "fsync_landing", walk)
    with _fs.Barrier(tmp_path, "auto", inline=True) as barrier:
        barrier.commit(3, unit="bundle")
    assert walks == []
    assert capsys.readouterr().err == "made 3 landed bundle(s) durable (fsync: nfs4 is not vetted for auto)\n"


def test_a_walk_whose_first_listing_fails_fails_the_commit_with_one_labelled_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    real = os.listdir

    def listing(path: Any = ".") -> list[str]:
        if isinstance(path, int):
            raise OSError(errno.EIO, "injected")
        return real(path)

    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow._fs"),
        _fs.Barrier(tmp_path, "fsync", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed) as raised,
    ):
        monkeypatch.setattr(os, "listdir", listing)
        barrier.commit()
    assert isinstance(raised.value, _fs.SyncUnreadable)
    assert raised.value.errno == errno.EIO and f"cannot read {tmp_path} to sync it to disk" in str(raised.value)
    assert [record.getMessage() for record in caplog.records] == [
        f"the sync barrier (fsync (io.sync is fsync)) of {tmp_path} failed: {raised.value}"
    ]


def test_a_walk_refuses_a_tree_deeper_than_256_levels_without_leaking(tmp_path: Path) -> None:
    deep = tmp_path
    for _ in range(300):
        deep = deep / "d"
    deep.mkdir(parents=True)
    (deep / "file").write_bytes(b"x")
    before = len(os.listdir("/proc/self/fd"))
    with _fs.Barrier(tmp_path, "fsync", quiet=True) as barrier, pytest.raises(_fs.SyncFailed) as raised:
        barrier.commit()
    assert raised.value.errno == errno.ELOOP and "deeper than 256 levels" in str(raised.value)
    assert "/d" * 257 in str(raised.value) and "/d" * 258 not in str(raised.value)
    assert len(os.listdir("/proc/self/fd")) == before
    # 256 levels are walked.
    shallow = tmp_path / "shallow"
    for _ in range(256):
        shallow = shallow / "d"
    shallow.mkdir(parents=True)
    with _fs.Barrier(tmp_path / "shallow", "fsync", quiet=True) as barrier:
        barrier.commit()


def test_an_entry_the_walk_cannot_open_fails_the_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    if os.geteuid() == 0:
        pytest.skip("root opens a file of mode 000")
    landed_tree(tmp_path)
    (tmp_path / "a" / "file").chmod(0)
    try:
        before = len(os.listdir("/proc/self/fd"))
        with _fs.Barrier(tmp_path, "fsync", quiet=True) as barrier, pytest.raises(_fs.SyncFailed) as raised:
            barrier.commit()
        assert raised.value.errno == errno.EACCES
        # Not a write error: the entry is named as unreadable (SyncUnreadable).
        assert isinstance(raised.value, _fs.SyncUnreadable)
        assert f"cannot read {tmp_path}/a/file to sync it to disk" in str(raised.value)
        assert len(os.listdir("/proc/self/fd")) == before
    finally:
        (tmp_path / "a" / "file").chmod(0o600)


# -- extract_tar(durable=True) ----------------------------------------------------------------------------------------


def archive(members: Sequence[tuple[tarfile.TarInfo, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as writer:
        for info, data in members:
            writer.addfile(info, io.BytesIO(data) if info.isreg() else None)
    return buffer.getvalue()


def member(name: str, kind: bytes, data: bytes = b"", link: str = "") -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.type, info.linkname, info.mode, info.mtime = kind, link, 0o644 if kind != tarfile.DIRTYPE else 0o755, 1e9
    info.size = len(data)
    return info, data


def stream_members(files: int) -> list[tuple[tarfile.TarInfo, bytes]]:
    members = [member("job", tarfile.DIRTYPE), member("job/sub", tarfile.DIRTYPE), member("job/empty", tarfile.DIRTYPE)]
    members += [member(f"job/sub/f{index}", tarfile.REGTYPE, f"data {index}".encode()) for index in range(files)]
    # Implicit directories (no member of their own), a large member written by the reading thread, a hard-link copy.
    members += [
        member("implicit/deeper/g", tarfile.REGTYPE, b"g"),
        member("job/big", tarfile.REGTYPE, b"b" * (2 << 20)),
    ]
    members += [member("job/linked", tarfile.LNKTYPE, link="job/sub/f0"), member("job/sym", tarfile.SYMTYPE, link="x")]
    return members


class WriteTracker:
    """Pairs each written descriptor with its fsync before close, across threads, and orders every event."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.lock = threading.Lock()
        self.pending: dict[int, int] = {}  # descriptor -> inode written and not yet fsynced
        self.synced_files: set[int] = set()
        self.closed_unsynced: set[int] = set()
        self.events: list[tuple[str, int]] = []
        real_write, real_fsync, real_close, real_utime = _fs.write_all, os.fsync, os.close, os.utime

        def write_all(descriptor: int, data: bytes) -> None:
            real_write(descriptor, data)
            with self.lock:
                self.pending[descriptor] = os.fstat(descriptor).st_ino
                self.events.append(("write", self.pending[descriptor]))

        def fsync(descriptor: int) -> None:
            real_fsync(descriptor)
            info = os.fstat(descriptor)
            with self.lock:
                if self.pending.pop(descriptor, None) is not None:
                    self.synced_files.add(info.st_ino)
                self.events.append(("dir" if stat.S_ISDIR(info.st_mode) else "fsync", info.st_ino))

        def close(descriptor: int) -> None:
            with self.lock:
                if (inode := self.pending.pop(descriptor, None)) is not None:
                    self.closed_unsynced.add(inode)
            real_close(descriptor)

        def utime(path: Any, *arguments: Any, **options: Any) -> None:
            real_utime(path, *arguments, **options)
            if isinstance(path, int):
                inode = os.fstat(path).st_ino
            else:
                inode = os.stat(path, dir_fd=options.get("dir_fd"), follow_symlinks=False).st_ino
            with self.lock:
                self.events.append(("utime", inode))

        monkeypatch.setattr(_fs, "write_all", write_all)
        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(os, "close", close)
        monkeypatch.setattr(os, "utime", utime)


def inodes(root: Path, *, directories: bool) -> set[int]:
    found = {os.stat(root).st_ino} if directories else set()
    for path in root.rglob("*"):
        info = os.lstat(path)
        if (stat.S_ISDIR if directories else stat.S_ISREG)(info.st_mode):
            found.add(info.st_ino)
    return found


@WORKERS
def test_a_durable_extraction_fsyncs_every_file_on_its_writing_descriptor_and_directories_after_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    data = archive(stream_members(40))
    tracker = WriteTracker(monkeypatch)
    count = _fs.extract_tar(io.BytesIO(data), tmp_path, workers=workers, durable=True)
    monkeypatch.undo()
    assert count == 43  # 40 small files, g, big, the hard-link copy
    files, directories = inodes(tmp_path, directories=False), inodes(tmp_path, directories=True)
    assert len(files) == 43 and tracker.closed_unsynced == set()
    assert tracker.synced_files == files
    synced_directories = [inode for kind, inode in tracker.events if kind == "dir"]
    # Each once: job, sub, the empty member, implicit, deeper, the target.
    assert sorted(synced_directories) == sorted(directories) and len(directories) == 6
    last_write = max(index for index, (kind, _) in enumerate(tracker.events) if kind == "write")
    first_directory = min(index for index, (kind, _) in enumerate(tracker.events) if kind == "dir")
    assert last_write < first_directory
    assert synced_directories[-1] == os.stat(tmp_path).st_ino
    if workers == 1:
        # Deepest first in sequence; pooled (plan WORKFLOW_TRANSFER_SESSION §7.11, T6a item 2) the directories below
        # the target run in any order, all of them finished before the target's, which the line above pins.
        deeper = os.stat(tmp_path / "implicit" / "deeper").st_ino
        assert synced_directories.index(deeper) < synced_directories.index(os.stat(tmp_path / "implicit").st_ino)
    # A directory member's time is restored before the directory is fsynced, so the fsync makes it durable too.
    for name in ("job", "job/sub", "job/empty"):
        inode = os.stat(tmp_path / name).st_ino
        times = [index for index, event in enumerate(tracker.events) if event == ("utime", inode)]
        assert times and max(times) < tracker.events.index(("dir", inode))


@WORKERS
def test_a_plain_extraction_fsyncs_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int) -> None:
    calls: list[int] = []
    monkeypatch.setattr(os, "fsync", calls.append)
    assert _fs.extract_tar(io.BytesIO(archive(stream_members(10))), tmp_path, workers=workers) == 13
    assert calls == []


@WORKERS
def test_a_failed_fsync_fails_a_durable_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    def failing(_descriptor: int) -> None:
        raise OSError(errno.EIO, "injected")

    monkeypatch.setattr(os, "fsync", failing)
    with pytest.raises(OSError) as raised:
        _fs.extract_tar(io.BytesIO(archive(stream_members(10))), tmp_path, workers=workers, durable=True)
    assert raised.value.errno == errno.EIO


@WORKERS
def test_a_failed_directory_fsync_fails_a_durable_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    real = os.fsync
    files: list[int] = []

    target = os.stat(tmp_path).st_ino

    def failing(descriptor: int) -> None:
        info = os.fstat(descriptor)
        if stat.S_ISDIR(info.st_mode) and info.st_ino != target:  # a directory below the target, not the target
            raise OSError(errno.EIO, "injected")
        real(descriptor)
        files.append(descriptor)

    monkeypatch.setattr(os, "fsync", failing)
    with pytest.raises(OSError) as raised:
        _fs.extract_tar(io.BytesIO(archive(stream_members(10))), tmp_path, workers=workers, durable=True)
    assert raised.value.errno == errno.EIO and len(files) == 13
