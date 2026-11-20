"""A recorder of the renames, replaces, unlinks, ``rmdir``, ``fsync`` and ``syncfs`` calls of a test, in call order
(plan WORKFLOW_LOCAL_COMMANDS amendment 24: crash tests assert the call order, not end states)."""

import os
from pathlib import Path

import pytest

from httk.workflow import _fs


def where(path: object, dir_fd: int | None) -> str:
    if isinstance(path, int):
        return os.readlink(f"/proc/self/fd/{path}")
    if dir_fd is None:
        return str(path)
    return os.path.join(os.readlink(f"/proc/self/fd/{dir_fd}"), str(path))


class Journal(list[tuple[str, str]]):
    """``(operation, absolute path)`` of every rename, replace, unlink, rmdir, fsync and syncfs, in call order.

    Recorded with ``writes=True`` it also has ``create`` (a write temporary, ``*.tmp``, opened with ``O_CREAT``),
    ``write`` (an ``os.write`` on such a temporary or on a descriptor opened for appending), ``mkdir``; and
    :attr:`devices` gives the ``st_dev`` of each of those entries, by position.
    """

    def __init__(self) -> None:
        super().__init__()
        self.devices: dict[int, int] = {}

    def index_of(self, operation: str, path: Path | str) -> int:
        return self.index((operation, str(path)))

    def positions(self, operation: str, path: Path | str | None = None) -> list[int]:
        return [i for i, (op, where) in enumerate(self) if op == operation and (path is None or where == str(path))]


def record(monkeypatch: pytest.MonkeyPatch, *, writes: bool = False) -> Journal:
    """Start recording into a new journal for the rest of the test (``syncfs`` is recorded, never called).

    :param monkeypatch: The test's monkeypatch.
    :param writes: Record temporary creation, writes to temporaries and appended files, and ``mkdir`` too.
    :return: The journal.
    """

    recorded = Journal()
    rename, replace, unlink, rmdir, fsync = os.rename, os.replace, os.unlink, os.rmdir, os.fsync

    def renaming(src: object, dst: object, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        recorded.append(("rename", f"{where(src, src_dir_fd)} -> {where(dst, dst_dir_fd)}"))
        rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)  # type: ignore[arg-type]

    def replacing(src: object, dst: object, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        recorded.append(("replace", f"{where(src, src_dir_fd)} -> {where(dst, dst_dir_fd)}"))
        replace(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)  # type: ignore[arg-type]

    def unlinking(path: object, *, dir_fd: int | None = None) -> None:
        recorded.append(("unlink", where(path, dir_fd)))
        unlink(path, dir_fd=dir_fd)  # type: ignore[arg-type]

    def removing(path: object, *, dir_fd: int | None = None) -> None:
        recorded.append(("rmdir", where(path, dir_fd)))
        rmdir(path, dir_fd=dir_fd)  # type: ignore[arg-type]

    def fsyncing(descriptor: int) -> None:
        recorded.append(("fsync", os.readlink(f"/proc/self/fd/{descriptor}")))
        fsync(descriptor)

    monkeypatch.setattr(os, "rename", renaming)
    monkeypatch.setattr(os, "replace", replacing)
    monkeypatch.setattr(os, "unlink", unlinking)
    monkeypatch.setattr(os, "rmdir", removing)
    monkeypatch.setattr(os, "fsync", fsyncing)
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: recorded.append(("syncfs", where(descriptor, None))))
    if writes:
        _record_writes(monkeypatch, recorded)
    return recorded


def _record_writes(monkeypatch: pytest.MonkeyPatch, recorded: Journal) -> None:
    opening, writing, closing, making = os.open, os.write, os.close, os.mkdir
    tracked: dict[int, str] = {}

    def opened(path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        descriptor = opening(path, flags, mode, dir_fd=dir_fd)  # type: ignore[arg-type]
        temporary = flags & os.O_CREAT and str(path).endswith(".tmp")
        if temporary or flags & os.O_APPEND:
            tracked[descriptor] = os.readlink(f"/proc/self/fd/{descriptor}")
            if temporary:
                recorded.devices[len(recorded)] = os.fstat(descriptor).st_dev
                recorded.append(("create", tracked[descriptor]))
        return descriptor

    def wrote(descriptor: int, data: object) -> int:
        count = writing(descriptor, data)  # type: ignore[arg-type]
        if descriptor in tracked:
            recorded.devices[len(recorded)] = os.fstat(descriptor).st_dev
            recorded.append(("write", tracked[descriptor]))
        return count

    def closed(descriptor: int) -> None:
        tracked.pop(descriptor, None)
        closing(descriptor)

    def made(path: object, mode: int = 0o777, *, dir_fd: int | None = None) -> None:
        making(path, mode, dir_fd=dir_fd)  # type: ignore[arg-type]
        recorded.devices[len(recorded)] = os.stat(str(path) if dir_fd is None else path, dir_fd=dir_fd).st_dev  # type: ignore[arg-type]
        recorded.append(("mkdir", where(path, dir_fd)))

    monkeypatch.setattr(os, "open", opened)
    monkeypatch.setattr(os, "write", wrote)
    monkeypatch.setattr(os, "close", closed)
    monkeypatch.setattr(os, "mkdir", made)
