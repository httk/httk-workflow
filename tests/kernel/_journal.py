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
    """``(operation, absolute path)`` of every rename, replace, unlink, rmdir, fsync and syncfs, in call order."""

    def index_of(self, operation: str, path: Path | str) -> int:
        return self.index((operation, str(path)))

    def positions(self, operation: str, path: Path | str | None = None) -> list[int]:
        return [i for i, (op, where) in enumerate(self) if op == operation and (path is None or where == str(path))]


def record(monkeypatch: pytest.MonkeyPatch) -> Journal:
    """Start recording into a new journal for the rest of the test (``syncfs`` is recorded, never called).

    :param monkeypatch: The test's monkeypatch.
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
    return recorded
