"""Bottom-up tree removal (:func:`httk.workflow._fs._remove_tree`) answers entry types from the listing."""

import errno
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import _fs
from httk.workflow._fs import Loc


@pytest.fixture(autouse=True)
def _no_fault_injector() -> Iterator[None]:
    yield
    _fs.set_fault_injector(None)


def _trees(tmp_path: Path) -> tuple[Path, Path]:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    tree = tmp_path / "tree"
    (tree / "d1" / "d2").mkdir(parents=True)
    (tree / "empty").mkdir()
    (tree / "a.txt").write_text("a")
    (tree / "d1" / "b.txt").write_text("b")
    (tree / "d1" / "victim").write_text("v")
    (tree / "d1" / "d2" / "c.txt").write_text("c")
    (tree / "link").symlink_to(outside, target_is_directory=True)
    (tree / "d1" / "filelink").symlink_to(outside / "keep.txt")
    return tree, outside


def _spy_lstat(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = os.lstat

    def lstat(path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        calls.append(str(path))
        return real(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", lstat)
    return calls


def test_listed_entries_are_removed_without_lstat(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tree, outside = _trees(tmp_path)
    calls = _spy_lstat(monkeypatch)
    _fs._remove_tree(Loc(tree))
    monkeypatch.undo()
    assert not os.path.lexists(tree)
    assert (outside / "keep.txt").read_text() == "keep"
    # No Python-level os.lstat for a listed entry: only the top-level target, which no listing typed.
    assert calls == [str(tree)]


def test_a_file_swapped_for_a_directory_before_unlink_is_walked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree, outside = _trees(tmp_path)
    victim = tree / "d1" / "victim"
    swapped: list[Loc] = []

    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        if op == "remove" and src is not None and src.path.name == "victim" and not swapped:
            swapped.append(src)
            victim.unlink()
            victim.mkdir()
            (victim / "inner").mkdir()
            (victim / "inner" / "x.txt").write_text("x")

    _fs.set_fault_injector(fault)
    calls = _spy_lstat(monkeypatch)
    _fs._remove_tree(Loc(tree))
    monkeypatch.undo()
    assert swapped and not os.path.lexists(tree)
    assert sorted(os.listdir(outside)) == ["keep.txt"] and (outside / "keep.txt").read_text() == "keep"
    # The unlink refused the directory; only then does a Python-level os.lstat look at the entry again.
    assert calls == [str(tree), "victim"]


def test_a_directory_swapped_for_a_symlink_before_its_walk_is_unlinked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree, outside = _trees(tmp_path)
    real_open_listing = _fs._open_listing
    swapped: list[str] = []

    def open_listing(at: int | None, name: str) -> tuple[int, list[tuple[str, bool]]] | None:
        if name == "d2" and not swapped:
            # Listed as a directory, it is a symlink to the outside by the time it is opened.
            os.rename("d2", tmp_path / "aside", src_dir_fd=at)
            os.symlink(outside, "d2", dir_fd=at)
            swapped.append(name)
        return real_open_listing(at, name)

    monkeypatch.setattr(_fs, "_open_listing", open_listing)
    _fs._remove_tree(Loc(tree))
    assert swapped and not os.path.lexists(tree)
    assert sorted(os.listdir(outside)) == ["keep.txt"] and (outside / "keep.txt").read_text() == "keep"


def test_an_entry_gone_before_its_unlink_counts_as_removed(tmp_path: Path) -> None:
    tree, outside = _trees(tmp_path)
    vanished: list[str] = []

    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        if src is not None and src.path.name == "b.txt" and not vanished:
            os.unlink("b.txt", dir_fd=src.at)
            vanished.append("b.txt")

    _fs.set_fault_injector(fault)
    _fs._remove_tree(Loc(tree))
    assert vanished and not os.path.lexists(tree)
    assert (outside / "keep.txt").read_text() == "keep"


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
def test_a_genuine_permission_error_on_unlink_propagates(tmp_path: Path) -> None:
    tree, _ = _trees(tmp_path)
    locked = tree / "locked"
    locked.mkdir()
    (locked / "only.txt").write_text("only")
    # Its only entry is a file, so the unlink (not an rmdir) is what the read-only directory refuses.
    locked.chmod(0o500)
    try:
        with pytest.raises(PermissionError):
            _fs._remove_tree(Loc(tree))
        assert (locked / "only.txt").exists()
    finally:
        locked.chmod(0o700)


def test_an_eperm_unlink_of_a_listed_file_propagates_without_leaking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree, _ = _trees(tmp_path)
    real_unlink = os.unlink

    def unlink(path: Any, *args: Any, **kwargs: Any) -> None:
        if str(path) == "b.txt":
            raise PermissionError(errno.EPERM, "not permitted", path)
        real_unlink(path, *args, **kwargs)

    descriptors = sorted(os.listdir("/proc/self/fd"))
    monkeypatch.setattr(os, "unlink", unlink)
    with pytest.raises(PermissionError) as raised:
        _fs._remove_tree(Loc(tree))
    monkeypatch.undo()
    # EPERM may mean "a directory" (POSIX); lstat shows a file, so it is a genuine refusal.
    assert raised.value.errno == errno.EPERM and (tree / "d1" / "b.txt").exists()
    assert sorted(os.listdir("/proc/self/fd")) == descriptors
