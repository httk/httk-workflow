"""Tests of :func:`httk.workflow._fs.extract_tar`: the anchored extraction of an untrusted tar stream.

This is a trust boundary, so the tests are exhaustive on purpose. The differential test runs every archive
through the extractor it replaced (tarfile's ``data``/``tar`` filters, copied below as the oracle) and through
:func:`~httk.workflow._fs.extract_tar`, and checks a verdict declared per archive: both accept the same tree, both
refuse, or one of the recorded differences.
"""

import errno
import functools
import io
import os
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import types
from collections import Counter
from collections.abc import Callable, Collection, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, cast

import pytest

from httk.workflow import Workspace, _fs, _landing, _pool, adapter_runtime

MTIME = 1_000_000_000

type Member = tuple[tarfile.TarInfo, bytes]
type Builder = Callable[[Path], Sequence[Member]]
type Setup = Callable[[Path, Path], None]
type Extract = Callable[[IO[bytes], Path], None]


def receive_tar(workspace: Workspace, landing: Path, **options: Any) -> list[str]:
    """The far side's receiver reading standard input (a transfer session's ``stage`` reads its upload alike)."""

    return _landing.receive_tar(workspace, landing, sys.stdin.buffer, **options)


def old_extract(stream: IO[bytes], target: Path, *, rename: tuple[str, str] | None = None) -> None:
    """The oracle: ``adapter_runtime._extract`` as it was before the anchored extractor."""

    def checked(member: tarfile.TarInfo, path: str) -> tarfile.TarInfo:
        if rename is not None:
            if member.name != rename[0] or not member.isreg():
                raise tarfile.FilterError(f"unexpected member in a single-file pull: {member.name!r}")
            member = member.replace(name=rename[1], deep=False)
        return tarfile.tar_filter(member, path) if member.issym() else tarfile.data_filter(member, path)

    with tarfile.open(fileobj=stream, mode="r|") as archive:
        archive.extractall(target, filter=checked)


def new_extract(stream: IO[bytes], target: Path, *, workers: int = 1) -> None:
    _fs.extract_tar(stream, target, workers=workers)


#: The extractor sequential (today's path) and pooled (the far side of a push with ``io.workers`` 8).
WORKERS = pytest.mark.parametrize("workers", [1, 8], ids=["sequential", "pooled"])


def member(
    name: str,
    kind: bytes = tarfile.REGTYPE,
    data: bytes = b"",
    *,
    link: str = "",
    mode: int = 0o644,
    mtime: float = MTIME,
    pax: dict[str, str] | None = None,
) -> Member:
    info = tarfile.TarInfo(name)
    info.type, info.linkname, info.mode, info.mtime = kind, link, mode, mtime
    info.size = len(data) if info.isreg() else 0
    if pax is not None:
        info.pax_headers = dict(pax)  # read back over the header fields (PAX archives only)
    return info, data


def F(name: str, data: bytes = b"data", *, mode: int = 0o644, mtime: float = MTIME) -> Member:
    return member(name, tarfile.REGTYPE, data, mode=mode, mtime=mtime)


def D(name: str, mtime: float = MTIME, mode: int = 0o755) -> Member:
    return member(name, tarfile.DIRTYPE, mode=mode, mtime=mtime)


def S(name: str, link: str) -> Member:
    return member(name, tarfile.SYMTYPE, link=link, mode=0o777)


def H(name: str, link: str, mode: int = 0o644) -> Member:
    return member(name, tarfile.LNKTYPE, link=link, mode=mode)


def archive(members: Sequence[Member], fmt: int = tarfile.PAX_FORMAT) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=fmt) as writer:
        for info, data in members:
            writer.addfile(info, io.BytesIO(data) if info.isreg() else None)
    return buffer.getvalue()


def dated_directories(data: bytes) -> set[str]:
    """The normalized names of the directory members, whose times both extractors restore."""

    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as reader:
        return {os.path.normpath(info.name.lstrip("/")) for info in reader if info.isdir()}


def snapshot(root: Path, base: Path, dated: Collection[str] = ()) -> dict[str, tuple[object, ...]]:
    """Every entry below *root*: type, link text, bytes, modes, file times and the times of *dated* directories."""

    found: dict[str, tuple[object, ...]] = {}
    for directory, directories, files in os.walk(root):
        for name in [*directories, *files]:
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                found[relative] = ("link", os.readlink(path).replace(str(base), "<base>"))
            elif stat.S_ISDIR(info.st_mode):
                dated_time = info.st_mtime_ns if relative in dated else None
                found[relative] = ("dir", stat.S_IMODE(info.st_mode), dated_time)
            elif stat.S_ISREG(info.st_mode):
                found[relative] = ("file", stat.S_IMODE(info.st_mode), info.st_mtime_ns, path.read_bytes())
            else:
                found[relative] = ("special", stat.S_IFMT(info.st_mode))
    if "." in dated:
        found["."] = ("root", root.stat().st_mtime_ns)
    return found


def linked_files(root: Path) -> list[str]:
    """The regular files below *root* that share their inode with another name."""

    found = []
    for directory, _, files in os.walk(root):
        for name in files:
            info = (Path(directory) / name).lstat()
            if stat.S_ISREG(info.st_mode) and info.st_nlink > 1:
                found.append(str(Path(directory) / name))
    return sorted(found)


_OUTSIDE: dict[Path, dict[str, tuple[object, ...]]] = {}


def outside_state(base: Path) -> dict[str, tuple[object, ...]]:
    """Everything outside the landing: the entries of *base*, and every entry of the outside directory with its
    type, mode, time, link count, size and content."""

    state: dict[str, tuple[object, ...]] = {"<base>": tuple(sorted(os.listdir(base)))}
    outside = base / "outside"
    for path in [outside, *sorted(outside.rglob("*"))]:
        info = path.lstat()
        content: object = None
        if stat.S_ISREG(info.st_mode):
            content = path.read_bytes()
        elif stat.S_ISLNK(info.st_mode):
            content = os.readlink(path)
        state[path.relative_to(base).as_posix()] = (
            stat.S_IFMT(info.st_mode),
            stat.S_IMODE(info.st_mode),
            info.st_mtime_ns,
            info.st_nlink,
            info.st_size,
            content,
        )
    return state


def prepare(base: Path) -> tuple[Path, Path]:
    target, outside = base / "landing", base / "outside"
    target.mkdir(parents=True)
    (outside / "sub").mkdir(parents=True)
    (outside / "secret").write_text("secret", encoding="utf-8")
    _OUTSIDE[base] = outside_state(base)
    return target, outside


def outside_untouched(base: Path) -> bool:
    return outside_state(base) == _OUTSIDE[base]


def run(extract: Extract, data: bytes, base: Path, setup: Setup | None = None) -> tuple[Path, Exception | None]:
    target, outside = prepare(base)
    if setup is not None:
        setup(target, outside)
    try:
        extract(io.BytesIO(data), target)
    except Exception as exc:  # the verdict decides which errors are expected
        return target, exc
    return target, None


# -- the differential test ------------------------------------------------------------------------------------------

ACCEPT = "both accept the same tree"
COPY = "both accept the same tree; the oracle aliases hard links, the new extractor copies them"
REFUSE = "both refuse"
# Old accepts, new refuses (narrowings).
NARROW_DOTDOT = "narrowing: a '..' component inside the tree"
NARROW_LEAF = "narrowing: an existing symlink at a regular file's name"
NARROW_COMPONENT = "narrowing: a symlink at a directory component, even one pointing inside"
NARROW_HARDLINK_SYMLINK = "narrowing: a hard link to a symlink"
NARROW_HARDLINK_OTHER = "narrowing: a hard link to a directory or to a file this stream did not write"
NARROW_DEPTH = "narrowing: more than 256 components"
NARROW_OLD_SKIPS = "narrowing: a symlink the oracle cannot create, which it silently skips"
NARROW_TRAILING = "narrowing: a non-directory whose name ends in '/' or '/.'"
NARROW_SPARSE = "narrowing: a sparse member (the oracle writes it sparse, at whatever size its map claims)"
# Old refuses, new accepts (relaxations).
RELAX_SYMLINK = "relaxation: a re-delivered symlink over an outside-pointing symlink"
RELAX_HARDLINK = "relaxation: a hard link over an existing name, copied (the oracle cannot seek back in a stream)"
RELAX_SELF_LINK = "relaxation: a hard link naming itself after its file, as GNU tar sends a name given twice"
RELAX_TIME = "relaxation: an unusable time (NaN, out of range) is not fatal"

NARROWINGS = frozenset(
    {
        NARROW_DOTDOT,
        NARROW_LEAF,
        NARROW_COMPONENT,
        NARROW_HARDLINK_SYMLINK,
        NARROW_HARDLINK_OTHER,
        NARROW_DEPTH,
        NARROW_OLD_SKIPS,
        NARROW_TRAILING,
        NARROW_SPARSE,
    }
)
RELAXATIONS = frozenset({RELAX_SYMLINK, RELAX_HARDLINK, RELAX_SELF_LINK, RELAX_TIME})
FORMATS = {tarfile.PAX_FORMAT: "pax", tarfile.GNU_FORMAT: "gnu"}
PAX_ONLY = (tarfile.PAX_FORMAT,)
#: A PAX 0.1 sparse map claiming a 64 GiB file for one byte of data.
HUGE_SPARSE = {"GNU.sparse.map": "0,1", "GNU.sparse.size": str(1 << 36), "GNU.sparse.numblocks": "1"}
#: The same map claiming 1 MiB: the oracle writes what the map claims, so the differential keeps it small.
SMALL_SPARSE = {**HUGE_SPARSE, "GNU.sparse.size": str(1 << 20)}


@dataclass(frozen=True)
class Case:
    """One generated archive and its expected verdict.

    :param name: The test id.
    :param build: The members, given the outside directory.
    :param verdict: The expected outcome.
    :param error: The class the new extractor raises when it refuses.
    :param setup: Plants entries in the landing (and outside) before both extractions.
    :param formats: The archive formats that can carry the members (PAX headers need the PAX format).
    """

    name: str
    build: Builder
    verdict: str
    error: type[Exception] = tarfile.FilterError
    setup: Setup | None = None
    formats: tuple[int, ...] = tuple(FORMATS)


def _plant_file(name: str, data: bytes = b"earlier") -> Setup:
    def setup(target: Path, outside: Path) -> None:
        (target / name).write_bytes(data)

    return setup


def _plant_outside_symlink(name: str) -> Setup:
    def setup(target: Path, outside: Path) -> None:
        (target / name).symlink_to(outside)

    return setup


def _plant_inside_symlink(target: Path, outside: Path) -> None:
    (target / "y").write_bytes(b"y")
    (target / "x").symlink_to("y")


CASES = [
    Case(
        "plain files and modes",
        lambda o: [
            D("d"),
            F("d/f", b"plain"),
            F("d/x", b"#!/bin/sh\n", mode=0o755),
            F("d/g", mode=0o640),
            F("setuid", mode=0o4777),
            F("world-writable", mode=0o666),
            F("owner-exec-only", mode=0o100),
            F("group-exec-only", mode=0o650),
            F("fractional-time", mtime=MTIME + 0.5),
            F("empty", b""),
            F("big", bytes(range(256)) * 4200),  # just over one 1 MiB chunk,
        ],
        ACCEPT,
    ),
    Case("empty and implicit directories", lambda o: [D("e"), D("e/f"), D("g/h"), D("i/j/k", mtime=5)], ACCEPT),
    Case("contiguous member", lambda o: [member("c", tarfile.CONTTYPE, b"contiguous")], ACCEPT),
    Case("absolute name", lambda o: [F("/abs/f"), D("//abs/g")], ACCEPT),
    Case(
        "dot directory members", lambda o: [D(".", mtime=7), F("./a"), D("./d"), F("./d/b"), D("./", mtime=9)], ACCEPT
    ),
    Case("empty and dot components", lambda o: [D("a"), F("a//./b"), F("./a/./c")], ACCEPT),
    Case("symlink texts", lambda o: [S("abs", "/opt/pseudopotentials/Si"), S("rel", "../up"), S("dot", ".")], ACCEPT),
    Case("duplicate file then file", lambda o: [F("a", b"1"), F("a", b"2", mode=0o755)], ACCEPT),
    Case("duplicate file then symlink", lambda o: [F("a"), S("a", "b")], ACCEPT),
    Case("duplicate symlink then symlink", lambda o: [S("a", "b"), S("a", "c")], ACCEPT),
    Case("duplicate directory", lambda o: [D("d", mtime=1), F("d/f"), D("d", mtime=2)], ACCEPT),
    Case(
        "long names",
        lambda o: [D("d" * 100), F("d" * 100 + "/" + "e" * 150 + "/" + "f" * 200, b"x"), S("l" * 180, "t" * 300)],
        ACCEPT,
    ),
    Case("re-delivery over an existing file", lambda o: [F("a", b"new")], ACCEPT, setup=_plant_file("a")),
    Case("hard link inside", lambda o: [F("data", b"payload\n"), H("alias", "data")], COPY),
    Case("hard link chain", lambda o: [F("a", b"chain", mode=0o755), H("b", "a", 0o755), H("c", "b", 0o755)], COPY),
    Case("hard link through dot components", lambda o: [D("d"), F("d/a"), H("d/b", "./d//a")], COPY),
    Case("hard link over an existing name", lambda o: [F("a", b"a"), F("b", b"b"), H("b", "a")], RELAX_HARDLINK),
    Case("dotdot escape", lambda o: [F("../escape")], REFUSE, tarfile.OutsideDestinationError),
    Case("dotdot deep escape", lambda o: [D("a"), F("a/../../escape")], REFUSE, tarfile.OutsideDestinationError),
    Case("dotdot inside", lambda o: [D("a"), F("a/../b")], NARROW_DOTDOT, tarfile.OutsideDestinationError),
    Case("dotdot directory inside", lambda o: [D("a/.."), F("b")], NARROW_DOTDOT, tarfile.OutsideDestinationError),
    Case(
        "symlink outside then member through it",
        lambda o: [S("link", str(o)), F("link/evil")],
        REFUSE,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "symlink outside then directory through it",
        lambda o: [S("link", str(o)), D("link/evil")],
        REFUSE,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "symlink inside then member through it",
        lambda o: [D("real"), S("link", "real"), F("link/f")],
        NARROW_COMPONENT,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "symlink inside then directory at its name",
        lambda o: [D("real"), S("d", "real"), D("d")],
        NARROW_COMPONENT,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "symlink to an outside file then file at its name",
        lambda o: [S("x", str(o / "secret")), F("x", b"pwned")],
        REFUSE,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "symlink to outside then file at its name",
        lambda o: [S("x", str(o)), F("x", b"pwned")],
        REFUSE,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "symlink inside then file at its name",
        lambda o: [F("y", b"y"), S("x", "y"), F("x", b"through")],
        NARROW_LEAF,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "dangling inside symlink then file at its name",
        lambda o: [S("x", "y"), F("x", b"through")],
        NARROW_LEAF,
        tarfile.OutsideDestinationError,
    ),
    Case(
        "hard link outside",
        lambda o: [H("x", "../outside/secret"), F("x", b"pwned")],
        REFUSE,
        tarfile.LinkOutsideDestinationError,
    ),
    Case("hard link absolute", lambda o: [H("x", str(o / "secret"))], REFUSE, tarfile.AbsoluteLinkError),
    Case(
        "hard link with dotdot inside",
        lambda o: [D("d"), F("data"), H("h", "d/../data")],
        NARROW_DOTDOT,
        tarfile.LinkOutsideDestinationError,
    ),
    Case("hard link to a symlink inside", lambda o: [F("data"), S("s", "data"), H("h", "s")], NARROW_HARDLINK_SYMLINK),
    Case("hard link to a symlink outside", lambda o: [S("s", str(o / "secret")), H("h", "s")], REFUSE),
    Case("hard link to a replaced file", lambda o: [F("a"), S("a", "x"), H("h", "a")], NARROW_HARDLINK_SYMLINK),
    Case("hard link to a directory", lambda o: [D("d"), H("h", "d")], NARROW_HARDLINK_OTHER),
    Case("hard link to itself", lambda o: [H("f", "f")], REFUSE),
    Case("hard link to itself after its file", lambda o: [F("f"), H("f", "f")], RELAX_SELF_LINK),
    Case("hard link to an unknown name", lambda o: [H("h", "nope")], REFUSE),
    Case(
        "hard link to a file of an earlier delivery",
        lambda o: [H("h", "pre")],
        NARROW_HARDLINK_OTHER,
        setup=_plant_file("pre"),
    ),
    Case("hard link naming the target", lambda o: [F("f"), H(".", "f")], REFUSE, tarfile.OutsideDestinationError),
    Case("character device", lambda o: [member("c", tarfile.CHRTYPE)], REFUSE, tarfile.SpecialFileError),
    Case("block device", lambda o: [member("b", tarfile.BLKTYPE)], REFUSE, tarfile.SpecialFileError),
    Case("fifo", lambda o: [member("p", tarfile.FIFOTYPE)], REFUSE, tarfile.SpecialFileError),
    Case("unknown type", lambda o: [member("u", b"Z")], REFUSE, tarfile.SpecialFileError),
    Case("special file escaping", lambda o: [member("../c", tarfile.CHRTYPE)], REFUSE, tarfile.OutsideDestinationError),
    Case("directory then file", lambda o: [D("d"), F("d")], REFUSE),
    Case("file then directory", lambda o: [F("d"), D("d")], REFUSE),
    Case("file then member below it", lambda o: [F("d"), F("d/f")], REFUSE),
    Case("directory then symlink", lambda o: [D("d"), S("d", "x")], NARROW_OLD_SKIPS),
    Case("file then symlink below it", lambda o: [F("d"), S("d/s", "x")], REFUSE),
    Case("symlink named dot", lambda o: [S(".", "x")], NARROW_OLD_SKIPS, tarfile.OutsideDestinationError),
    Case("regular member named dot", lambda o: [F(".")], REFUSE, tarfile.OutsideDestinationError),
    Case("regular member named dot slash", lambda o: [F("./")], REFUSE),
    Case("file named like a directory", lambda o: [F("a/")], NARROW_TRAILING),
    Case("file named with a trailing dot component", lambda o: [F("a/.")], REFUSE),
    Case("symlink named with a trailing dot component", lambda o: [S("a/.", "x")], NARROW_TRAILING),
    Case("hard link named with a trailing dot component", lambda o: [F("f"), H("a/.", "f")], REFUSE),
    Case("non-UTF-8 names", lambda o: [D("\udcff"), F("\udcff/\udcfe", b"x"), S("\udcfd", "\udcfe")], ACCEPT),
    Case("NUL in a name", lambda o: [member("x", data=b"x", pax={"path": "a\0b"})], REFUSE, formats=PAX_ONLY),
    Case("symlink with an empty link text", lambda o: [S("s", "")], NARROW_OLD_SKIPS),
    Case(
        "symlink with a NUL in its link text",
        lambda o: [member("s", tarfile.SYMTYPE, pax={"linkpath": "a\0b"})],
        REFUSE,
        formats=PAX_ONLY,
    ),
    Case(
        "NaN file time",
        lambda o: [member("f", data=b"x", pax={"mtime": "nan"}), F("after")],
        RELAX_TIME,
        formats=PAX_ONLY,
    ),
    Case(
        "NaN directory time",
        lambda o: [member("d", tarfile.DIRTYPE, pax={"mtime": "nan"}), F("d/after")],
        RELAX_TIME,
        formats=PAX_ONLY,
    ),
    Case(
        "NaN time of the target",
        lambda o: [member(".", tarfile.DIRTYPE, pax={"mtime": "nan"}), F("after")],
        RELAX_TIME,
        formats=PAX_ONLY,
    ),
    Case(
        "out-of-range file time",
        lambda o: [member("f", data=b"x", pax={"mtime": "1e300"}), F("after")],
        RELAX_TIME,
        formats=PAX_ONLY,
    ),
    Case(
        "out-of-range directory time",
        lambda o: [member("d", tarfile.DIRTYPE, pax={"mtime": "-1e300"}), F("d/after")],
        RELAX_TIME,
        formats=PAX_ONLY,
    ),
    Case(
        "sparse member",
        lambda o: [member("sparse", data=b"x", pax=SMALL_SPARSE)],
        NARROW_SPARSE,
        formats=PAX_ONLY,
    ),
    Case("name beyond the full-path bound", lambda o: [F("/".join(["p" * 200] * 25))], REFUSE),
    Case("component beyond NAME_MAX", lambda o: [F("n" * 300)], REFUSE, OSError),
    Case("deeper than 256 components", lambda o: [F("a/" * 300 + "f")], NARROW_DEPTH),
    Case(
        "re-delivered symlink over an outside-pointing symlink",
        lambda o: [S("x", "y")],
        RELAX_SYMLINK,
        setup=_plant_outside_symlink("x"),
    ),
    Case(
        "re-delivered file over a planted inside symlink",
        lambda o: [F("x", b"through")],
        NARROW_LEAF,
        tarfile.OutsideDestinationError,
        setup=_plant_inside_symlink,
    ),
    Case(
        "planted outside symlink at a component",
        lambda o: [F("a/f")],
        REFUSE,
        tarfile.OutsideDestinationError,
        setup=_plant_outside_symlink("a"),
    ),
]


def check_verdict(
    verdict: str,
    old_data: bytes,
    new_data: bytes,
    tmp_path: Path,
    setup: Setup | None,
    error: type[Exception],
    workers: int = 1,
) -> None:
    """Run the oracle on *old_data* and the new extractor on *new_data* (the same members) and check *verdict*."""

    old_target, old_error = run(old_extract, old_data, tmp_path / "old", setup)
    new_target, new_error = run(functools.partial(new_extract, workers=workers), new_data, tmp_path / "new", setup)
    assert outside_untouched(tmp_path / "old") and outside_untouched(tmp_path / "new")
    assert linked_files(new_target) == []  # never a hard link
    if new_error is not None:
        assert isinstance(new_error, error), repr(new_error)
        if error is not OSError:
            assert isinstance(new_error, tarfile.FilterError), repr(new_error)
    if verdict in (ACCEPT, COPY):
        assert old_error is None and new_error is None, (old_error, new_error)
        dated = dated_directories(old_data)
        assert snapshot(new_target, tmp_path / "new", dated) == snapshot(old_target, tmp_path / "old", dated)
        assert bool(linked_files(old_target)) == (verdict == COPY)
    elif verdict == REFUSE:
        assert old_error is not None and new_error is not None, (old_error, new_error)
    elif verdict in NARROWINGS:
        assert old_error is None and new_error is not None, (old_error, new_error)
    else:
        assert verdict in RELAXATIONS
        assert old_error is not None and new_error is None, (old_error, new_error)


@functools.cache
def oracle_relinks_over_existing_names() -> bool:
    """Whether this Python's tarfile unlinks an existing name before ``os.link`` (3.13.12 and 3.14.3 do).

    Without that, the oracle falls back to re-extracting the earlier member, which a stream cannot seek back to.
    """

    with tempfile.TemporaryDirectory() as scratch:
        try:
            old_extract(io.BytesIO(archive([F("a"), F("b"), H("b", "a")])), Path(scratch))
        except tarfile.StreamError:
            return False
    return True


@WORKERS
@pytest.mark.parametrize(
    ("case", "fmt"),
    [
        pytest.param(case, fmt, id=f"{case.name}-{label}")
        for case in CASES
        for fmt, label in FORMATS.items()
        if fmt in case.formats
    ],
)
def test_differential_against_the_stdlib_filters(tmp_path: Path, case: Case, fmt: int, workers: int) -> None:
    # Members naming the outside name each run's own outside directory.
    old_data = archive(case.build(tmp_path / "old" / "outside"), fmt)
    new_data = archive(case.build(tmp_path / "new" / "outside"), fmt)
    verdict = case.verdict
    if verdict == RELAX_HARDLINK and oracle_relinks_over_existing_names():
        verdict = COPY  # a newer oracle accepts too, aliasing where the new extractor copies
    check_verdict(verdict, old_data, new_data, tmp_path, case.setup, case.error, workers)


def test_the_verdict_list_covers_every_recorded_difference() -> None:
    used = {case.verdict for case in CASES}
    assert used == {ACCEPT, COPY, REFUSE} | NARROWINGS | RELAXATIONS


# -- archives written by GNU tar ------------------------------------------------------------------------------------

TAR = shutil.which("tar")
needs_tar = pytest.mark.skipif(TAR is None or sys.platform != "linux", reason="needs GNU tar")


def job_tree(root: Path) -> Path:
    """A realistic job tree: nested directories, exec and plain files, symlinks, a hard link, an empty directory."""

    job = root / "job--c7f0b2"
    (job / "inputs" / "deep" / "deeper").mkdir(parents=True)
    (job / "outputs").mkdir()
    (job / "empty").mkdir()
    (job / "inputs" / "INCAR").write_text("ENCUT = 520\n", encoding="utf-8")
    (job / "inputs" / "deep" / "deeper" / "data.bin").write_bytes(bytes(range(256)) * 600)
    (job / "run.sh").write_text("#!/bin/sh\necho run\n", encoding="utf-8")
    (job / "run.sh").chmod(0o755)
    (job / "private").write_text("private\n", encoding="utf-8")
    (job / "private").chmod(0o600)
    (job / "outputs" / "OUTCAR").write_text("done\n" * 100, encoding="utf-8")
    (job / "outputs" / "relative").symlink_to("../inputs/INCAR")
    (job / "outputs" / "absolute").symlink_to("/opt/pseudopotentials/Si")
    (job / ("long-" + "n" * 200)).write_text("long\n", encoding="utf-8")
    os.link(job / "inputs" / "INCAR", job / "outputs" / "INCAR.copy")
    second = root / "job--d8e1c3" / "a" / "b" / "c"
    second.mkdir(parents=True)
    (second / "file").write_text("second\n", encoding="utf-8")
    # Canonical modes, whatever the umask: the data filter would change any other.
    for path in root.rglob("*"):
        if path.is_file() and not path.is_symlink() and path.name not in ("run.sh", "private"):
            path.chmod(0o644)
    # Times last, deepest first, so that no later write moves them.
    paths = sorted((path for path in root.rglob("*")), key=lambda path: len(path.parts), reverse=True)
    for index, path in enumerate([*paths, root]):
        os.utime(path, (MTIME + index, MTIME + index), follow_symlinks=False)
    return root


def gnu_tar(source: Path, names: Sequence[str], *options: str) -> bytes:
    assert TAR is not None
    result = subprocess.run(
        [TAR, *options, "-cf", "-", "-C", str(source), "--", *names], capture_output=True, check=True
    )
    return result.stdout


@needs_tar
@WORKERS
@pytest.mark.parametrize("fmt", ["gnu", "posix", "ustar"])
@pytest.mark.parametrize("selection", ["top", "nested", "dot"])
def test_differential_on_gnu_tar_archives(tmp_path: Path, fmt: str, selection: str, workers: int) -> None:
    # The three shapes a pull sends: a batch of top-level names, a batch of nested paths (their parents implicit),
    # and a whole tree (``-- .``, whose first member names the target itself).
    source = job_tree(tmp_path / "src")
    names = {
        "top": sorted(os.listdir(source)),
        "nested": ["job--c7f0b2/inputs/INCAR", "job--c7f0b2/outputs", "job--d8e1c3/a/b"],
        "dot": ["."],
    }[selection]
    if fmt == "ustar":
        os.unlink(source / "job--c7f0b2" / ("long-" + "n" * 200))  # ustar cannot carry the long name
    data = gnu_tar(source, names, f"--format={fmt}")
    assert ("." in dated_directories(data)) == (selection == "dot")
    check_verdict(COPY, data, data, tmp_path, None, tarfile.FilterError, workers)


def allocated(root: Path) -> int:
    """The bytes allocated to every entry below *root*."""

    return sum(path.lstat().st_blocks * 512 for path in root.rglob("*"))


@needs_tar
@pytest.mark.parametrize("fmt", ["gnu", "posix"])
def test_a_sparse_member_written_by_gnu_tar_is_refused(tmp_path: Path, fmt: str) -> None:
    source = tmp_path / "src"
    source.mkdir()
    (source / "before").write_bytes(b"before")
    (source / "before").chmod(0o644)
    with open(source / "sparse", "wb") as handle:
        handle.seek(512 << 10)
        handle.write(b"tail")
        handle.truncate(1 << 20)
    data = gnu_tar(source, ["before", "sparse"], "-S", f"--format={fmt}")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as reader:
        assert [info.sparse is not None for info in reader] == [False, True]
    check_verdict(NARROW_SPARSE, data, data, tmp_path, None, tarfile.FilterError)
    landing = tmp_path / "new" / "landing"
    assert os.listdir(landing) == ["before"] and allocated(landing) < 64 << 10


def test_a_handcrafted_sparse_member_claiming_a_huge_size_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Should the refusal regress, the dense write fails after 1 MiB instead of filling the disk.
    written = [0]
    real_write_all = _fs.write_all

    def bounded(descriptor: int, data: bytes) -> None:
        written[0] += len(data)
        if written[0] > 1 << 20:
            raise AssertionError("the sparse member is being written")
        real_write_all(descriptor, data)

    monkeypatch.setattr(_fs, "write_all", bounded)
    target, _ = prepare(tmp_path)
    data = archive([F("before", b"x"), member("sparse", data=b"x", pax=HUGE_SPARSE)])
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as reader:
        assert [info.size for info in reader] == [1, 1 << 36]
    with pytest.raises(tarfile.FilterError, match="sparse members are not accepted"):
        _fs.extract_tar(io.BytesIO(data), target)
    assert os.listdir(target) == ["before"] and allocated(target) < 64 << 10
    assert outside_untouched(tmp_path)


@needs_tar
@pytest.mark.parametrize("fmt", ["gnu", "posix"])
def test_a_name_gnu_tar_was_given_twice_is_a_self_link_and_a_no_op(tmp_path: Path, fmt: str) -> None:
    source = tmp_path / "src"
    (source / "d").mkdir(parents=True)
    (source / "d" / "a").write_bytes(b"twice")
    (source / "d" / "a").chmod(0o644)
    data = gnu_tar(source, ["d", "d/a"], f"--format={fmt}")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as reader:
        assert [(info.name, info.type, info.linkname) for info in reader][-1] == ("d/a", tarfile.LNKTYPE, "d/a")
    check_verdict(RELAX_SELF_LINK, data, data, tmp_path, None, tarfile.FilterError)
    assert (tmp_path / "new" / "landing" / "d" / "a").read_bytes() == b"twice"


def test_a_self_link_before_its_file_is_refused(tmp_path: Path) -> None:
    with pytest.raises(tarfile.FilterError, match="not a regular file this stream extracted"):
        _fs.extract_tar(io.BytesIO(archive([F("g"), H("f", "f")])), tmp_path)


# -- planted symlinks, a symlinked target -------------------------------------------------------------------------


@pytest.mark.parametrize("pointing", ["outside", "inside"])
def test_a_planted_symlink_at_an_intermediate_component_refuses_the_member(tmp_path: Path, pointing: str) -> None:
    target, outside = prepare(tmp_path)
    (target / "d" / "real").mkdir(parents=True)
    (target / "d" / "e").symlink_to(outside if pointing == "outside" else target / "d" / "real")
    for name in ("d/e/f", "d/e/f/g", "d/e/sub/f"):
        with pytest.raises(tarfile.OutsideDestinationError):
            _fs.extract_tar(io.BytesIO(archive([F(name, b"pwned")])), target)
    assert outside_untouched(tmp_path)
    assert os.listdir(target / "d" / "real") == []


def test_a_symlink_left_by_an_earlier_extraction_refuses_later_members(tmp_path: Path) -> None:
    target, outside = prepare(tmp_path)
    _fs.extract_tar(io.BytesIO(archive([S("a", str(outside)), S("s", str(outside / "secret"))])), target)
    assert os.readlink(target / "a") == str(outside)
    for members in ([F("a/f", b"pwned")], [D("a/sub")], [F("s", b"pwned")], [F("a/sub/f")], [H("a/h", "x")]):
        with pytest.raises(tarfile.FilterError):
            _fs.extract_tar(io.BytesIO(archive([F("x"), *members])), target)
    assert outside_untouched(tmp_path)


def test_a_target_given_through_a_symlink_works(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "via").symlink_to(real)
    _fs.extract_tar(io.BytesIO(archive([D("d"), F("d/f", b"x"), S("d/s", "f"), H("h", "d/f")])), tmp_path / "via")
    assert (real / "d" / "f").read_bytes() == (real / "h").read_bytes() == b"x"
    assert os.readlink(real / "d" / "s") == "f"


def test_a_target_that_is_missing_or_not_a_directory_raises_os_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _fs.extract_tar(io.BytesIO(archive([F("f")])), tmp_path / "missing")
    (tmp_path / "file").write_bytes(b"")
    with pytest.raises(NotADirectoryError):
        _fs.extract_tar(io.BytesIO(archive([F("f")])), tmp_path / "file")


# -- hard links ----------------------------------------------------------------------------------------------------


def test_a_hard_link_copy_over_a_destination_already_linked_to_its_source(tmp_path: Path) -> None:
    target, _ = prepare(tmp_path)
    (target / "a").write_bytes(b"old")
    os.link(target / "a", target / "b")
    _fs.extract_tar(io.BytesIO(archive([F("a", b"new"), H("b", "a")])), target)
    assert (target / "a").read_bytes() == (target / "b").read_bytes() == b"new"
    assert (target / "a").stat().st_nlink == (target / "b").stat().st_nlink == 1


def test_a_hard_link_copy_takes_its_own_members_mode_and_time(tmp_path: Path) -> None:
    target, _ = prepare(tmp_path)
    link = member("h", tarfile.LNKTYPE, link="f", mode=0o4777, mtime=MTIME + 77)
    _fs.extract_tar(io.BytesIO(archive([F("f", b"x", mode=0o640), link])), target)
    assert stat.S_IMODE((target / "f").stat().st_mode) == 0o640
    assert (target / "f").stat().st_mtime == MTIME
    assert stat.S_IMODE((target / "h").stat().st_mode) == 0o755
    assert (target / "h").stat().st_mtime == MTIME + 77
    assert (target / "h").read_bytes() == b"x"


def test_a_hard_link_over_a_directory_is_refused(tmp_path: Path) -> None:
    target, _ = prepare(tmp_path)
    with pytest.raises(tarfile.FilterError, match="is a directory"):
        _fs.extract_tar(io.BytesIO(archive([F("f"), D("d"), F("d/x"), H("d", "f")])), target)
    assert (target / "d" / "x").exists()


# -- a realistic round trip -----------------------------------------------------------------------------------------


def _source_view(root: Path) -> dict[str, tuple[object, ...]]:
    found: dict[str, tuple[object, ...]] = {}
    for directory, directories, files in os.walk(root):
        for name in [*directories, *files]:
            path = Path(directory) / name
            info = path.lstat()
            relative = path.relative_to(root).as_posix()
            if stat.S_ISLNK(info.st_mode):
                found[relative] = ("link", os.readlink(path))
            elif stat.S_ISDIR(info.st_mode):
                found[relative] = ("dir", info.st_mtime_ns)
            else:
                found[relative] = ("file", stat.S_IMODE(info.st_mode), info.st_mtime_ns, path.read_bytes())
    return found


@WORKERS
@pytest.mark.parametrize("writer", ["tarfile", pytest.param("gnu-tar", marks=needs_tar)])
def test_a_job_tree_round_trips(tmp_path: Path, writer: str, workers: int) -> None:
    source = job_tree(tmp_path / "src")
    names = sorted(os.listdir(source))
    if writer == "tarfile":
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w|") as tar:
            for name in names:
                tar.add(source / name, arcname=name)
        data = buffer.getvalue()
    else:
        data = gnu_tar(source, names)
    target = tmp_path / "landing"
    target.mkdir()
    _fs.extract_tar(io.BytesIO(data), target, workers=workers)
    assert _source_view(target) == _source_view(source)
    copy = target / "job--c7f0b2" / "outputs" / "INCAR.copy"
    assert copy.stat().st_nlink == 1 and copy.read_text(encoding="utf-8") == "ENCUT = 520\n"
    assert os.access(target / "job--c7f0b2" / "run.sh", os.X_OK)


# -- single-file pulls ----------------------------------------------------------------------------------------------


def test_a_single_file_pull_writes_the_one_member_under_its_new_name(tmp_path: Path) -> None:
    target, _ = prepare(tmp_path)
    (target / "other").write_bytes(b"user file")
    _fs.extract_tar(
        io.BytesIO(archive([F("remote.txt", b"pulled", mode=0o755)])), target, rename=("remote.txt", "pulled.txt")
    )
    assert (target / "pulled.txt").read_bytes() == b"pulled"
    assert stat.S_IMODE((target / "pulled.txt").stat().st_mode) == 0o755
    assert sorted(os.listdir(target)) == ["other", "pulled.txt"]


@pytest.mark.parametrize(
    "members",
    [
        [D("remote.txt")],
        [S("remote.txt", "other")],
        [H("remote.txt", "other")],
        [member("remote.txt", tarfile.FIFOTYPE)],
        [F("another.txt")],
        [F("./remote.txt")],
        [F("remote.txt", b"pulled"), F("another.txt")],
        [F("remote.txt", b"pulled"), S("remote.txt", "other")],
    ],
    ids=["directory", "symlink", "hard-link", "fifo", "other-name", "other-spelling", "second-member", "then-link"],
)
def test_a_single_file_pull_refuses_every_other_member(tmp_path: Path, members: list[Member]) -> None:
    target, _ = prepare(tmp_path)
    (target / "other").write_bytes(b"user file")
    with pytest.raises(tarfile.FilterError, match="single-file pull"):
        _fs.extract_tar(io.BytesIO(archive(members)), target, rename=("remote.txt", "pulled.txt"))
    assert (target / "other").read_bytes() == b"user file"
    assert sorted(os.listdir(target)) in (["other"], ["other", "pulled.txt"])


def test_a_single_file_pull_refuses_a_symlink_at_the_new_name(tmp_path: Path) -> None:
    target, _ = prepare(tmp_path)
    (target / "sibling").write_bytes(b"original")
    (target / "pulled.txt").symlink_to("sibling")
    with pytest.raises(tarfile.OutsideDestinationError):
        _fs.extract_tar(io.BytesIO(archive([F("remote.txt", b"pwned")])), target, rename=("remote.txt", "pulled.txt"))
    assert (target / "sibling").read_bytes() == b"original"


# -- names, modes and errors ----------------------------------------------------------------------------------------


def test_the_full_path_bound_is_exact(tmp_path: Path) -> None:
    target = tmp_path / "landing"
    target.mkdir()
    # PATH_MAX counts the separator after the target and the terminating NUL: the longest name is one byte less.
    longest = 4096 - len(os.fsencode(str(target))) - 1 - 1
    components = ["c" * 200] * ((longest - 1) // 201)
    fitting = "/".join([*components, "f" * (longest - 201 * len(components))])
    assert len(os.fsencode(str(target))) + 1 + len(fitting) == 4095
    _fs.extract_tar(io.BytesIO(archive([F(fitting)])), target)
    with pytest.raises(tarfile.FilterError, match="reaches 4096 bytes"):
        _fs.extract_tar(io.BytesIO(archive([F(fitting + "x")])), target)


def test_the_depth_bound_is_exact(tmp_path: Path) -> None:
    target = tmp_path / "landing"
    target.mkdir()
    _fs.extract_tar(io.BytesIO(archive([F("a/" * 255 + "f")])), target)
    with pytest.raises(tarfile.FilterError, match="deeper than 256"):
        _fs.extract_tar(io.BytesIO(archive([F("a/" * 256 + "f")])), target)


def test_the_mode_rule_is_the_data_filters(tmp_path: Path) -> None:
    for mode in range(0o10000):
        info = tarfile.TarInfo("f")
        info.mode = mode
        assert _fs._data_mode(mode) == tarfile.data_filter(info, str(tmp_path)).mode, oct(mode)


def test_mode_and_time_failures_are_not_fatal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def failing(*args: object, **kwargs: object) -> None:
        raise PermissionError(errno.EPERM, "not permitted")

    monkeypatch.setattr(os, "fchmod", failing)
    monkeypatch.setattr(os, "utime", failing)
    target = tmp_path / "landing"
    target.mkdir()
    _fs.extract_tar(io.BytesIO(archive([D("d"), F("d/f", b"x"), H("h", "d/f")])), target)
    assert (target / "d" / "f").read_bytes() == (target / "h").read_bytes() == b"x"


def test_every_refusal_is_a_tar_error(tmp_path: Path) -> None:
    refused = 0
    for case in CASES:
        if case.verdict in (ACCEPT, COPY) or case.verdict in RELAXATIONS or case.error is OSError:
            continue
        target, outside = prepare(tmp_path / case.name)
        if case.setup is not None:
            case.setup(target, outside)
        with pytest.raises(tarfile.TarError):
            _fs.extract_tar(io.BytesIO(archive(case.build(outside))), target)
        refused += 1
    assert refused > 30


def test_a_refused_stream_keeps_what_was_written_before(tmp_path: Path) -> None:
    target, _ = prepare(tmp_path)
    with pytest.raises(tarfile.OutsideDestinationError):
        _fs.extract_tar(io.BytesIO(archive([D("d"), F("d/first"), F("../escape"), F("later")])), target)
    assert (target / "d" / "first").exists() and not (target / "later").exists()


def test_a_truncated_stream_is_a_tar_error(tmp_path: Path) -> None:
    data = archive([F("big", b"x" * 5000)], tarfile.USTAR_FORMAT)[:2048]
    with pytest.raises(tarfile.ReadError):
        _fs.extract_tar(io.BytesIO(data), tmp_path)


def test_a_full_filesystem_propagates_as_os_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def full(descriptor: int, data: bytes) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(_fs, "write_all", full)
    before = open_descriptors()
    for members in ([F("f", b"x")], [F("f", b"x"), H("h", "f")]):
        with pytest.raises(OSError) as raised:
            _fs.extract_tar(io.BytesIO(archive(members)), tmp_path)
        assert raised.value.errno == errno.ENOSPC and not isinstance(raised.value, tarfile.TarError)
    assert open_descriptors() == before


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores permissions")
def test_a_permission_error_propagates_as_os_error(tmp_path: Path) -> None:
    (tmp_path / "ro").mkdir(mode=0o500)
    try:
        with pytest.raises(PermissionError):
            _fs.extract_tar(io.BytesIO(archive([F("ro/f")])), tmp_path)
        with pytest.raises(PermissionError):
            _fs.extract_tar(io.BytesIO(archive([D("ro/d")])), tmp_path)
    finally:
        (tmp_path / "ro").chmod(0o700)


# -- call budget and descriptors -----------------------------------------------------------------------------------

_AUDITED = ("open", "os.mkdir", "os.symlink", "os.chmod", "os.utime", "os.remove", "os.rename", "os.link")
_SPIED = ("open", "stat", "lstat", "fstat", "fchmod", "close", "mkdir", "utime", "unlink", "symlink", "readlink")
_audit: list[Counter[str]] = []


def _audit_hook(event: str, args: tuple[object, ...]) -> None:
    if _audit and event in _AUDITED:
        _audit[-1][event] += 1


sys.addaudithook(_audit_hook)


@dataclass
class Calls:
    """The ``os`` calls and audit events of one extraction."""

    spied: Counter[str]
    audited: Counter[str]


def counted(data: bytes, target: Path, monkeypatch: pytest.MonkeyPatch) -> Calls:
    spied: Counter[str] = Counter()

    def spy(name: str) -> Callable[..., object]:
        real = getattr(os, name)

        def counting(*args: object, **kwargs: object) -> object:
            spied[name] += 1
            return real(*args, **kwargs)

        return counting

    with monkeypatch.context() as patch:
        for name in _SPIED:
            patch.setattr(os, name, spy(name))
        _audit.append(Counter())
        try:
            _fs.extract_tar(io.BytesIO(data), target)
        finally:
            audited = _audit.pop()
    return Calls(spied, audited)


def test_a_regular_member_in_an_existing_directory_costs_one_open_fchmod_utime_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    shorter = counted(archive([D("d"), F("d/f1", b"1")]), tmp_path / "a", monkeypatch)
    longer = counted(archive([D("d"), F("d/f1", b"1"), F("d/f2", b"2")]), tmp_path / "b", monkeypatch)
    # Exactly the leaf's open, fchmod, utime and close: lower and upper bounds at once.
    assert longer.spied - shorter.spied == Counter({"open": 1, "fchmod": 1, "utime": 1, "close": 1})
    assert shorter.spied - longer.spied == Counter()
    assert longer.audited - shorter.audited == Counter({"open": 1, "os.chmod": 1, "os.utime": 1})
    assert shorter.audited - longer.audited == Counter()
    # The whole extraction: target, d, two files; one mkdir; d's time restored; no stat of any kind anywhere.
    assert longer.spied == Counter({"open": 4, "close": 4, "mkdir": 1, "fchmod": 2, "utime": 3})
    assert longer.audited == Counter({"open": 4, "os.mkdir": 1, "os.chmod": 2, "os.utime": 3})
    assert (tmp_path / "b" / "d" / "f2").read_bytes() == b"2"


def open_descriptors() -> int:
    return len(os.listdir("/proc/self/fd"))


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc")
def test_no_descriptor_leaks_after_success_or_refusal(tmp_path: Path) -> None:
    before = open_descriptors()
    many = [member for index in range(100) for member in (D(f"d{index}"), F(f"d{index}/a"))]
    _fs.extract_tar(io.BytesIO(archive(many)), tmp_path)
    assert open_descriptors() == before
    for index, members in enumerate(
        [
            [*many[:20], F("../escape")],
            [*many[:20], F("d1/a/x")],
            [*many[:20], H("h", "d3")],
            [*many[:20], F("f"), H("d4", "f")],
            [*many[:20], S("d5", "x")],
            [*many[:20], member("d6/c", tarfile.CHRTYPE)],
        ]
    ):
        landing = tmp_path / f"refused{index}"
        landing.mkdir()
        with pytest.raises(tarfile.FilterError):
            _fs.extract_tar(io.BytesIO(archive(members)), landing)
        assert open_descriptors() == before


def test_at_most_64_directories_stay_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    opened: set[int] = set()
    peak = [0]
    real_open, real_close = os.open, os.close

    def spy_open(*args: object, **kwargs: object) -> int:
        descriptor = real_open(*args, **kwargs)  # type: ignore[arg-type]
        opened.add(descriptor)
        peak[0] = max(peak[0], len(opened))
        return descriptor

    def spy_close(descriptor: int) -> None:
        opened.discard(descriptor)
        real_close(descriptor)

    members = [D(f"d{index}") for index in range(100)]
    members += [F(f"d{index}/a/b/c", b"first") for index in range(100)]
    members += [F(f"d{index}/a/b/c", b"second") for index in reversed(range(100))]
    data = archive(members)
    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "close", spy_close)
    _fs.extract_tar(io.BytesIO(data), tmp_path)
    monkeypatch.undo()
    assert opened == set()
    assert 64 < peak[0] <= 64 + 2  # the cache, the target and one file
    assert all((tmp_path / f"d{index}" / "a" / "b" / "c").read_bytes() == b"second" for index in range(100))


# -- the adapter --------------------------------------------------------------------------------------------------


def test_the_adapter_extraction_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[object, ...]] = []

    def spy(stream: IO[bytes], target: Path, *, rename: tuple[str, str] | None = None) -> None:
        calls.append((stream, target, rename))

    monkeypatch.setattr(_fs, "extract_tar", spy)
    stream = io.BytesIO(b"")
    adapter_runtime._extract(stream, tmp_path, rename=("a", "b"))
    adapter_runtime._extract(stream, tmp_path)
    assert calls == [(stream, tmp_path, ("a", "b")), (stream, tmp_path, None)]


# -- the end of the stream -----------------------------------------------------------------------------------------


def end_of_members(data: bytes) -> int:
    """The offset of the end-of-archive block: where the header after the last member starts."""

    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as reader:
        for info in reader:
            if info.isreg():
                reader.extractfile(info).read()  # type: ignore[union-attr]
        return reader.offset


def test_tarfile_reports_how_a_stream_ended() -> None:
    # The contract the end-of-archive check relies on, checked on every interpreter: after the iteration, the
    # stream position minus the next header's offset is a whole block after the end marker, nothing after a cut at
    # a member boundary, and the part read after a cut inside a header.
    data = archive([F("a", b"x" * 700), D("d"), F("b", b"")])
    end = end_of_members(data)

    def consumed(cut: int) -> int:
        with tarfile.open(fileobj=io.BytesIO(data[:cut]), mode="r|") as reader:
            for info in reader:
                if info.isreg():
                    reader.extractfile(info).read()  # type: ignore[union-attr]
            return cast(Any, reader.fileobj).pos - reader.offset

    assert consumed(len(data)) == consumed(end + 512) == 512
    assert consumed(end) == 0
    assert consumed(end - 512) == 0  # the boundary before the last member, a header-only file
    assert [consumed(end + part) for part in (1, 100, 511)] == [1, 100, 511]


def test_every_cut_of_a_stream_is_refused(tmp_path: Path) -> None:
    data = archive([D("d"), F("d/a", b"x" * 700), S("d/s", "a"), H("h", "d/a"), F("e", b""), D("f")])
    complete = end_of_members(data) + 512
    before = open_descriptors()
    for cut in [*range(complete + 520), len(data)]:
        target = tmp_path / str(cut)
        target.mkdir()
        try:
            _fs.extract_tar(io.BytesIO(data[:cut]), target)
        except (tarfile.TarError, EOFError):
            accepted = False
        else:
            accepted = True
        assert accepted == (cut >= complete), cut
        shutil.rmtree(target)  # thousands of landings: keep the footprint small
    assert open_descriptors() == before


def test_a_stream_cut_at_a_member_boundary_names_the_missing_marker(tmp_path: Path) -> None:
    data = archive([F("a", b"x"), F("b", b"y")])
    with pytest.raises(tarfile.ReadError, match="end-of-archive marker"):
        _fs.extract_tar(io.BytesIO(data[: end_of_members(data)]), tmp_path)
    assert (tmp_path / "b").read_bytes() == b"y"  # what came before stays, as for any refusal


# -- hard-link amplification, hostile names, times and modes -------------------------------------------------------


@pytest.mark.parametrize("factor", [2, 8])
def test_hard_link_copies_are_bounded_by_the_streams_own_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, factor: int
) -> None:
    # A file under factor + 1 names passes (factor 2: three names; the real factor 8: nine); one more is refused.
    monkeypatch.setattr(_fs, "_HARD_LINK_FACTOR", factor)
    monkeypatch.setattr(_fs, "_HARD_LINK_ALLOWANCE", 10)
    target, _ = prepare(tmp_path / "passing")
    links = [H(f"l{index}", "a") for index in range(factor)]
    _fs.extract_tar(io.BytesIO(archive([F("a", b"x" * 100), *links])), target)
    assert all((target / f"l{index}").read_bytes() == b"x" * 100 for index in range(factor))
    target, _ = prepare(tmp_path / "refused")
    data = archive([F("a", b"x" * 100), *links, H("over", "a")])
    with pytest.raises(tarfile.FilterError, match=f"would write more than {factor * 100 + 10} bytes, {factor} times"):
        _fs.extract_tar(io.BytesIO(data), target)
    assert (target / f"l{factor - 1}").exists() and not (target / "over").exists()
    # More file bytes from the stream raise the bound.
    target, _ = prepare(tmp_path / "padded")
    _fs.extract_tar(io.BytesIO(archive([F("a", b"x" * 100), *links, F("pad", b"p" * 100), H("over", "a")])), target)
    assert (target / "over").read_bytes() == b"x" * 100


def test_a_nul_in_a_name_is_refused_as_a_filter_error(tmp_path: Path) -> None:
    for members in (
        [member("x", data=b"x", pax={"path": "a\0b"})],
        [member("x", tarfile.DIRTYPE, pax={"path": "d\0"})],
    ):
        with pytest.raises(tarfile.FilterError, match="NUL character"):
            _fs.extract_tar(io.BytesIO(archive(members)), tmp_path)
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("text", ["", "a\0b"], ids=["empty", "nul"])
def test_a_symlink_with_an_unusable_link_text_is_refused(tmp_path: Path, text: str) -> None:
    link = member("s", tarfile.SYMTYPE, pax={"linkpath": text}) if text else S("s", "")
    with pytest.raises(tarfile.FilterError, match="empty link text or a NUL"):
        _fs.extract_tar(io.BytesIO(archive([link])), tmp_path)
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("time", ["nan", "inf", "1e300", "-1e300"])
def test_an_unusable_time_is_not_fatal(tmp_path: Path, time: str) -> None:
    members = [
        member(".", tarfile.DIRTYPE, pax={"mtime": time}),
        member("d", tarfile.DIRTYPE, pax={"mtime": time}),
        member("d/f", data=b"x", pax={"mtime": time}),
        member("h", tarfile.LNKTYPE, link="d/f", pax={"mtime": time}),
        F("after"),
    ]
    _fs.extract_tar(io.BytesIO(archive(members)), tmp_path)
    assert (tmp_path / "d" / "f").read_bytes() == (tmp_path / "h").read_bytes() == b"x"
    assert (tmp_path / "after").exists()


@pytest.mark.parametrize("mode", [0o7777, 0o4755, 0o2755, 0o1777, 0o777, 0o711, 0o500, 0o000])
def test_directory_members_get_the_default_mode(tmp_path: Path, mode: int) -> None:
    umask = os.umask(0o022)
    os.umask(umask)
    _fs.extract_tar(io.BytesIO(archive([D("x", mode=mode), D("x/y", mode=mode), F("x/y/f")])), tmp_path)
    for path in (tmp_path / "x", tmp_path / "x" / "y"):
        assert stat.S_IMODE(path.stat().st_mode) == 0o777 & ~umask, oct(mode)


class Tripwire(io.BytesIO):
    """A stream that runs *action* once, as it serves the read that reaches offset *at* (a concurrent actor)."""

    def __init__(self, data: bytes, at: int, action: Callable[[], None]) -> None:
        super().__init__(data)
        self.at = at
        self.action: Callable[[], None] | None = action

    def read(self, size: int | None = -1, /) -> bytes:
        start = self.tell()
        chunk = super().read(size)
        if self.action is not None and start + len(chunk) > self.at:
            action, self.action = self.action, None
            action()
        return chunk


def _swapped_before_the_link(tmp_path: Path, swap: Callable[[Path, Path], None]) -> tuple[Path, Exception | None]:
    """Extract ``a`` (64 KiB, so that the link's header comes in a later read) and the hard link ``h`` to it, with
    ``a`` swapped by *swap* while it is being written, after its name was recorded as written."""

    target, outside = prepare(tmp_path)
    data = archive([F("a", b"x" * (64 << 10)), H("h", "a")])
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as reader:
        link = next(info for info in reader if info.islnk()).offset
    try:
        _fs.extract_tar(Tripwire(data, link, lambda: swap(target, outside)), target)
    except Exception as exc:  # the caller asserts on it
        return target, exc
    return target, None


def test_a_hard_link_source_swapped_for_a_symlink_is_not_followed(tmp_path: Path) -> None:
    def swap(target: Path, outside: Path) -> None:
        os.unlink(target / "a")
        (target / "a").symlink_to(outside / "secret")

    target, error = _swapped_before_the_link(tmp_path, swap)
    assert isinstance(error, tarfile.OutsideDestinationError), repr(error)
    assert not (target / "h").exists() and outside_untouched(tmp_path)


def test_a_hard_link_source_swapped_for_a_fifo_is_refused(tmp_path: Path) -> None:
    def swap(target: Path, outside: Path) -> None:
        os.unlink(target / "a")
        os.mkfifo(target / "a")

    target, error = _swapped_before_the_link(tmp_path, swap)
    assert isinstance(error, tarfile.FilterError) and "which is not a regular file" in str(error), repr(error)
    assert not (target / "h").exists()


def test_a_file_replaced_by_a_symlink_member_is_no_longer_a_hard_link_source(tmp_path: Path) -> None:
    with pytest.raises(tarfile.FilterError) as raised:
        _fs.extract_tar(io.BytesIO(archive([F("a"), S("a", "b"), H("h", "a")])), tmp_path)
    assert type(raised.value) is tarfile.FilterError
    assert "not a regular file this stream extracted" in str(raised.value)


def test_a_hard_link_over_an_outside_pointing_symlink_replaces_the_symlink(tmp_path: Path) -> None:
    target, outside = prepare(tmp_path)
    (target / "h").symlink_to(outside / "secret")
    _fs.extract_tar(io.BytesIO(archive([F("a", b"x"), H("h", "a")])), target)
    assert not (target / "h").is_symlink() and (target / "h").read_bytes() == b"x"
    assert outside_untouched(tmp_path)


def test_a_regular_member_writes_through_a_hard_link_planted_in_the_landing(tmp_path: Path) -> None:
    # Documented behaviour, as with tarfile: the landing is trusted, and an existing regular file at a member's
    # name is written in place (O_TRUNC), so a hard link a trusted actor planted there is written through.
    target, outside = prepare(tmp_path)
    os.link(outside / "secret", target / "a")
    _fs.extract_tar(io.BytesIO(archive([F("a", b"new")])), target)
    assert (outside / "secret").read_bytes() == b"new"


@pytest.mark.parametrize("fmt", list(FORMATS), ids=list(FORMATS.values()))
def test_names_that_are_not_utf_8_are_extracted_as_bytes(tmp_path: Path, fmt: int) -> None:
    members = [D("\udcff"), F("\udcff/\udcfe", b"x"), S("\udcfd", "\udcfe"), H("\udcfc", "\udcff/\udcfe")]
    _fs.extract_tar(io.BytesIO(archive(members, fmt)), tmp_path)
    root = os.fsencode(tmp_path)
    assert sorted(os.listdir(root)) == [b"\xfc", b"\xfd", b"\xff"]
    assert os.listdir(os.path.join(root, b"\xff")) == [b"\xfe"]
    assert os.readlink(os.path.join(root, b"\xfd")) == b"\xfe"
    assert Path(os.fsdecode(os.path.join(root, b"\xfc"))).read_bytes() == b"x"


@pytest.fixture(autouse=True)
def _no_leftover_audit() -> Iterator[None]:
    yield
    assert _audit == []


# -- pooled extraction (workers > 1) ---------------------------------------------------------------------------------

#: How long a pooled test waits for something that should happen at once before calling it a failure.
PATIENCE = 10.0


@dataclass
class Writes:
    """The worker writes of pooled extractions: how many run now, the most at once, and each one's span."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    running: int = 0
    peak: int = 0
    started: list[str] = field(default_factory=list)
    spans: list[tuple[str, float, float]] = field(default_factory=list)
    #: Seconds a worker sleeps before creating the named file.
    delay: dict[str, float] = field(default_factory=dict)
    #: The thread that wrote each name.
    threads: dict[str, int] = field(default_factory=dict)

    def span(self, name: str) -> list[tuple[float, float]]:
        return [(begin, end) for found, begin, end in self.spans if found == name]


@pytest.fixture
def writes(monkeypatch: pytest.MonkeyPatch) -> Writes:
    record = Writes()
    real = _fs._write_member

    def tracked(landing: Any, task: Any) -> None:
        name = "/".join(task.parts)
        with record.lock:
            record.running += 1
            record.peak = max(record.peak, record.running)
            record.started.append(name)
            record.threads[name] = threading.get_ident()
        begin = time.monotonic()
        try:
            time.sleep(record.delay.get(name, 0))
            real(landing, task)
        finally:
            with record.lock:
                record.running -= 1
                record.spans.append((name, begin, time.monotonic()))

    monkeypatch.setattr(_fs, "_write_member", tracked)
    return record


def gated_writes(monkeypatch: pytest.MonkeyPatch, gates: dict[bytes, Callable[[], object]]) -> None:
    """Run ``gates[prefix]`` before writing data that starts with *prefix* (a sleep, a wait, a failure)."""

    real = _fs.write_all

    def write(descriptor: int, data: bytes) -> None:
        for prefix, gate in gates.items():
            if bytes(data[: len(prefix)]) == prefix:
                gate()
        real(descriptor, data)

    monkeypatch.setattr(_fs, "write_all", write)


def slow(seconds: float = 0.2) -> Callable[[], None]:
    return lambda: time.sleep(seconds)


def full_disk() -> None:
    raise OSError(errno.ENOSPC, "No space left on device")


def pooled(members: Sequence[Member], target: Path, workers: int = 4) -> None:
    _fs.extract_tar(io.BytesIO(archive(members)), target, workers=workers)


@pytest.fixture
def default_sigint() -> Iterator[None]:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    yield
    signal.signal(signal.SIGINT, previous)


def test_pooled_writes_run_at_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes) -> None:
    # Without overlap the serialization tests below would pass vacuously.
    gated_writes(monkeypatch, {b"slow": slow(0.05)})
    pooled([F(f"f{index}", b"slow") for index in range(16)], tmp_path)
    assert writes.peak >= 2 and len(writes.started) == 16 and writes.running == 0


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc")
def test_a_failing_member_stops_the_pooled_extraction_with_nothing_running_and_no_descriptor_left(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow(0.05), b"fail": full_disk})
    members = [F(f"d/f{index}", b"fail" if index == 5 else b"slow") for index in range(40)]
    before = open_descriptors()
    with pytest.raises(OSError) as raised:
        pooled([D("d"), *members], tmp_path)
    assert raised.value.errno == errno.ENOSPC and not isinstance(raised.value, tarfile.TarError)
    assert writes.running == 0
    started = len(writes.started)
    assert "d/f5" in writes.started and started < 40
    time.sleep(0.2)
    assert len(writes.started) == started  # nothing started after the raise
    assert open_descriptors() == before


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc")
def test_a_worker_failing_while_the_reader_waits_on_a_full_queue_wakes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    # Two workers, so at most 8 tasks handed out: f0 (failing) and f1 run parked, f2..f7 queued, the reader waits
    # for room at f8 when f0 fails.
    blocked, fail, release = threading.Event(), threading.Event(), threading.Event()
    real_room = _pool.Feed.room

    def room(feed: Any, weight: int) -> bool:
        free = real_room(feed, weight)
        if not free:
            blocked.set()
        return free

    def failing() -> None:
        fail.wait(PATIENCE)
        full_disk()

    monkeypatch.setattr(_pool.Feed, "room", room)
    gated_writes(monkeypatch, {b"fail": failing, b"park": lambda: release.wait(PATIENCE)})
    members = [F(f"f{index}", b"fail" if index == 0 else b"park") for index in range(20)]
    seen: list[bool] = []

    def orchestrate() -> None:
        seen.append(blocked.wait(PATIENCE))  # the reader is waiting for room now
        fail.set()
        time.sleep(0.3)
        release.set()

    helper = threading.Thread(target=orchestrate)
    before = open_descriptors()
    helper.start()
    began = time.monotonic()
    try:
        with pytest.raises(OSError) as raised:
            pooled(members, tmp_path, workers=2)
    finally:
        fail.set()
        release.set()
        helper.join()
    assert time.monotonic() - began < PATIENCE
    assert seen == [True] and raised.value.errno == errno.ENOSPC
    assert sorted(writes.started) == ["f0", "f1"] and writes.running == 0
    assert open_descriptors() == before  # the six queued tasks' descriptors were closed untaken
    assert sorted(os.listdir(tmp_path)) == ["f0", "f1"]  # f0 created before its write failed, as without workers


def test_normalized_duplicate_names_wait_for_each_other(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow()})
    pooled([F("a/b", b"slow first"), F("./a//b", b"second")], tmp_path)
    (_, first_end), (second_begin, _) = writes.span("a/b")
    assert first_end <= second_begin
    assert (tmp_path / "a" / "b").read_bytes() == b"second"


def test_case_variant_names_wait_for_each_other(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow()})
    pooled([F("Dir/NAME", b"slow upper"), F("dir/name", b"lower")], tmp_path)
    ((_, upper_end),) = writes.span("Dir/NAME")
    ((lower_begin, _),) = writes.span("dir/name")
    assert upper_end <= lower_begin


def test_a_symlink_member_waits_for_a_queued_file_of_the_same_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow()})
    pooled([F("x", b"slow"), S("x", "elsewhere")], tmp_path)
    assert os.readlink(tmp_path / "x") == "elsewhere"


def test_a_member_below_a_name_queued_as_a_file_is_refused_as_without_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    writes.delay["a"] = 0.2  # so that, without the wait, "a" would be made a directory before the file is created
    errors = []
    for workers in (1, 4):
        target = tmp_path / str(workers)
        target.mkdir()
        with pytest.raises(tarfile.FilterError) as raised:
            pooled([F("a", b"slow"), F("a/b", b"x")], target, workers)
        errors.append((type(raised.value), str(raised.value).replace(str(target), "<target>")))
        assert (target / "a").read_bytes() == b"slow"
    assert errors[0] == errors[1] and "which is not a directory" in errors[0][1]


def test_a_hard_link_waits_for_its_queued_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow()})
    data = b"slow" + bytes(range(256)) * 40
    pooled([F("a", data), H("h", "a"), H("g", "h")], tmp_path)
    assert (tmp_path / "h").read_bytes() == (tmp_path / "g").read_bytes() == data
    assert (tmp_path / "h").stat().st_nlink == 1


def test_directory_times_are_restored_after_every_pooled_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow(0.02)})
    members = [D(".", MTIME + 1), D("d", MTIME + 2), D("d/e", MTIME + 3)]
    members += [F(f"d/f{index}", b"slow") for index in range(10)] + [F(f"d/e/g{index}", b"x") for index in range(10)]
    snapshots = []
    for workers in (1, 8):
        target = tmp_path / str(workers)
        target.mkdir()
        pooled(members, target, workers)
        assert (target / "d").stat().st_mtime == MTIME + 2 and (target / "d" / "e").stat().st_mtime == MTIME + 3
        assert target.stat().st_mtime == MTIME + 1
        snapshots.append(snapshot(target, target, {".", "d", "d/e"}))
    assert snapshots[0] == snapshots[1] and writes.peak >= 2


@pytest.mark.parametrize("failing_start", [1, 3])
def test_failed_thread_starts_leave_the_writes_to_the_started_threads_or_this_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes, failing_start: int
) -> None:
    original = threading.Thread.start
    starts = Counter[str]()

    def start(thread: threading.Thread) -> None:
        starts["calls"] += 1
        if starts["calls"] >= failing_start:
            raise RuntimeError("can't start new thread")
        original(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    members = [D("d"), *(F(f"d/f{index}", bytes([index])) for index in range(30)), H("h", "d/f3")]
    pooled(members, tmp_path, workers=8)
    monkeypatch.undo()
    assert sorted(writes.started) == sorted(f"d/f{index}" for index in range(30))
    assert all((tmp_path / "d" / f"f{index}").read_bytes() == bytes([index]) for index in range(30))
    assert (tmp_path / "h").read_bytes() == bytes([3])


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc")
def test_two_real_interrupts_during_a_pooled_extraction_wait_for_every_running_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    writes: Writes,
    default_sigint: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release = threading.Event()
    gated_writes(monkeypatch, {b"park": lambda: release.wait(PATIENCE)})
    main = threading.main_thread().ident
    assert main is not None
    at_raise: list[int] = []

    def interrupt() -> None:
        deadline = time.monotonic() + PATIENCE
        while len(writes.started) < 4 and time.monotonic() < deadline:
            time.sleep(0.001)
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)  # the reader is now draining: the writes are still parked
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)
        release.set()

    helper = threading.Thread(target=interrupt)
    before = open_descriptors()
    helper.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            try:
                pooled([F(f"f{index}", b"park") for index in range(40)], tmp_path)
            finally:
                at_raise.append(writes.running)
    finally:
        release.set()
        helper.join()
    assert at_raise == [0] and len(writes.started) == 4 and len(writes.spans) == 4
    time.sleep(0.1)
    assert len(writes.started) == 4  # no write starts after the stop
    assert open_descriptors() == before  # the queued tasks' descriptors were closed untaken
    assert capsys.readouterr().err.count("waiting for 4 running operations to finish") == 1
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_a_custom_sigint_handler_that_returns_lets_the_extraction_finish_here(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    calls: list[int] = []
    release = threading.Event()
    gated_writes(monkeypatch, {b"park": lambda: release.wait(PATIENCE)})
    main = threading.main_thread().ident
    assert main is not None

    def interrupt() -> None:
        deadline = time.monotonic() + PATIENCE
        while len(writes.started) < 4 and time.monotonic() < deadline:
            time.sleep(0.001)
        signal.pthread_kill(main, signal.SIGINT)
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)
        release.set()

    previous = signal.signal(signal.SIGINT, lambda signum, _frame: calls.append(signum))
    helper = threading.Thread(target=interrupt)
    helper.start()
    try:
        members = [D("d"), *(F(f"d/f{index}", b"park%d" % index) for index in range(40)), H("h", "d/f39")]
        pooled(members, tmp_path)
    finally:
        signal.signal(signal.SIGINT, previous)
        release.set()
        helper.join()
    assert calls == [signal.SIGINT]
    assert sorted(writes.started) == sorted(f"d/f{index}" for index in range(40))
    assert all((tmp_path / "d" / f"f{index}").read_bytes() == b"park%d" % index for index in range(40))
    assert (tmp_path / "h").read_bytes() == b"park39"


def _receiving(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, members: Sequence[Member]) -> tuple[Workspace, Path]:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.configure_settings({"io.workers": 4})
    data = archive(members)
    monkeypatch.setattr(sys, "stdin", types.SimpleNamespace(buffer=io.BytesIO(data)))
    return workspace, workspace.control / "transfers" / "incoming" / "landing"


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc")
def test_receive_tar_discards_the_landing_only_after_every_write_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow(0.2), b"fail": full_disk})
    members = [D("job"), F("job/a", b"slow"), F("job/b", b"slow"), F("job/c", b"fail"), F("job/d", b"slow")]
    workspace, landing = _receiving(tmp_path, monkeypatch, members)
    assert workspace.io_workers == 4
    real_discard = _fs.discard
    running_at_discard: list[int] = []

    def discard(target: _fs.Loc, **kwargs: Any) -> None:
        running_at_discard.append(writes.running)
        real_discard(target, **kwargs)

    monkeypatch.setattr(_fs, "discard", discard)
    before = open_descriptors()
    with pytest.raises(ValueError, match="refused the stream"):
        receive_tar(workspace, landing)
    assert running_at_discard == [0] and writes.peak >= 2
    assert not landing.exists() and open_descriptors() == before


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc")
def test_receive_tar_interrupted_raises_only_after_every_write_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes, default_sigint: None
) -> None:
    release = threading.Event()
    gated_writes(monkeypatch, {b"park": lambda: release.wait(PATIENCE)})
    workspace, landing = _receiving(tmp_path, monkeypatch, [D("job"), *(F(f"job/f{i}", b"park") for i in range(30))])
    main = threading.main_thread().ident
    assert main is not None

    def interrupt() -> None:
        deadline = time.monotonic() + PATIENCE
        while len(writes.started) < 4 and time.monotonic() < deadline:
            time.sleep(0.001)
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)
        release.set()

    helper = threading.Thread(target=interrupt)
    before = open_descriptors()
    helper.start()
    at_raise: list[int] = []
    try:
        with pytest.raises(KeyboardInterrupt):
            try:
                receive_tar(workspace, landing)
            finally:
                at_raise.append(writes.running)
    finally:
        release.set()
        helper.join()
    # An interrupt is not a refused stream: the landing stays (as without workers), with no write left running.
    assert at_raise == [0] and len(writes.spans) == len(writes.started) == 4
    assert open_descriptors() == before and landing.is_dir()


def test_a_pooled_refusal_before_a_reading_thread_refusal_is_the_one_raised(tmp_path: Path, writes: Writes) -> None:
    # The worker meets "d" (a directory in the landing) late; the reader refuses "../escape" first.
    writes.delay["d"] = 0.3
    errors = []
    for workers in (1, 4):
        target = tmp_path / str(workers)
        (target / "d").mkdir(parents=True)
        with pytest.raises(tarfile.FilterError) as raised:
            pooled([F("first"), F("d", b"x"), F("../escape")], target, workers)
        errors.append((type(raised.value), str(raised.value).replace(str(target), "<target>")))
    assert errors[0] == errors[1] and errors[0][0] is tarfile.FilterError and "'d'" in errors[0][1]
    assert sorted(writes.started) == ["d", "first"]  # both written by workers in the pooled run


def test_members_handed_out_before_a_reading_thread_refusal_are_all_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    gated_writes(monkeypatch, {b"slow": slow(0.05)})
    members = [F(f"f{index}", b"slow %d" % index) for index in range(10)]
    with pytest.raises(tarfile.OutsideDestinationError):
        pooled([*members, F("../escape")], tmp_path, workers=2)
    assert all((tmp_path / f"f{index}").read_bytes() == b"slow %d" % index for index in range(10))
    assert len(writes.spans) == 10 and writes.running == 0


def _pending_peaks(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int, int]]:
    """Record, after every submission, the feed's pending tasks, their weight and the tasks still alive."""

    import weakref

    peaks: list[tuple[int, int, int]] = []
    alive: list[weakref.ref[Any]] = []
    real_submit = _pool.Feed.submit

    def submit(self: Any, task: Any, weight: int) -> int:
        alive.append(weakref.ref(task))
        index = real_submit(self, task, weight)
        with self.condition:
            peaks.append((self.pending, self.pending_weight, sum(ref() is not None for ref in alive)))
        return index

    monkeypatch.setattr(_pool.Feed, "submit", submit)
    return peaks


def test_the_byte_bound_limits_what_is_handed_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    monkeypatch.setattr(_fs, "_POOLED_BYTES", 2500)
    gated_writes(monkeypatch, {b"slow": slow(0.01)})
    peaks = _pending_peaks(monkeypatch)
    pooled([F(f"f{index}", b"slow" + bytes(996)) for index in range(30)], tmp_path, workers=8)
    assert max(weight for _, weight, _ in peaks) == 2000  # a third 1000-byte member would pass 2500
    assert max(pending for pending, _, _ in peaks) == 2
    assert len(writes.spans) == 30


def test_a_long_pooled_stream_keeps_only_the_pending_members_in_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    import tracemalloc

    gated_writes(monkeypatch, {b"slow": slow(0.001)})
    peaks = _pending_peaks(monkeypatch)
    data = archive([F(f"f{index}", b"slow" + bytes(64 << 10)) for index in range(300)])  # 19.7 MB of members
    tracemalloc.start()
    try:
        _fs.extract_tar(io.BytesIO(data), tmp_path, workers=4)
        current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    # At most 16 pending (4 per worker), the 4 being written and the reader's own: never the whole stream, whose
    # members' data alone would be 19.7 MB.
    assert peak < 6 << 20 and current < 1 << 20, (peak, current)
    assert max(alive for _, _, alive in peaks) <= 16 + 4 + 2
    assert max(weight for _, weight, _ in peaks) <= 64 << 20 and len(writes.spans) == 300


def test_members_above_1_mib_stay_in_the_reading_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    real = _fs._extract_regular
    seen: dict[str, int] = {}

    def extract_regular(landing: Any, archive: Any, info: tarfile.TarInfo, parts: Any) -> None:
        seen[info.name] = threading.get_ident()
        real(landing, archive, info, parts)

    monkeypatch.setattr(_fs, "_extract_regular", extract_regular)
    caller = threading.get_ident()
    members = [F("big", b"b" * ((1 << 20) + 1)), F("edge", b"e" * (1 << 20)), F("small", b"s")]
    pooled(members, tmp_path)
    assert seen == {"big": caller}
    assert sorted(writes.threads) == ["edge", "small"] and caller not in writes.threads.values()
    assert (tmp_path / "big").stat().st_size == (1 << 20) + 1 and (tmp_path / "edge").stat().st_size == 1 << 20


@pytest.mark.parametrize("workers", [1, 4])
def test_pooled_bytes_count_for_the_hard_link_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes, workers: int
) -> None:
    monkeypatch.setattr(_fs, "_HARD_LINK_FACTOR", 1)
    monkeypatch.setattr(_fs, "_HARD_LINK_ALLOWANCE", 0)
    (tmp_path / "ok").mkdir()
    pooled([F("a", bytes(100)), H("h", "a")], tmp_path / "ok", workers)
    assert (tmp_path / "ok" / "h").read_bytes() == bytes(100)
    (tmp_path / "over").mkdir()
    with pytest.raises(tarfile.FilterError, match="hard-link copies would write more than 100 bytes"):
        pooled([F("a", bytes(100)), H("h", "a"), H("g", "a")], tmp_path / "over", workers)
    assert bool(writes.started) == (workers > 1)


def test_a_custom_sigint_handler_returning_after_the_reader_finished_lets_every_parked_member_be_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writes: Writes
) -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    calls: list[int] = []
    release = threading.Event()
    gated_writes(monkeypatch, {b"park": lambda: release.wait(PATIENCE)})
    main = threading.main_thread().ident
    assert main is not None

    def interrupt() -> None:
        deadline = time.monotonic() + PATIENCE
        while len(writes.started) < 4 and time.monotonic() < deadline:
            time.sleep(0.001)
        time.sleep(0.2)  # the reader has handed out all ten members (16 fit) and waits for them
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)
        release.set()

    previous = signal.signal(signal.SIGINT, lambda signum, _frame: calls.append(signum))
    helper = threading.Thread(target=interrupt)
    helper.start()
    try:
        members = [D("d", MTIME + 5), *(F(f"d/f{index}", b"park%d" % index) for index in range(10))]
        pooled(members, tmp_path)
    finally:
        signal.signal(signal.SIGINT, previous)
        release.set()
        helper.join()
    assert calls == [signal.SIGINT]
    # Four ran in workers before the interrupt; the six never taken ran here once the handler let it pass.
    assert sorted(writes.started) == sorted(f"d/f{index}" for index in range(10))
    assert all((tmp_path / "d" / f"f{index}").read_bytes() == b"park%d" % index for index in range(10))
    assert (tmp_path / "d").stat().st_mtime == MTIME + 5
