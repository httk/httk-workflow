"""Unit tests of the descriptor-anchored job-directory handle."""

import fcntl
import os
import socket
import stat
import threading
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from httk.workflow.errors import FormatError

_PLACEMENT = PurePosixPath("project/group")
_KEY = "job-key"


@pytest.fixture
def layout(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return a workspace root, its job directory, and an outside directory with a sentinel."""

    root = tmp_path / "workspace"
    job = root / "project" / "group" / _KEY
    job.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel").write_text("outside\n", encoding="utf-8")
    return root, job, outside


@pytest.fixture
def job_dir(layout: tuple[Path, Path, Path]) -> Iterator[JobDirectory]:
    root, _job, _outside = layout
    with JobDirectory.open(jobs=root, placement=_PLACEMENT, job_key=_KEY) as handle:
        yield handle


def _outside_state(outside: Path) -> dict[str, bytes | None]:
    """Snapshot every entry below *outside* so a write through a link is caught."""

    state: dict[str, bytes | None] = {}
    for path in sorted(outside.rglob("*")):
        state[str(path.relative_to(outside))] = None if path.is_dir() else path.read_bytes()
    return state


def _bounded[T](call: Callable[[], T], seconds: float = 5.0) -> T | BaseException:
    """Run *call* in a thread and fail the test if it blocks; return its result or exception."""

    outcome: list[T | BaseException] = []

    def target() -> None:
        try:
            outcome.append(call())
        except BaseException as exc:
            outcome.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), "the call blocked"
    return outcome[0]


# -- opening -------------------------------------------------------------------


def test_open_pins_the_job_directory(layout: tuple[Path, Path, Path]) -> None:
    root, job, _outside = layout
    with JobDirectory.open(jobs=root, placement=_PLACEMENT, job_key=_KEY) as handle:
        assert handle.path == job
        assert os.path.samestat(os.fstat(handle.fd), os.stat(job))
    with pytest.raises(ValueError, match="closed"):
        _ = handle.fd


@pytest.mark.parametrize("component", ["project", "group"])
def test_open_follows_a_symlinked_placement_directory(layout: tuple[Path, Path, Path], component: str) -> None:
    root, job, _outside = layout
    target = {"project": root / "project", "group": root / "project" / "group"}[component]
    moved = target.with_name(target.name + ".real")
    target.rename(moved)
    target.symlink_to(moved, target_is_directory=True)
    # Placement directories are operator layout, e.g. project -> /scratch/project.
    with JobDirectory.open(jobs=root, placement=_PLACEMENT, job_key=_KEY) as handle:
        assert handle.path == job
        assert handle.stat("absent") is None


def test_open_refuses_a_symlinked_job_directory(layout: tuple[Path, Path, Path]) -> None:
    root, job, outside = layout
    moved = job.with_name(job.name + ".real")
    job.rename(moved)
    job.symlink_to(moved, target_is_directory=True)
    with pytest.raises(JobDirectoryError):
        JobDirectory.open(jobs=root, placement=_PLACEMENT, job_key=_KEY)
    assert _outside_state(outside) == {"sentinel": b"outside\n"}


def test_open_reports_a_missing_job_as_not_found(layout: tuple[Path, Path, Path]) -> None:
    root, _job, _outside = layout
    with pytest.raises(FileNotFoundError):
        JobDirectory.open(jobs=root, placement=_PLACEMENT, job_key="absent")


@pytest.mark.parametrize("key", ["..", ".", "", "a/b", "a\0b"])
def test_open_refuses_an_invalid_job_key(layout: tuple[Path, Path, Path], key: str) -> None:
    root, _job, _outside = layout
    with pytest.raises(JobDirectoryError):
        JobDirectory.open(jobs=root, placement=_PLACEMENT, job_key=key)


def test_at_trusts_its_anchor_but_nothing_below_it(layout: tuple[Path, Path, Path], tmp_path: Path) -> None:
    _root, job, outside = layout
    alias = tmp_path / "alias"
    alias.symlink_to(job, target_is_directory=True)
    (job / "link").symlink_to(outside, target_is_directory=True)
    with JobDirectory.at(alias) as handle:
        assert handle.stat("link") is not None
        with pytest.raises(JobDirectoryError):
            handle.read("link/sentinel", 100)


@pytest.mark.parametrize(
    "relative",
    ["", "/etc/passwd", "..", "a/../b", "a//b", ".", "./a", "a/", "a\0b", PurePosixPath("/abs"), PurePosixPath(".")],
)
def test_invalid_relative_paths_are_refused(job_dir: JobDirectory, relative: str | PurePosixPath) -> None:
    with pytest.raises(JobDirectoryError):
        job_dir.stat(relative)
    with pytest.raises(JobDirectoryError):
        job_dir.write_atomic(relative, b"x")


# -- symlinks on every method --------------------------------------------------

_EVERY_METHOD: dict[str, Callable[[JobDirectory, str], object]] = {
    "directory": lambda handle, path: handle.directory(path).close(),
    "directory_create": lambda handle, path: handle.directory(path + "/new", create=True).close(),
    "exists_dir": lambda handle, path: handle.exists_dir(path),
    "read": lambda handle, path: handle.read(path, 1024),
    "read_json": lambda handle, path: handle.read_json(path),
    "open_read": lambda handle, path: os.close(handle.open_read(path)),
    "open_append": lambda handle, path: os.close(handle.open_append(path)),
    "append": lambda handle, path: handle.append(path, b"appended\n"),
    "write_atomic": lambda handle, path: handle.write_atomic(path + ".new", b"written\n"),
    "create_exclusive": lambda handle, path: handle.create_exclusive(path + ".new", b"written\n"),
    "remove_tree": lambda handle, path: handle.remove_tree(path),
    "unlink": lambda handle, path: handle.unlink(path),
    "rename_out": lambda handle, path: handle.rename_out(path, handle.fd, "renamed"),
}


@pytest.mark.parametrize("method", sorted(_EVERY_METHOD))
def test_a_symlinked_intermediate_component_is_refused_by_every_method(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory, method: str
) -> None:
    _root, job, outside = layout
    (job / "logs").symlink_to(outside, target_is_directory=True)
    before = _outside_state(outside)
    with pytest.raises(JobDirectoryError):
        _EVERY_METHOD[method](job_dir, "logs/sentinel")
    assert _outside_state(outside) == before
    assert (job / "logs").is_symlink()


@pytest.mark.parametrize(
    "method", ["directory", "exists_dir", "read", "read_json", "open_read", "open_append", "append"]
)
def test_a_symlinked_final_component_is_refused(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory, method: str
) -> None:
    _root, job, outside = layout
    wants_directory = method in ("directory", "exists_dir")
    (job / "entry").symlink_to(
        outside if wants_directory else outside / "sentinel", target_is_directory=wants_directory
    )
    before = _outside_state(outside)
    with pytest.raises(JobDirectoryError):
        _EVERY_METHOD[method](job_dir, "entry")
    assert _outside_state(outside) == before


def test_create_exclusive_refuses_a_symlink_at_its_name(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, outside = layout
    (job / "prelude.sh").symlink_to(outside / "sentinel")
    with pytest.raises(JobDirectoryError):
        job_dir.create_exclusive("prelude.sh", b"echo pwned\n")
    assert _outside_state(outside) == {"sentinel": b"outside\n"}
    (job / "plain").write_bytes(b"x")
    with pytest.raises(FileExistsError):
        job_dir.create_exclusive("plain", b"y")


def test_a_directory_create_refuses_a_symlink_or_file_in_the_way(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory
) -> None:
    _root, job, outside = layout
    (job / "run").symlink_to(outside, target_is_directory=True)
    with pytest.raises(JobDirectoryError):
        job_dir.directory("run", create=True)
    (job / "file").write_text("not a directory", encoding="utf-8")
    with pytest.raises(JobDirectoryError):
        job_dir.directory("file/below", create=True)
    assert _outside_state(outside) == {"sentinel": b"outside\n"}


def test_directory_creates_parents_and_honours_exclusive(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory
) -> None:
    _root, job, _outside = layout
    with job_dir.directory("attempts/one", create=True, exclusive=True) as created:
        assert created.path == job / "attempts" / "one"
        assert (job / "attempts" / "one").is_dir()
    with pytest.raises(FileExistsError):
        job_dir.directory("attempts/one", create=True, exclusive=True)
    # The intermediate container may exist; only the last component is exclusive.
    job_dir.directory("attempts/two", create=True, exclusive=True).close()
    umask = os.umask(0)
    os.umask(umask)
    assert stat.S_IMODE((job / "attempts" / "two").stat().st_mode) == 0o777 & ~umask


def test_exists_dir_distinguishes_absence_from_tampering(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory
) -> None:
    _root, job, outside = layout
    assert not job_dir.exists_dir("outcome.ready")
    assert not job_dir.exists_dir("absent/outcome.ready")
    (job / "real").mkdir()
    assert job_dir.exists_dir("real")
    (job / "outcome.ready").symlink_to(outside, target_is_directory=True)
    with pytest.raises(JobDirectoryError):
        job_dir.exists_dir("outcome.ready")
    (job / "file").write_bytes(b"x")
    with pytest.raises(JobDirectoryError):
        job_dir.exists_dir("file")


def test_stat_reports_the_entry_itself(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, outside = layout
    (job / "link").symlink_to(outside / "sentinel")
    information = job_dir.stat("link")
    assert information is not None and stat.S_ISLNK(information.st_mode)
    assert job_dir.stat("absent") is None
    assert job_dir.stat("absent/below") is None


# -- special files and bounds --------------------------------------------------


@pytest.mark.parametrize("method", ["read", "read_json", "open_read", "open_append", "append"])
def test_a_fifo_is_refused_without_blocking(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory, method: str
) -> None:
    _root, job, _outside = layout
    os.mkfifo(job / "stdio.out")
    result = _bounded(lambda: _EVERY_METHOD[method](job_dir, "stdio.out"))
    assert isinstance(result, JobDirectoryError)


def test_a_socket_is_refused(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _root, job, _outside = layout
    monkeypatch.chdir(job)  # keep the socket path short
    server = socket.socket(socket.AF_UNIX)
    try:
        server.bind("sock")
        with pytest.raises(JobDirectoryError):
            job_dir.read("sock", 100)
        with pytest.raises(JobDirectoryError):
            job_dir.open_append("sock")
    finally:
        server.close()


def test_reads_are_bounded(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, _outside = layout
    (job / "exact").write_bytes(b"x" * 64)
    assert job_dir.read("exact", 64) == b"x" * 64
    with pytest.raises(JobDirectoryError, match="exceeds"):
        job_dir.read("exact", 63)
    with pytest.raises(JobDirectoryError, match="exceeds"):
        job_dir.open_read("exact", limit=10)


def test_an_oversized_control_document_is_refused(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, _outside = layout
    with (job / "outcome.json").open("wb") as handle:
        handle.truncate(CONTROL_DOCUMENT_LIMIT + 1)
    with pytest.raises(JobDirectoryError):
        job_dir.read_json("outcome.json")


def test_read_json_requires_an_object(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, _outside = layout
    (job / "list.json").write_text("[1]", encoding="utf-8")
    (job / "broken.json").write_text("{", encoding="utf-8")
    (job / "good.json").write_text('{"action": "succeed"}', encoding="utf-8")
    with pytest.raises(FormatError, match="expected JSON object"):
        job_dir.read_json("list.json")
    with pytest.raises(FormatError, match="cannot read JSON object"):
        job_dir.read_json("broken.json")
    with pytest.raises(FormatError, match="cannot read JSON object"):
        job_dir.read_json("absent.json")
    assert job_dir.read_json("good.json") == {"action": "succeed"}


def test_open_append_returns_a_blocking_appending_descriptor(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory
) -> None:
    _root, job, _outside = layout
    (job / "logs").mkdir()
    descriptor = job_dir.open_append("logs/stdio.out")
    try:
        assert not fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_NONBLOCK
        os.write(descriptor, b"one\n")
    finally:
        os.close(descriptor)
    job_dir.append("logs/stdio.out", b"two\n")
    assert (job / "logs" / "stdio.out").read_bytes() == b"one\ntwo\n"
    assert stat.S_IMODE((job / "logs" / "stdio.out").stat().st_mode) == 0o600


# -- replacing, removing and renaming -------------------------------------------


def test_write_atomic_replaces_and_leaves_no_temporary(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, _outside = layout
    (job / "binding.json").write_bytes(b"old")
    job_dir.write_atomic("binding.json", b"new", durable=True)
    assert (job / "binding.json").read_bytes() == b"new"
    assert sorted(entry.name for entry in job.iterdir()) == ["binding.json"]


def test_write_atomic_replaces_a_symlink_instead_of_following_it(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory
) -> None:
    _root, job, outside = layout
    (job / "commit-wedge.json").symlink_to(outside / "sentinel")
    job_dir.write_atomic("commit-wedge.json", b"{}\n")
    assert not (job / "commit-wedge.json").is_symlink()
    assert (job / "commit-wedge.json").read_bytes() == b"{}\n"
    assert _outside_state(outside) == {"sentinel": b"outside\n"}


def test_write_atomic_refuses_a_directory_at_its_name(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, _outside = layout
    (job / "binding.json").mkdir()
    with pytest.raises(JobDirectoryError):
        job_dir.write_atomic("binding.json", b"{}")
    assert [entry.name for entry in job.iterdir()] == ["binding.json"]


def test_remove_tree_never_follows_a_planted_symlink(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, outside = layout
    tree = job / "attempts" / "one"
    (tree / "outcome.ready" / "children").mkdir(parents=True)
    (tree / "outcome.ready" / "outcome.json").write_text("{}", encoding="utf-8")
    (tree / "outcome.ready" / "children" / "escape").symlink_to(outside, target_is_directory=True)
    (tree / "escape-file").symlink_to(outside / "sentinel")
    job_dir.remove_tree("attempts/one")
    assert not tree.exists() and (job / "attempts").is_dir()
    assert _outside_state(outside) == {"sentinel": b"outside\n"}
    # A symlink in place of the tree is unlinked, never followed.
    (job / "attempts" / "two").symlink_to(outside, target_is_directory=True)
    job_dir.remove_tree("attempts/two")
    assert not os.path.lexists(job / "attempts" / "two")
    assert _outside_state(outside) == {"sentinel": b"outside\n"}
    job_dir.remove_tree("attempts/absent")
    job_dir.remove_tree("absent/also")


def test_unlink_removes_a_link_not_its_target(layout: tuple[Path, Path, Path], job_dir: JobDirectory) -> None:
    _root, job, outside = layout
    (job / ".httk-job").mkdir()
    (job / ".httk-job" / "seal.json").symlink_to(outside / "sentinel")
    job_dir.unlink(".httk-job/seal.json")
    assert not os.path.lexists(job / ".httk-job" / "seal.json")
    assert _outside_state(outside) == {"sentinel": b"outside\n"}
    job_dir.unlink(".httk-job/seal.json")
    with pytest.raises(FileNotFoundError):
        job_dir.unlink(".httk-job/seal.json", missing_ok=False)
    with pytest.raises(JobDirectoryError):
        job_dir.unlink(".httk-job")


def test_rename_out_moves_only_entries_of_the_required_type(
    layout: tuple[Path, Path, Path], job_dir: JobDirectory, tmp_path: Path
) -> None:
    _root, job, outside = layout
    staging = tmp_path / "staging"
    staging.mkdir()
    (job / "children" / "child").mkdir(parents=True)
    (job / "children" / "child" / "job.json").write_text("{}", encoding="utf-8")
    (job / "children" / "file").write_text("x", encoding="utf-8")
    (job / "children" / "link").symlink_to(outside, target_is_directory=True)
    target = os.open(staging, os.O_RDONLY | os.O_DIRECTORY)
    try:
        job_dir.rename_out("children/child", target, "child.staged")
        assert (staging / "child.staged" / "job.json").is_file()
        with pytest.raises(JobDirectoryError):
            job_dir.rename_out("children/file", target, "file.staged")
        with pytest.raises(JobDirectoryError):
            job_dir.rename_out("children/link", target, "link.staged")
        job_dir.rename_out("children/file", target, "file.staged", directory=False)
        assert (staging / "file.staged").read_text(encoding="utf-8") == "x"
        with pytest.raises(JobDirectoryError):
            job_dir.rename_out("children/link", target, "../escape")
    finally:
        os.close(target)
    assert sorted(entry.name for entry in staging.iterdir()) == ["child.staged", "file.staged"]
    assert (job / "children" / "link").is_symlink()
    assert _outside_state(outside) == {"sentinel": b"outside\n"}
