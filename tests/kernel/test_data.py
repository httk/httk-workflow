"""Unit tests of :mod:`httk.workflow._data` (job-data transactions, note §7.2) on a real filesystem."""

import json
import os
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import _fs
from httk.workflow._data import DataConflict, Transaction, apply_transactions, discard_uncommitted
from httk.workflow._fs import Loc
from httk.workflow._kernel import OwnedJob, claim, register_owner, submit
from httk.workflow.errors import FormatError, WorkflowError


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@pytest.fixture(autouse=True)
def _no_faults() -> Iterator[None]:
    yield
    _fs.set_fault_injector(None)


@pytest.fixture()
def job(tmp_path: Path) -> OwnedJob:
    root = tmp_path / "ws"
    ws = FakeWorkspace(root, root / ".httk-workspace", root / "jobs", False, 0.2)
    owner = register_owner(ws, kind="manager", label=None, allocation=None, advertised={})
    staging = owner.scratch("build") / "job"
    staging.mkdir(parents=True)
    job_id = str(uuid.uuid4())
    document = {"format": "httk-workflow-job", "format_version": 3, "id": job_id, "tag": "t", "placement": "p"}
    (staging / "job.json").write_text(json.dumps({**document, "priority": 500}))
    owned = claim(ws, owner, submit(ws, owner, staging))
    assert owned is not None
    return owned


ATTEMPT = "attempts/0b5c1f8e-4d2a-4c55-9a43-0d8f7e6a1b23"


def stage(job: OwnedJob, files: dict[str, str | None], *, attempt: str = ATTEMPT) -> Transaction:
    """Commit one transaction: a value is file content, ``None`` a directory."""

    transaction = Transaction(job.path / attempt, durable=False)
    for name, content in files.items():
        path = transaction.path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if content is None:
            path.mkdir()
        else:
            path.write_text(content)
    transaction.commit()
    return transaction


def staging_left(job: OwnedJob) -> list[str]:
    return sorted(p.relative_to(job.path).as_posix() for p in (job.path / "attempts").rglob("*"))


def test_file_add_replace_and_nested(job: OwnedJob) -> None:
    (job.path / "old.txt").write_text("old")
    stage(job, {"new.txt": "new", "old.txt": "replaced", "a/b/c.txt": "deep"})
    assert apply_transactions(job) == 3  # new.txt, old.txt and the whole directory a
    assert (job.path / "new.txt").read_text() == "new"
    assert (job.path / "old.txt").read_text() == "replaced"
    assert (job.path / "a/b/c.txt").read_text() == "deep"
    assert staging_left(job) == [ATTEMPT]
    assert apply_transactions(job) == 0


def test_symlink_leaf_added_and_replacing_a_file(job: OwnedJob) -> None:
    (job.path / "data").mkdir()
    (job.path / "data/x").write_text("file")
    transaction = Transaction(job.path / ATTEMPT, durable=False)
    (transaction.path / "data").mkdir()
    (transaction.path / "data/x").symlink_to("/nonexistent/target")
    (transaction.path / "link").symlink_to("../elsewhere")
    transaction.commit()
    assert apply_transactions(job) == 2
    assert os.readlink(job.path / "data/x") == "/nonexistent/target"
    assert os.readlink(job.path / "link") == "../elsewhere"


def test_directory_fast_path_and_merge(job: OwnedJob) -> None:
    (job.path / "merge").mkdir()
    (job.path / "merge/kept.txt").write_text("kept")
    stage(job, {"whole/x": "1", "whole/sub/y": "2", "empty": None, "merge/added.txt": "added", "merge/kept.txt": "new"})
    # whole and empty move as directories; merge recurses into two files.
    assert apply_transactions(job) == 4
    assert sorted(p.name for p in (job.path / "whole").iterdir()) == ["sub", "x"]
    assert (job.path / "empty").is_dir()
    assert (job.path / "merge/added.txt").read_text() == "added"
    assert (job.path / "merge/kept.txt").read_text() == "new"
    assert staging_left(job) == [ATTEMPT]


def test_seq_order(job: OwnedJob) -> None:
    first = stage(job, {"f": "one"})
    second = stage(job, {"f": "two"})
    assert (first.path.name, second.path.name) == ("000001.tmp", "000002.tmp")
    assert apply_transactions(job) == 2
    assert (job.path / "f").read_text() == "two"


def test_resume_after_crash(job: OwnedJob) -> None:
    files = {f"d/f{i}": str(i) for i in range(5)}
    (job.path / "d").mkdir()
    stage(job, {**files, "top": "t"})
    renames = 0

    def crash(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        nonlocal renames
        if op == "rename" and phase == "after":
            renames += 1
            if renames == 3:
                raise SystemExit("crash")

    _fs.set_fault_injector(crash)
    with pytest.raises(SystemExit):
        apply_transactions(job)
    _fs.set_fault_injector(None)
    assert apply_transactions(job) == 3
    assert {f"d/{p.name}": p.read_text() for p in (job.path / "d").iterdir()} == files
    assert (job.path / "top").read_text() == "t"
    assert staging_left(job) == [ATTEMPT]


@pytest.mark.parametrize(
    ("make", "files", "conflict"),
    [
        (lambda p: (p / "x").mkdir(), {"x": "file over dir"}, "x"),
        (lambda p: (p / "x").write_text("f"), {"x/y": "dir over file"}, "x"),
        (lambda p: (p / "x").write_text("f"), {"x/y/z": "parent is a file"}, "x"),
        (lambda p: (p / "x").symlink_to(p / "real"), {"x/y": "parent is a symlink"}, "x"),
    ],
)
def test_data_conflicts(
    job: OwnedJob, make: Callable[[Path], object], files: dict[str, str | None], conflict: str
) -> None:
    (job.path / "real").mkdir()
    make(job.path)
    stage(job, files)
    with pytest.raises(DataConflict) as caught:
        apply_transactions(job)
    assert caught.value.path == PurePosixPath(conflict)
    assert list((job.path / "real").iterdir()) == []
    assert any(name.endswith("/txn/000001") for name in staging_left(job))


@pytest.mark.parametrize("name", ["job.json", "logs/x", "attempts/x", "seal.json"])
def test_reserved_names_are_a_protocol_error(job: OwnedJob, name: str) -> None:
    transaction = Transaction(job.path / ATTEMPT, durable=False)
    path = transaction.path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    transaction.commit()
    with pytest.raises(FormatError, match="reserved"):
        apply_transactions(job)
    assert json.loads((job.path / "job.json").read_text())["id"] == job.job_id


def test_fifo_and_hard_link_are_a_protocol_error(job: OwnedJob) -> None:
    transaction = Transaction(job.path / ATTEMPT, durable=False)
    os.mkfifo(transaction.path / "fifo")
    transaction.commit()
    with pytest.raises(FormatError, match="special file"):
        apply_transactions(job)
    _fs.discard(_fs.loc(job.path / "attempts"), trash_dir=job.path, durable=False)
    (job.path / "outside").write_text("x")
    transaction = Transaction(job.path / ATTEMPT, durable=False)
    os.link(job.path / "outside", transaction.path / "linked")
    transaction.commit()
    with pytest.raises(FormatError, match="hard-linked"):
        apply_transactions(job)


def test_symlinked_staging_directories_are_never_entered(job: OwnedJob, tmp_path: Path) -> None:
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "000001").mkdir(parents=True)
    (elsewhere / "000001" / "stolen").write_text("x")
    (job.path / ATTEMPT).mkdir(parents=True)
    (job.path / ATTEMPT / "txn").symlink_to(elsewhere)
    assert apply_transactions(job) == 0
    assert (elsewhere / "000001" / "stolen").exists()


def test_discard_uncommitted_removes_staging_only(job: OwnedJob) -> None:
    stage(job, {"kept": "k"})
    Transaction(job.path / ATTEMPT, durable=False).put(job.path / "job.json", "dropped")
    assert discard_uncommitted(job) == 1
    assert staging_left(job) == [ATTEMPT, f"{ATTEMPT}/txn", f"{ATTEMPT}/txn/000001", f"{ATTEMPT}/txn/000001/kept"]
    assert apply_transactions(job) == 1
    assert not (job.path / "dropped").exists()


def test_quiescence_is_required(job: OwnedJob) -> None:
    stage(job, {"f": "x"})
    job.begin_attempt(ATTEMPT.split("/")[1])
    with pytest.raises(WorkflowError, match="still live"):
        apply_transactions(job)
    with pytest.raises(WorkflowError, match="still live"):
        discard_uncommitted(job)
    assert not (job.path / "f").exists()


def test_job_side_transaction(job: OwnedJob, tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "tree").mkdir(parents=True)
    (source / "tree/a").write_text("a")
    (source / "tree/link").symlink_to("a")
    (source / "file").write_text("f")
    transaction = Transaction(job.path / ATTEMPT, durable=True)
    assert transaction.path.name == "000001.tmp"
    transaction.put(source / "file", "out/file")
    transaction.put(str(source / "tree"), PurePosixPath("out/tree"))
    for bad in ["/abs", "../up", "a/../../b", "", ".", "logs/x", "job.json"]:
        with pytest.raises(ValueError):
            transaction.put(source / "file", bad)
    with pytest.raises(IsADirectoryError):
        transaction.put(source / "file", "out/tree")
    assert Transaction(job.path / ATTEMPT, durable=False).path.name == "000002.tmp"
    transaction.commit()
    assert (job.path / ATTEMPT / "txn/000001/out/file").read_text() == "f"
    with pytest.raises(WorkflowError):
        transaction.put(source / "file", "late")
    with pytest.raises(WorkflowError):
        transaction.commit()
    assert Transaction(job.path / ATTEMPT, durable=False).path.name == "000003.tmp"
    discard_uncommitted(job)
    assert apply_transactions(job) == 1
    assert (job.path / "out/tree/a").read_text() == "a"
    assert os.readlink(job.path / "out/tree/link") == "a"
    assert (job.path / "out/file").read_text() == "f"
