"""The kernel and ``_fs`` surface added for moving: ``submit(priority=)``, ``Owner.discard_scratch``, ``copy_tree``."""

import json
import os
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
