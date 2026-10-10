"""Tests of :meth:`httk.workflow._kernel.OwnedJob.extract`: the move of an owned job into its owner's scratch."""

import json
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from httk.workflow import _fs, _kernel
from httk.workflow._kernel import OwnedJob, Owner, ReleasedJobError, claim, register_owner, register_reconciler, submit
from httk.workflow.errors import WorkflowError


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


@pytest.fixture(autouse=True)
def _clean_kernel(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_kernel, "_RECONCILERS", {})
    yield
    _fs.set_fault_injector(None)


def new_owner(ws: FakeWorkspace) -> Owner:
    return register_owner(ws, kind="cli", label=None, allocation=None, advertised={})


def claimed(ws: FakeWorkspace, owner: Owner, *, state: str = "paused", priority: int = 321) -> OwnedJob:
    staging = owner.scratch("build") / "job"
    staging.mkdir()
    document = {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": str(uuid.uuid4()),
        "tag": "t",
        "placement": "p/q",
        "priority": priority,
    }
    (staging / "job.json").write_text(json.dumps(document))
    (staging / "payload.txt").write_text("content")
    job = claim(ws, owner, submit(ws, owner, staging, state=state))
    assert job is not None
    return job


def test_extract_into_own_scratch(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claimed(ws, owner)
    (ref,) = owner.owned()
    source = job.path
    scratch = owner.scratch("eject")
    (scratch / "bundle" / "jobs" / "p" / "q").mkdir(parents=True)
    target = scratch / "bundle" / "jobs" / "p" / "q" / job.job_key
    job.extract(target)
    assert (target / "payload.txt").read_text() == "content"
    assert not source.exists() and owner.owned() == []
    # No state write and no run-log line: the bundle manifest is the only record.
    assert sorted(path.name for path in target.iterdir()) == ["job.json", "payload.txt"]
    assert not owner.holds(ref)
    with pytest.raises(ReleasedJobError):
        job.placement()
    with pytest.raises(ReleasedJobError):
        job.extract(scratch / "again")
    # The eject scratch has no reconciler here and may hold the only copy of a job: close keeps it, and with it
    # owners/<self>/.
    owner.close()
    assert (target / "payload.txt").is_file() and (owner.path / "owner.json").is_file()


def test_close_reconciles_the_scratch_holding_an_extracted_job(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claimed(ws, owner)
    scratch = owner.scratch("eject")
    job.extract(scratch / "job")
    seen: list[Path] = []

    def reconcile(reconciling: Owner, path: Path) -> bool:
        seen.append(path)
        assert reconciling is owner and (path / "job" / "payload.txt").is_file()
        return True

    register_reconciler("eject", reconcile)
    owner.close()
    assert seen == [scratch] and not owner.path.exists()


def test_extract_directly_below_the_scratch(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claimed(ws, owner)
    scratch = owner.scratch("adopt")
    job.extract(scratch / "job")
    assert (scratch / "job" / "job.json").is_file()
    owner._discard(scratch)
    owner.close()


def refused_targets(ws: FakeWorkspace, owner: Owner, other: Owner, tmp_path: Path) -> list[Path]:
    scratch = owner.scratch("eject")
    (scratch / "occupied").mkdir()
    foreign = other.scratch("eject")
    tmp = scratch.parent
    return [
        tmp_path / "outside",  # not below tmp/
        ws.control / "tmp-lookalike" / scratch.name / "job",  # a sibling of tmp/
        tmp / "loose",  # directly in tmp/, not inside a scratch
        scratch,  # the scratch itself
        tmp / f"{owner.owner_id}.eject" / "job",  # not a scratch name
        tmp / f"{owner.owner_id}.Eject.{'a' * 16}" / "job",  # malformed purpose
        foreign / "job",  # another owner's scratch
        scratch / ".." / foreign.name / "job",  # escapes lexically
        scratch / "occupied",  # exists
        scratch / "missing" / "job",  # no parent
        Path("relative") / "job",  # not absolute
    ]


def test_extract_refusals_leave_the_job_owned(ws: FakeWorkspace, tmp_path: Path) -> None:
    owner, other = new_owner(ws), new_owner(ws)
    job = claimed(ws, owner)
    for target in refused_targets(ws, owner, other, tmp_path):
        with pytest.raises(ValueError):
            job.extract(target)
        assert job.path.is_dir() and owner.holds(job.ref), target
    # A file at the destination's parent is not a directory.
    scratch = owner.scratch("eject")
    (scratch / "file").write_text("")
    for target in (scratch / "file" / "job", scratch / "file" / "deeper" / "job"):
        with pytest.raises(ValueError):
            job.extract(target)
    # The handle still works after every refusal.
    job.give_back()
    assert owner.owned() == []


def test_extract_refuses_while_an_attempt_runs(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claimed(ws, owner)
    attempt = str(uuid.uuid4())
    job.begin_attempt(attempt)
    scratch = owner.scratch("eject")
    with pytest.raises(WorkflowError):
        job.extract(scratch / "job")
    assert job.path.is_dir() and not (scratch / "job").exists()
    job.end_attempt(attempt)
    job.extract(scratch / "job")
    assert (scratch / "job" / "job.json").is_file()


def test_extract_refuses_a_released_handle(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claimed(ws, owner)
    returned = job.give_back()
    scratch = owner.scratch("eject")
    with pytest.raises(ReleasedJobError):
        job.extract(scratch / "job")
    assert returned.path.is_dir() and not (scratch / "job").exists()


def test_extract_of_a_vanished_job_raises_owner_lost(ws: FakeWorkspace, tmp_path: Path) -> None:
    owner = new_owner(ws)
    job = claimed(ws, owner)
    job.path.rename(tmp_path / "recovered-elsewhere")
    scratch = owner.scratch("eject")
    with pytest.raises(_kernel.OwnerLost):
        job.extract(scratch / "job")
    assert not (scratch / "job").exists()
