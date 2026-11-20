"""Job creation and ``job submit`` publish in batches, each made durable with one sync (plan
WORKFLOW_SYNC_SEGMENTS_AND_TRANSFER_SESSION §7.3).

``new_jobs(..., batch=N)`` (``job new`` and campaigns pass ``_BATCH_JOBS``; the library default 1 publishes each job
before the next item is asked for) stages up to N jobs, closes the batch between items at N jobs or
``_BATCH_SECONDS`` after its first one, at the end of the input, and around a job that moves inputs; a batch is
committed once and then published in order. Exactly the yielded jobs are submitted, and a job already named
``job/``: closing, interrupting or failing discards the other staged jobs and moves their moved inputs back. The
recorded order of commits, stagings and renames is the property under test.
"""

import errno
import json
import os
import signal
import subprocess
import sys
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

import test_input_moves
from conftest import register_ws
from httk.workflow import DuplicateTagError, Workspace, _fs, _kernel
from httk.workflow import scaffold as scaffold_module
from httk.workflow.scaffold import JobItem, new_job, new_jobs
from httk.workflow.seals import read_job_releases
from httk.workflow.workflow_cli import command
from test_input_moves import _unregister  # noqa: F401 - an autouse fixture
from test_job_creation import assert_nothing_left, install, raw_package, ready, workspace_at
from test_sync_publication import (
    Events,
    durable_moves_workspace,
    durable_workspace,
    hooked_package,
    job_rename,
    prepared_payloads,
    submit,
)
from v3_helpers import cli_owner

pytestmark = pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="names descriptors through /proc")

SOURCE = Path(__file__).resolve().parents[1] / "src"
#: A batch size no test reaches by itself.
LARGE = 256


@pytest.fixture(autouse=True)
def _fresh(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    # No time cap unless a test sets the clock: a loaded machine must not close a batch early.
    monkeypatch.setattr(scaffold_module, "_clock", lambda: 0.0)
    yield
    _fs.set_fault_injector(None)


def raw_workspace(tmp_path: Path, sync: str = "syncfs") -> Workspace:
    """A durable workspace with ``io.sync`` forced and the one-step ``tests.raw`` package installed."""

    ws = durable_workspace(tmp_path / "ws", sync)
    install(ws, raw_package(tmp_path / "package"))
    return ws


def tagged(count: int, prefix: str = "j") -> list[JobItem]:
    return [{"tag": f"{prefix}{index}"} for index in range(count)]


def tmp_entries(ws: Workspace) -> list[Path]:
    return sorted((ws.control / "tmp").iterdir())


def ready_tags(ws: Workspace) -> list[str]:
    return sorted(ref.job_key.split("--")[0] for ref in ready(ws))


class Clock:
    """A clock the test sets, replacing the batches' clock."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.now = 0.0
        monkeypatch.setattr(scaffold_module, "_clock", lambda: self.now)


class Descriptors:
    """The origin descriptors of the moved inputs that are open now (a number is not reused while open)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.open: set[int] = set()
        self.opened = 0
        real_move, real_close = scaffold_module._move_input, os.close

        def move(*arguments: Any, **options: Any) -> Any:
            result = real_move(*arguments, **options)
            self.open.add(result.directory)
            self.opened += 1
            return result

        def close(descriptor: int) -> None:
            self.open.discard(descriptor)
            real_close(descriptor)

        monkeypatch.setattr(scaffold_module, "_move_input", move)
        monkeypatch.setattr(os, "close", close)


# -- batches ------------------------------------------------------------------------------------------------------


def test_600_jobs_cost_one_syncfs_per_batch_and_each_commit_precedes_its_batch_s_renames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    # The per-job publication of each batch (the batched renames, test_batched_publish, add their own commit points).
    monkeypatch.setattr(scaffold_module, "_BATCH_MIN", LARGE + 1)
    events = Events(monkeypatch)
    jobs = list(new_jobs(ws, "tests.raw", iter(tagged(600)), batch=256))
    assert [job.tag for job in jobs] == [f"j{index}" for index in range(600)] and len(ready(ws)) == 600
    # Batches of 256, 256 and 88: the prime and three commits, all on the barrier's one descriptor.
    assert len(events.barriers) == 1 and events.details("syncfs") == events.details("enter") * 4
    prime, *commits = events.indices("syncfs")
    staged, renamed = events.indices("job.json"), events.renames(job_rename)
    assert len(staged) == len(renamed) == 600 and prime < staged[0]
    bounds = [0, 256, 512, 600]
    for batch, commit in enumerate(commits):
        first, end = bounds[batch], bounds[batch + 1]
        # Every job of the batch is staged before its commit, and none is named job/ before it.
        assert staged[end - 1] < commit < renamed[first]
        # Each batch is published, in order, before the next one is staged.
        assert renamed[first:end] == sorted(renamed[first:end])
        if end < 600:
            assert renamed[end - 1] < staged[end] < commits[batch + 1]
    assert_nothing_left(ws)


def test_the_default_publishes_each_job_before_asking_for_the_next_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    received: list[str] = []

    def feedback() -> Iterator[JobItem]:
        # Each item depends on the job before it, as a loop feeding on its own results does.
        for index in range(3):
            assert len(received) == index
            yield {"tag": f"j{index}", "parameters": {"after": received[-1] if received else None}}

    events = Events(monkeypatch)
    for job in new_jobs(ws, "tests.raw", feedback()):
        received.append(job.job_key)
    assert len(received) == 3 and len(ready(ws)) == 3
    # One barrier for the call, a commit per job: N + 1 syncfs, each commit before its own rename.
    assert len(events.barriers) == 1 and events.kinds().count("syncfs") == 4
    _prime, *commits = events.indices("syncfs")
    for commit, renamed in zip(commits, events.renames(job_rename), strict=True):
        assert commit < renamed


def test_batch_must_be_a_positive_number(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    for wrong in (0, -1, True, 1.5):
        with pytest.raises(ValueError, match="batch must be a positive number"):
            list(new_jobs(ws, "tests.raw", [], batch=wrong))  # type: ignore[arg-type]


def test_under_fsync_a_batch_commit_walks_each_of_its_jobs_before_any_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    install(ws, hooked_package(tmp_path / "package"))
    events = Events(monkeypatch)
    jobs = list(new_jobs(ws, "tests.sync", iter(tagged(3)), batch=LARGE))
    assert len(jobs) == 3 and "syncfs" not in events.kinds()
    # One walk per job, each of its own submit scratch, all after the last job's hook and before the first rename.
    walked = [Path(path) for path in events.details("walk-start")]
    assert len(walked) == 3 and len(set(walked)) == 3
    assert all(path.parent == ws.control / "tmp" and ".submit." in path.name for path in walked)
    renamed = events.renames(job_rename)
    assert events.last("hook") < events.first("walk-start") and events.last("walk") < renamed[0]
    assert [Path(str(events.events[index][1])).parent for index in renamed] == walked


def test_under_fsync_a_failed_walk_names_its_job_and_suggests_no_setting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    install(ws, hooked_package(tmp_path / "package"))
    Events(monkeypatch, fsync_failure="hook.txt")
    with pytest.raises(_fs.SyncFailed) as raised:
        list(new_jobs(ws, "tests.sync", iter(tagged(3)), batch=LARGE))
    message = str(raised.value)
    assert "the 3 jobs of this batch (j0--" in message and "(syncing the job j0--" in message
    assert "io.sync fsync" not in message and "io.sync_errors" not in message
    assert ready(ws) == []
    assert_nothing_left(ws)


def test_a_batch_closes_ten_seconds_after_its_first_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = raw_workspace(tmp_path)
    clock = Clock(monkeypatch)
    requested: list[int] = []

    def items() -> Iterator[JobItem]:
        # Staged at 0, 1, 2 and 11 seconds: the batch closes after the fourth, then 12 and 13 end the input.
        for index, at in enumerate((0.0, 1.0, 2.0, 11.0, 12.0, 13.0)):
            requested.append(index)
            clock.now = at
            yield {"tag": f"j{index}"}

    events = Events(monkeypatch)
    jobs = new_jobs(ws, "tests.raw", items(), batch=LARGE)
    assert next(jobs).tag == "j0"
    # The first batch closed after the fourth item, with fewer than its size, and is yielded whole before the fifth
    # item is asked for.
    assert requested == [0, 1, 2, 3] and len(ready(ws)) == 1
    assert [next(jobs).tag for _ in range(3)] == ["j1", "j2", "j3"]
    assert requested == [0, 1, 2, 3] and len(ready(ws)) == 4
    assert [job.tag for job in jobs] == ["j4", "j5"]
    _prime, first, second = events.indices("syncfs")
    renamed = events.renames(job_rename)
    assert first < renamed[0] and renamed[3] < second < renamed[4]


@pytest.mark.parametrize("taken", ["existing", "in-batch"])
def test_the_time_cap_holds_on_skip_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, taken: str) -> None:
    ws = raw_workspace(tmp_path)
    if taken == "existing":
        new_job(ws, "tests.raw", tag="x")
    clock = Clock(monkeypatch)
    requested: list[int] = []

    def items() -> Iterator[JobItem]:
        # j0 staged at 0; the skipped x at 11 seconds; then j2.
        for index, (tag, at) in enumerate((("j0" if taken == "existing" else "x", 0.0), ("x", 11.0), ("j2", 12.0))):
            requested.append(index)
            clock.now = at
            yield {"tag": tag}

    jobs = new_jobs(ws, "tests.raw", items(), duplicate_tag="skip", batch=LARGE)
    first = next(jobs)
    # After the skip at 11 seconds the batch closed, before the third item was asked for.
    assert requested == [0, 1] and first.tag == ("j0" if taken == "existing" else "x")
    assert [job.tag for job in jobs] == ["j2"]


def test_without_a_time_or_size_cap_the_input_s_end_closes_the_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    requested: list[int] = []

    def items() -> Iterator[JobItem]:
        for index in range(3):
            requested.append(index)
            yield {"tag": f"j{index}"}

    events = Events(monkeypatch)
    jobs = new_jobs(ws, "tests.raw", items(), batch=LARGE)
    assert next(jobs).tag == "j0" and requested == [0, 1, 2]
    assert len(list(jobs)) == 2 and events.kinds().count("syncfs") == 2


def test_a_non_durable_workspace_submits_each_job_at_once(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    install(ws, raw_package(tmp_path / "package"))
    requested: list[int] = []

    def items() -> Iterator[JobItem]:
        for index in range(3):
            requested.append(index)
            yield {"tag": f"j{index}"}

    jobs = new_jobs(ws, "tests.raw", items(), batch=LARGE)
    assert next(jobs).tag == "j0" and requested == [0] and len(ready(ws)) == 1
    assert len(list(jobs)) == 2 and len(ready(ws)) == 3


# -- the input ----------------------------------------------------------------------------------------------------


def test_a_none_item_is_an_input_error_not_the_end_of_the_input(tmp_path: Path) -> None:
    ws = raw_workspace(tmp_path)
    items: list[Any] = [{"tag": "a"}, None, {"tag": "c"}]
    jobs = new_jobs(ws, "tests.raw", iter(items), batch=LARGE)
    assert next(jobs).tag == "a"
    with pytest.raises(AttributeError):
        next(jobs)
    assert ready_tags(ws) == ["a"]
    assert_nothing_left(ws)


def test_a_duplicate_tag_error_of_the_input_is_raised_like_any_input_error(tmp_path: Path) -> None:
    ws = raw_workspace(tmp_path)
    existing = new_job(ws, "tests.raw", tag="old")

    def items() -> Iterator[JobItem]:
        yield {"tag": "a"}
        raise DuplicateTagError("old", existing.placement, existing.ref)

    jobs = new_jobs(ws, "tests.raw", items(), duplicate_tag="skip", batch=LARGE)
    assert next(jobs).tag == "a"
    with pytest.raises(DuplicateTagError):
        next(jobs)
    assert ready_tags(ws) == ["a", "old"]
    assert_nothing_left(ws)


def test_an_input_error_publishes_the_jobs_staged_before_it_and_is_raised(tmp_path: Path) -> None:
    ws = raw_workspace(tmp_path)

    def items() -> Iterator[JobItem]:
        yield {"tag": "j0"}
        raise RuntimeError("the input broke")

    jobs = new_jobs(ws, "tests.raw", items(), batch=LARGE)
    assert next(jobs).tag == "j0"
    with pytest.raises(RuntimeError, match="the input broke"):
        next(jobs)
    assert len(ready(ws)) == 1
    assert_nothing_left(ws)


# -- moved inputs -------------------------------------------------------------------------------------------------


def moves_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Workspace, Path]:
    """A durable moves workspace and its succeeded origin, which holds ``run/traj.dat`` and ``run/other.dat``."""

    ws = durable_moves_workspace(tmp_path, monkeypatch)
    return ws, test_input_moves._origin(ws, extra="other.dat")


def moving(source: Path, tag: str) -> JobItem:
    return {"tag": tag, "move_inputs": {"trajectory": source}}


def moves_jobs(ws: Workspace, items: list[JobItem], **options: Any) -> Generator[Any, None, None]:
    return new_jobs(
        ws,
        test_input_moves._WORKFLOW,
        iter(items),
        parameters={"script": {"start": "succeed"}},
        **{"batch": LARGE, **options},
    )


def assert_home(origin: Path, source: Path) -> None:
    """The moved file is back in its origin and its release record withdrawn."""

    assert source.read_bytes() == test_input_moves._FRAMES and read_job_releases(origin) == ()


def test_a_job_that_moves_inputs_has_a_batch_of_its_own(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    traj, other = origin / "run" / "traj.dat", origin / "run" / "other.dat"
    descriptors = Descriptors(monkeypatch)
    events = Events(monkeypatch)
    items: list[JobItem] = [{"tag": "a"}, {"tag": "b"}, moving(traj, "c"), {"tag": "d"}, moving(other, "e")]
    seen: list[tuple[str | None, bool, int]] = []
    for job in moves_jobs(ws, items):
        # Whether each moved file is home, and how many origin descriptors are open, as each job is yielded.
        seen.append((job.tag, traj.exists(), len(descriptors.open)))
    # No job of a batch is yielded while a moved input is out of its origin and not yet published.
    assert seen == [("a", True, 0), ("b", True, 0), ("c", False, 0), ("d", False, 0), ("e", False, 0)]
    # Batches [a, b], [c], [d], [e]: the prime and four commits.
    _prime, *commits = events.indices("syncfs")
    renamed = events.renames(job_rename)
    assert len(commits) == 4 and len(renamed) == 5
    assert commits[0] < renamed[1] < commits[1] < renamed[2] < commits[2] < renamed[3] < commits[3] < renamed[4]
    assert descriptors.opened == 2 and descriptors.open == set()
    assert sorted(release.path for release in read_job_releases(origin)) == ["run/other.dat", "run/traj.dat"]
    assert_nothing_left(ws)


def test_closing_the_iterator_mid_batch_submits_exactly_the_yielded_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    events = Events(monkeypatch)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(4)), batch=LARGE)
    yielded = [next(jobs), next(jobs)]
    # All four are committed, two published.
    assert len(events.renames(job_rename)) == 2
    jobs.close()
    assert sorted(ref.job_key for ref in ready(ws)) == sorted(job.job_key for job in yielded)
    assert_nothing_left(ws)


def test_an_interrupt_while_staging_a_moving_job_moves_its_inputs_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    descriptors = Descriptors(monkeypatch)
    events = Events(monkeypatch)
    real_prepare = scaffold_module.prepare_job_payload
    calls: list[Path] = []

    def prepare(destination: Path, *arguments: Any, **options: Any) -> Any:
        calls.append(destination)
        if len(calls) == 3:
            # The third job has moved its input by now.
            assert not source.exists()
            raise KeyboardInterrupt
        return real_prepare(destination, *arguments, **options)

    monkeypatch.setattr(scaffold_module, "prepare_job_payload", prepare)
    jobs = moves_jobs(ws, [{"tag": "a"}, {"tag": "b"}, moving(source, "c")])
    with pytest.raises(KeyboardInterrupt):
        list(jobs)
    # a and b were published before the moving job was staged; c's move is undone.
    assert ready_tags(ws) == ["a", "b"] and len(events.renames(job_rename)) == 2
    assert_home(origin, source)
    assert descriptors.opened == 1 and descriptors.open == set()
    assert_nothing_left(ws)


@pytest.mark.parametrize("failure", ["error", "existing-tag"])
def test_a_job_refused_after_its_move_moves_it_back_and_closes_its_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    if failure == "existing-tag":
        new_job(ws, test_input_moves._WORKFLOW, parameters={"script": {"start": "succeed"}}, tag="x")
    else:
        failing_staging(monkeypatch, at=1)
    descriptors = Descriptors(monkeypatch)
    with pytest.raises(DuplicateTagError if failure == "existing-tag" else ValueError):
        list(moves_jobs(ws, [moving(source, "x")]))
    assert descriptors.opened == 1 and descriptors.open == set()
    assert_home(origin, source)
    assert_nothing_left(ws)


@pytest.mark.parametrize("point", ["append", "release"])
def test_an_interrupt_at_a_transition_leaves_no_move_unowned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    descriptors = Descriptors(monkeypatch)
    if point == "append":
        # Between the staging of the job and its entry into the batch (the batch's clock is read in between).
        def clock() -> float:
            raise KeyboardInterrupt

        monkeypatch.setattr(scaffold_module, "_clock", clock)
    else:
        # Between the job's submission and the release of its origin descriptor.
        real = scaffold_module._submit_staged

        def submitted(*arguments: Any, **options: Any) -> Any:
            real(*arguments, **options)
            raise KeyboardInterrupt

        monkeypatch.setattr(scaffold_module, "_submit_staged", submitted)
    with pytest.raises(KeyboardInterrupt):
        list(moves_jobs(ws, [moving(source, "x")]))
    assert descriptors.opened == 1 and descriptors.open == set()
    if point == "append":
        assert ready(ws) == []
        assert_home(origin, source)
    else:
        # Submitted: the moved file stays moved, in the published job, and its release stands.
        (ref,) = ready(ws)
        assert (ref.path / "files" / "trajectory.dat").read_bytes() == test_input_moves._FRAMES
        assert not source.exists() and [release.path for release in read_job_releases(origin)] == ["run/traj.dat"]
    assert_nothing_left(ws)


def test_an_interrupt_during_a_moving_job_s_commit_moves_its_inputs_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    descriptors = Descriptors(monkeypatch)
    real = _fs._syncfs
    calls: list[int] = []

    def syncfs(descriptor: int) -> None:
        calls.append(descriptor)
        if len(calls) == 2:  # the commit after the prime
            raise KeyboardInterrupt
        real(descriptor)

    monkeypatch.setattr(_fs, "_syncfs", syncfs)
    with pytest.raises(KeyboardInterrupt):
        list(moves_jobs(ws, [moving(source, "x")]))
    assert ready(ws) == [] and descriptors.open == set()
    assert_home(origin, source)
    assert_nothing_left(ws)


@pytest.mark.parametrize("where", ["publication", "yield"])
def test_an_interrupt_during_a_publication_or_at_a_yield(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, where: str
) -> None:
    ws = raw_workspace(tmp_path)
    if where == "publication":
        real_submit = _kernel.submit
        calls: list[object] = []

        def submit_job(*arguments: Any, **options: Any) -> Any:
            calls.append(arguments)
            if len(calls) == 2:
                raise KeyboardInterrupt
            return real_submit(*arguments, **options)

        monkeypatch.setattr(_kernel, "submit", submit_job)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(3)), batch=LARGE)
    first = next(jobs)
    with pytest.raises(KeyboardInterrupt):
        if where == "publication":
            next(jobs)
        else:
            jobs.throw(KeyboardInterrupt)
    assert [ref.job_key for ref in ready(ws)] == [first.job_key]
    assert_nothing_left(ws)


# -- duplicate tags inside a batch -------------------------------------------------------------------------------


def test_an_in_batch_duplicate_is_refused_after_the_jobs_before_it_are_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    events = Events(monkeypatch)
    items: list[JobItem] = [{"tag": "a"}, {"tag": "b"}, {"tag": "a"}, {"tag": "c"}]
    jobs = new_jobs(ws, "tests.raw", iter(items), batch=LARGE)
    first, second = next(jobs), next(jobs)
    with pytest.raises(DuplicateTagError) as refused:
        next(jobs)
    # The error names the published job by its real reference.
    existing = refused.value.existing
    assert existing.job_key == first.job_key and existing.state == "ready" and existing.path == first.payload
    assert existing.path.is_dir() and refused.value.tag == "a"
    assert sorted(ref.job_key for ref in ready(ws)) == sorted([first.job_key, second.job_key])
    assert events.kinds().count("syncfs") == 2 and events.indices("job.json")[2] < events.last("syncfs")
    assert_nothing_left(ws)


def test_an_in_batch_refusal_whose_prefix_commit_fails_notes_the_refused_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    items: list[JobItem] = [{"tag": "a"}, {"tag": "b"}, {"tag": "a"}]
    with pytest.raises(_fs.SyncFailed) as raised:
        list(new_jobs(ws, "tests.raw", iter(items), batch=LARGE))
    assert "the 2 jobs of this batch (a--" in str(raised.value)
    (note,) = raised.value.__notes__
    assert note.startswith("the job after them was refused: its tag 'a' is taken by a--")
    assert ready(ws) == []
    assert_nothing_left(ws)


def test_an_in_batch_duplicate_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("WARNING", logger="httk.workflow.scaffold")
    ws = raw_workspace(tmp_path)
    events = Events(monkeypatch)
    items: list[JobItem] = [{"tag": "x"}, {"tag": "y"}, {"tag": "x"}, {"tag": "x"}]
    jobs = list(new_jobs(ws, "tests.raw", iter(items), duplicate_tag="skip", batch=LARGE))
    assert [job.tag for job in jobs] == ["x", "y"] and len(ready(ws)) == 2
    # One batch; both later x jobs were skipped against the staged first one, whose reservation stayed.
    assert events.kinds().count("syncfs") == 2
    assert caplog.text.count("skipped tag x: already x--") == 2 and caplog.text.count("(being submitted)") == 2
    assert_nothing_left(ws)


def test_a_skipped_moving_job_moves_its_inputs_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    descriptors = Descriptors(monkeypatch)
    jobs = list(moves_jobs(ws, [{"tag": "x"}, moving(source, "x"), {"tag": "y"}], duplicate_tag="skip"))
    assert [job.tag for job in jobs] == ["x", "y"]
    assert descriptors.opened == 1 and descriptors.open == set()
    assert_home(origin, source)
    assert_nothing_left(ws)


def test_in_batch_duplicates_are_allowed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = raw_workspace(tmp_path)
    events = Events(monkeypatch)
    items: list[JobItem] = [{"tag": "a"}, {"tag": "a"}]
    jobs = list(new_jobs(ws, "tests.raw", iter(items), duplicate_tag="allow", batch=LARGE))
    assert [job.tag for job in jobs] == ["a", "a"] and len(ready(ws)) == 2
    assert events.kinds().count("syncfs") == 2


# -- failures -----------------------------------------------------------------------------------------------------


def failing_staging(monkeypatch: pytest.MonkeyPatch, *, at: int) -> None:
    """Make the staging of job number *at* (counted from 1) fail with ``ValueError``, after its moves."""

    real_prepare = scaffold_module.prepare_job_payload
    calls: list[Path] = []

    def prepare(destination: Path, *arguments: Any, **options: Any) -> Any:
        calls.append(destination)
        if len(calls) == at:
            raise ValueError("a bad item")
        return real_prepare(destination, *arguments, **options)

    monkeypatch.setattr(scaffold_module, "prepare_job_payload", prepare)


def test_a_staging_error_publishes_the_jobs_staged_before_it_and_is_raised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    events = Events(monkeypatch)
    failing_staging(monkeypatch, at=3)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(4)), batch=LARGE)
    assert [next(jobs).tag, next(jobs).tag] == ["j0", "j1"]
    with pytest.raises(ValueError, match="a bad item"):
        next(jobs)
    assert len(ready(ws)) == 2 and events.kinds().count("syncfs") == 2 and len(events.renames(job_rename)) == 2
    assert_nothing_left(ws)


@pytest.fixture
def not_zfs(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ZFS host stands for another filesystem: on ZFS a ``syncfs`` barrier always fails on its own commit error,
    ignoring ``io.sync_errors``, and job creation does not suggest that setting."""

    real = _fs._filesystem_type

    def not_zfs_type(path: Path, device: int) -> str | None:
        kind = real(path, device)
        return "ext4" if kind == "zfs" else kind

    monkeypatch.setattr(_fs, "_filesystem_type", not_zfs_type)


@pytest.mark.usefixtures("not_zfs")
@pytest.mark.parametrize("earlier", [0, 2])
def test_a_staging_error_whose_prefix_commit_fails_raises_sync_failed_chained_to_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, earlier: int
) -> None:
    ws = raw_workspace(tmp_path)
    # The prime, a successful commit per earlier batch, then the failing commit of the batch before the bad job.
    events = Events(monkeypatch, syncfs_failures=[None] * (1 + earlier // 2) + [errno.EIO])
    failing_staging(monkeypatch, at=earlier + 2)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(earlier + 3)), batch=2)
    published = [next(jobs) for _ in range(earlier)]
    with pytest.raises(_fs.SyncFailed) as raised:
        next(jobs)
    message = str(raised.value)
    assert isinstance(raised.value.__cause__, ValueError) and "a bad item" in str(raised.value.__cause__)
    assert raised.value.__notes__ == ["the batch was closed early by: ValueError: a bad item"]
    assert f"the job j{earlier}--" in message and "was not submitted, nothing was published" in message
    if earlier:
        assert "the 2 job(s) before it were submitted: create the rest with --duplicate-tag skip" in message
    else:
        assert "no job before it was submitted" in message
    assert "io.sync fsync" in message and "io.sync_errors ignore" in message
    assert sorted(ref.job_key for ref in ready(ws)) == sorted(job.job_key for job in published)
    assert len(events.renames(job_rename)) == earlier
    assert_nothing_left(ws)


@pytest.mark.usefixtures("not_zfs")
def test_a_failed_batch_commit_publishes_none_of_the_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = raw_workspace(tmp_path)
    events = Events(monkeypatch, syncfs_failures=[None, None, errno.EIO])
    jobs = new_jobs(ws, "tests.raw", iter(tagged(7)), batch=3)
    published = [next(jobs) for _ in range(3)]
    with pytest.raises(_fs.SyncFailed) as raised:
        next(jobs)
    message = str(raised.value)
    assert "the 3 jobs of this batch (j3--" in message and " to j5--" in message
    assert "were not submitted, nothing was published" in message
    assert "the 3 job(s) before them were submitted: create the rest with --duplicate-tag skip" in message
    assert "(a tagless job is created again)" in message and "io.sync fsync" in message
    assert len(ready(ws)) == 3 and [job.tag for job in published] == ["j0", "j1", "j2"]
    assert len(events.renames(job_rename)) == 3 and events.kinds().count("job.json") == 6
    assert_nothing_left(ws)


def failing_submission(monkeypatch: pytest.MonkeyPatch, *, at: int, after: bool = False) -> None:
    """Make the kernel submission of job number *at* fail with ``RuntimeError`` (once done, with *after*)."""

    real_submit = _kernel.submit
    calls: list[object] = []

    def submit_job(*arguments: Any, **options: Any) -> Any:
        calls.append(arguments)
        if len(calls) == at:
            if after:
                real_submit(*arguments, **options)
            raise RuntimeError("refused")
        return real_submit(*arguments, **options)

    monkeypatch.setattr(_kernel, "submit", submit_job)


def test_a_publication_error_discards_the_jobs_after_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = raw_workspace(tmp_path)
    failing_submission(monkeypatch, at=2)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(4)), batch=LARGE)
    first = next(jobs)
    with pytest.raises(RuntimeError, match="refused") as raised:
        next(jobs)
    (note,) = raised.value.__notes__
    assert note.startswith("1 job(s) of this command were reported as submitted; j1--")
    assert note.endswith(" was not submitted, and the 2 job(s) staged after it were not submitted")
    assert [ref.job_key for ref in ready(ws)] == [first.job_key]
    assert_nothing_left(ws)


def test_a_publication_error_of_an_early_batch_notes_the_staging_error_that_closed_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    failing_staging(monkeypatch, at=3)
    failing_submission(monkeypatch, at=2)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(3)), batch=LARGE)
    assert next(jobs).tag == "j0"
    with pytest.raises(RuntimeError, match="refused") as raised:
        next(jobs)
    assert raised.value.__notes__[-1] == "the batch was published early because a later job failed: a bad item"
    assert ready_tags(ws) == ["j0"]
    assert_nothing_left(ws)


@pytest.mark.parametrize("point", ["rename", "submit", "parent-fsync"])
def test_a_failure_after_a_job_is_named_job_counts_it_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    ws = raw_workspace(tmp_path)
    if point == "rename":
        # The rename to job/ happened and then failed (as when it reports an error it did not have).
        renames: list[Path] = []

        def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
            if op == "rename" and phase == "after" and dst is not None and dst.at is None and dst.path.name == "job":
                renames.append(dst.path)
                if len(renames) == 2:
                    raise OSError(errno.EIO, "rename reported a failure")

        _fs.set_fault_injector(fault)
    elif point == "submit":
        failing_submission(monkeypatch, at=2, after=True)
    else:
        real_fsync = os.fsync
        failed: list[str] = []

        def fsync(descriptor: int) -> None:
            path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
            # The parent directory's fsync after the second job's build/ was renamed to job/.
            scratches = len([entry for entry in tmp_entries(ws) if ".submit." in entry.name])
            if ".submit." in path.name and (path / "job").is_dir() and not failed and scratches == 2:
                failed.append(str(path))
                raise OSError(errno.EIO, os.strerror(errno.EIO))
            real_fsync(descriptor)

        monkeypatch.setattr(os, "fsync", fsync)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(3)), batch=LARGE)
    assert next(jobs).tag == "j0"
    with pytest.raises((OSError, RuntimeError)) as raised:
        next(jobs)
    (note,) = raised.value.__notes__
    assert note.startswith("1 job(s) of this command were reported as submitted; j1--")
    assert "was submitted too, though not reported (now, or by the owner's close or recovery)" in note
    assert note.endswith("and the 1 job(s) staged after it were not submitted")
    # The owner's close published what was still job/; j2 was discarded.
    assert ready_tags(ws) == ["j0", "j1"]
    assert_nothing_left(ws)


def test_a_moving_job_named_job_before_a_failure_keeps_its_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    descriptors = Descriptors(monkeypatch)

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "after" and dst is not None and dst.at is None and dst.path.name == "job":
            raise OSError(errno.EIO, "rename reported a failure")

    _fs.set_fault_injector(fault)
    with pytest.raises(OSError, match="rename reported a failure") as raised:
        list(moves_jobs(ws, [moving(source, "x")]))
    assert "x--" in raised.value.__notes__[0] and "was submitted" in raised.value.__notes__[0]
    _fs.set_fault_injector(None)
    (ref,) = ready(ws)
    assert (ref.path / "files" / "trajectory.dat").read_bytes() == test_input_moves._FRAMES
    assert not source.exists() and [release.path for release in read_job_releases(origin)] == ["run/traj.dat"]
    assert descriptors.open == set()
    assert_nothing_left(ws)


def test_the_cli_prints_an_error_s_notes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = raw_workspace(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "notes")
    failing_submission(monkeypatch, at=1)
    assert command(["job", "new", "--workspace", name, "--workflow", "tests.raw", "--tag", "x"], context) == 2
    lines = capsys.readouterr().err.splitlines()
    assert lines[0] == "httk: refused"
    assert lines[1].startswith("httk: 0 job(s) of this command were reported as submitted; x--")
    assert lines[1].endswith(" was not submitted, and the 0 job(s) staged after it were not submitted")


# -- job submit ---------------------------------------------------------------------------------------------------


def test_job_submit_batches_its_copies_and_reports_in_argument_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    first, second = prepared_payloads(ws, tmp_path, 2)
    bad = tmp_path / "bad"
    bad.mkdir()
    events = Events(monkeypatch)
    monkeypatch.setattr(sys, "stderr", sys.stdout)  # one stream, in the order printed
    assert submit(tmp_path, ws, str(first), str(bad), str(second)) == 1
    lines = capsys.readouterr().out.splitlines()
    assert [line.split(": ")[0] for line in lines] == [str(first), str(bad), str(second)]
    # The bad source was skipped; the other two were committed together and submitted.
    assert len(ready(ws)) == 2 and events.kinds().count("syncfs") == 2
    copies, renamed = events.indices("copy-tree"), events.renames(job_rename)
    assert copies[1] < events.last("syncfs") < renamed[0]
    assert_nothing_left(ws)


def test_job_submit_closes_a_batch_ten_seconds_after_its_first_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    payloads = prepared_payloads(ws, tmp_path, 3)
    ticks = iter(float(6 * step) for step in range(100))
    # Each reading advances 6 seconds: the first payload starts the batch at 0, the third is preceded by 12.
    monkeypatch.setattr(scaffold_module, "_clock", lambda: next(ticks))
    events = Events(monkeypatch)
    assert submit(tmp_path, ws, *map(str, payloads)) == 0
    capsys.readouterr()
    _prime, first, second = events.indices("syncfs")
    copies, renamed = events.indices("copy-tree"), events.renames(job_rename)
    assert copies[1] < first < renamed[1] < copies[2] < second < renamed[2]
    assert len(ready(ws)) == 3


@pytest.mark.usefixtures("not_zfs")
def test_job_submit_stops_at_a_failed_batch_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    payloads = prepared_payloads(ws, tmp_path, 5)
    monkeypatch.setattr(scaffold_module, "_BATCH_JOBS", 2)
    events = Events(monkeypatch, syncfs_failures=[None, None, errno.EIO])
    assert submit(tmp_path, ws, *map(str, payloads)) == 1
    err = capsys.readouterr().err
    assert f"the 2 payloads of this batch ({payloads[2]} to {payloads[3]}) were not submitted" in err
    assert "the 2 payload(s) before them were submitted: submit only the payloads not reported as submitted" in err
    assert "io.sync fsync" in err
    assert f"stopped: the 1 payload(s) after {payloads[3]} were not submitted" in err
    # The first batch stays submitted; the last payload was never copied.
    assert len(ready(ws)) == 2 and len(events.details("copy-tree")) == 4
    assert_nothing_left(ws)


# -- a real process killed around a batch's commit ----------------------------------------------------------------

_KILLED = """
import json, os, signal, sys
from httk.workflow import Workspace, _fs, scaffold

root, point, workflow, size = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
items = json.loads(sys.argv[5])
calls = []
real = _fs._syncfs
last = int(sys.argv[6])  # the syncfs call that commits the batch the kill is about


def syncfs(descriptor):
    calls.append(descriptor)
    if point == "before-commit" and len(calls) == last:
        os.kill(os.getpid(), signal.SIGKILL)
    real(descriptor)
    print(f"synced {len(calls)}", flush=True)
    if point == "after-commit" and len(calls) == last:
        os.kill(os.getpid(), signal.SIGKILL)


def fault(op, phase, src, dst):
    renamed = op == "rename" and phase == "after" and dst is not None and dst.at is None and dst.path.name == "job"
    if point == "after-rename" and renamed and len(calls) == last:
        os.kill(os.getpid(), signal.SIGKILL)


_fs._syncfs = syncfs
_fs.set_fault_injector(fault)
for job in scaffold.new_jobs(Workspace(root), workflow, items, batch=size, parameters={"script": {"start": "succeed"}}):
    print(job.tag, flush=True)
"""


def killed(ws: Workspace, point: str, workflow: str, size: int, items: list[JobItem], last: int) -> list[str]:
    """Run job creation in a process killed at *point* of the commit that is syncfs call *last*: its output lines."""

    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            _KILLED,
            str(ws.root),
            point,
            workflow,
            str(size),
            json.dumps(items, default=str),
            str(last),
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert process.returncode == -signal.SIGKILL, process.stderr
    return process.stdout.split("\n")[:-1]


def recover(ws: Workspace) -> None:
    (dead,) = [item.owner_id for item in _kernel.list_owners(ws) if item.record is not None]
    _kernel.attest_dead(ws, dead, by="operator", evidence=[], reason="test kill")
    with cli_owner(ws) as owner:
        _kernel.recover(ws, owner, dead)


@pytest.mark.slow
@pytest.mark.parametrize(
    ("point", "published"),
    [("before-commit", 3), ("after-commit", 3), ("after-rename", 4)],
)
def test_recovery_after_a_kill_publishes_exactly_the_committed_job_trees(
    tmp_path: Path, point: str, published: int
) -> None:
    ws = raw_workspace(tmp_path)
    # Batches [j0, j1, j2] and [j3, j4]; syncfs calls: the prime, the first commit, the second commit (3).
    lines = killed(ws, point, "tests.raw", 3, tagged(5), 3)
    # The first batch was committed, published and yielded; the kill came before or after the second commit.
    assert lines[:5] == ["synced 1", "synced 2", "j0", "j1", "j2"]
    assert lines[5:] == ([] if point == "before-commit" else ["synced 3"])
    assert len(ready(ws)) == 3
    scratches = [path for path in tmp_entries(ws) if ".submit." in path.name]
    assert len(scratches) == 2
    named = [path for path in scratches if (path / "job").is_dir()]
    assert len(named) == (1 if point == "after-rename" else 0)
    recover(ws)
    # Only a committed tree named job/ is published: a build/, committed (after-commit) or not (before-commit),
    # is discarded.
    assert ready_tags(ws) == [f"j{index}" for index in range(published)]
    assert [path for path in tmp_entries(ws) if ".submit." in path.name] == []


@pytest.mark.slow
@pytest.mark.parametrize("renamed", [False, True])
@pytest.mark.parametrize("point", ["after-commit", "after-rename"])
def test_recovery_after_a_kill_of_a_moving_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str, renamed: bool
) -> None:
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    # Batches [a] and [x], x moving; syncfs calls: the prime, a's commit, x's commit (3).
    lines = killed(ws, point, test_input_moves._WORKFLOW, LARGE, [{"tag": "a"}, moving(source, "x")], 3)
    assert lines == ["synced 1", "synced 2", "a", "synced 3"]
    assert not source.exists()
    if renamed:
        # Recovered under another path (another mount point, a bind, a moved workspace): the markers' absolute
        # paths name the old one.
        moved = ws.root.with_name(ws.root.name + "-moved")
        ws.root.rename(moved)
        origin = moved / origin.relative_to(ws.root)
        source = origin / "run" / "traj.dat"
        ws = Workspace(moved)
    recover(ws)
    assert not source.exists() and [release.path for release in read_job_releases(origin)] == ["run/traj.dat"]
    if point == "after-rename":
        # The committed job/ is published with the moved file; its release stands.
        assert ready_tags(ws) == ["a", "x"]
        (ref,) = [ref for ref in ready(ws) if ref.job_key.startswith("x--")]
        assert (ref.path / "files" / "trajectory.dat").read_bytes() == test_input_moves._FRAMES
    else:
        # A build/ holding a moved file is never discarded: recovery keeps it for the operator, as before batching.
        assert ready_tags(ws) == ["a"]
        (kept,) = [path for path in tmp_entries(ws) if ".submit." in path.name]
        assert (kept / "build" / "files" / "trajectory.dat").read_bytes() == test_input_moves._FRAMES


def write_marker(scratch: Path, document: dict[str, str]) -> Path:
    marker = scratch / f"{scaffold_module._MOVED_PREFIX}{'0' * 64}.json"
    marker.write_text(json.dumps(document), encoding="utf-8")
    return marker


def test_a_marker_whose_paths_cannot_be_placed_keeps_its_scratch(tmp_path: Path) -> None:
    ws = raw_workspace(tmp_path)
    home = tmp_path / "home.dat"
    with cli_owner(ws) as owner:
        scratch = owner.scratch("submit")
        (scratch / "build").mkdir()
        # Relative or unreadable paths are an unreadable marker (they never reach the filesystem layer).
        for document in ({"from": "relative/x", "to": "relative/y"}, {"from": str(home)}):
            assert scaffold_module._moved(owner, write_marker(scratch, document)) is True
        # A target with no scratch-named component: kept unless the source is home again.
        elsewhere = {"from": str(home), "to": str(tmp_path / "somewhere" / "build" / "x")}
        assert scaffold_module._moved(owner, write_marker(scratch, elsewhere)) is True
        home.write_text("home\n", encoding="utf-8")
        assert scaffold_module._moved(owner, write_marker(scratch, elsewhere)) is False
        # The reconciler keeps the build of a marker it cannot place, and the owner closes without an error.
        write_marker(scratch, {"from": "relative/x", "to": "relative/y"})
        assert _kernel.reconcile_scratch(owner, scratch) is False and (scratch / "build").is_dir()
        (scratch / f"{scaffold_module._MOVED_PREFIX}{'0' * 64}.json").unlink()


# -- owner lost, CLI batches and counts, campaigns, job submit notes -------------------------------------------------


def test_a_job_whose_scratch_a_recoverer_took_is_not_counted_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = raw_workspace(tmp_path)
    taken = tmp_path / "taken"
    real_submit = _kernel.submit
    calls: list[Path] = []

    def submit_job(workspace: Any, owner: Any, staging: Path, **options: Any) -> Any:
        calls.append(staging)
        if len(calls) == 2:
            # A recoverer took this owner's scratch (renamed it to its own name) before the submission.
            staging.parent.rename(taken)
            raise _kernel.OwnerLost(f"{staging} is gone")
        return real_submit(workspace, owner, staging, **options)

    monkeypatch.setattr(_kernel, "submit", submit_job)
    jobs = new_jobs(ws, "tests.raw", iter(tagged(3)), batch=LARGE)
    assert next(jobs).tag == "j0"
    with pytest.raises(_kernel.OwnerLost) as raised:
        next(jobs)
    (note,) = raised.value.__notes__
    assert note.startswith("1 job(s) of this command were reported as submitted; j1--")
    assert "was left to the recovery that took over this command's owner" in note and "submitted too" not in note
    assert ready_tags(ws) == ["j0"] and (taken / "job" / "job.json").is_file()


def cli_structures(tmp_path: Path, ws: Workspace, count: int) -> tuple[CLIContext, list[str]]:
    """``job new --input-from structure`` over *count* structures in the durable *ws*: the context and argv."""

    pytest.importorskip("httk.atomistic")
    from test_workflow_cli_packages import _POSCAR, _cli_package

    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "batches")
    structures = tmp_path / "structures"
    structures.mkdir()
    for index in range(count):
        (structures / f"s{index}.vasp").write_text(_POSCAR, encoding="utf-8")
    package = _cli_package(tmp_path / "cli-package")
    install(ws, package)
    argv = ["job", "new", "--workspace", name, "--workflow", "tests.cli.package"]
    return context, [*argv, "--input-from", "structure", str(structures)]


def test_the_cli_job_new_batches_its_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    context, argv = cli_structures(tmp_path, ws, 3)
    events = Events(monkeypatch)
    assert command(argv, context) == 0
    assert "submitted 3 jobs" in capsys.readouterr().err
    # One batch: the prime and one commit, not N + 1.
    assert len(ready(ws)) == 3 and events.kinds().count("syncfs") == 2


def test_the_cli_count_agrees_with_the_note_on_a_job_named_but_not_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    context, argv = cli_structures(tmp_path, ws, 3)
    failing_submission(monkeypatch, at=2, after=True)
    assert command(argv, context) == 2
    err = capsys.readouterr().err
    assert "submitted and reported 1 of 3 jobs before failing" in err
    assert "httk: 1 job(s) of this command were reported as submitted; s1--" in err
    assert "was submitted too, though not reported" in err
    assert len(ready(ws)) == 2


def test_the_cli_names_the_error_that_closed_a_batch_whose_commit_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    context, argv = cli_structures(tmp_path, ws, 3)
    failing_staging(monkeypatch, at=3)
    Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    assert command(argv, context) == 2
    err = capsys.readouterr().err
    assert "the 2 jobs of this batch (s0--" in err and "were not submitted, nothing was published" in err
    assert "httk: the batch was closed early by: ValueError: a bad item" in err
    assert ready(ws) == []


def test_campaign_submit_many_batches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.campaigns import campaign_submit_many, write_campaign
    from httk.workflow.projects import initialize_project
    from httk.workflow.registry import register_workspace

    ws = raw_workspace(tmp_path)
    root = tmp_path / "project"
    initialize_project(root, name="campaign")
    register_workspace("north", ws.root)
    write_campaign({"north": "north"}, assignment="explicit", project=root)
    events = Events(monkeypatch)
    jobs = campaign_submit_many("tests.raw", tagged(3), key="north", project=root)
    assert [job.tag for job in jobs] == ["j0", "j1", "j2"] and events.kinds().count("syncfs") == 2


def test_job_submit_reports_a_copy_submitted_all_the_same(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    first, second = prepared_payloads(ws, tmp_path, 2)
    failing_submission(monkeypatch, at=1, after=True)
    assert submit(tmp_path, ws, "--json", str(first), str(second)) == 1
    captured = capsys.readouterr()
    paths = sorted(str(ref.path) for ref in ready(ws))
    # Both are in ready/, and --json lists both; the first's error and its note are on stderr.
    assert len(paths) == 2 and sorted(json.loads(captured.out)) == paths
    assert f"{first}: refused" in captured.err
    assert f"{first}: {first} was submitted all the same (" in captured.err
    assert_nothing_left(ws)


@pytest.mark.usefixtures("not_zfs")
@pytest.mark.parametrize("strategy", ["syncfs", "fsync"])
def test_a_single_copy_s_failed_commit_suggests_the_settings_only_under_syncfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, strategy: str
) -> None:
    ws = durable_workspace(tmp_path / "ws", strategy)
    if strategy == "syncfs":
        (prepared,) = prepared_payloads(ws, tmp_path, 1)
        Events(monkeypatch, syncfs_failures=[None, errno.EIO])
        with cli_owner(ws) as owner, pytest.raises(_fs.SyncFailed) as raised:
            scaffold_module.submit_payload(ws, owner, prepared, move=False)
        assert "io.sync fsync" in str(raised.value) and "io.sync_errors ignore" in str(raised.value)
        return
    # A copy is fsynced inline under fsync, so its commit cannot fail; a walking barrier's can.
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "file").write_text("x\n", encoding="utf-8")
    Events(monkeypatch, fsync_failure="file")
    with _fs.Barrier(tmp_path, "fsync") as barrier, pytest.raises(_fs.SyncFailed) as raised:
        scaffold_module._commit_staged(barrier, "the payload P", tree=tree)
    assert str(raised.value.strerror).startswith("the payload P was not submitted, nothing was published")
    assert "io.sync" not in str(raised.value)


# -- a moved input renamed inside the build -----------------------------------------------------------------------

#: An instantiate hook that renames the moved input inside the payload, then fails the job's staging.
_RENAMING_HOOK = "#!/bin/sh\ncat >/dev/null\nmv files/trajectory.dat files/renamed.dat\nexit 3\n"


def test_a_moved_input_a_hook_renamed_in_a_failed_build_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from test_workflow_packages import _package
    from v3_helpers import RUNNER

    caplog.set_level("WARNING", logger="httk.workflow.scaffold")
    ws, origin = moves_setup(tmp_path, monkeypatch)
    source = origin / "run" / "traj.dat"
    manifest = test_input_moves._MANIFEST.replace(f'name = "{test_input_moves._WORKFLOW}"', 'name = "tests.renaming"')
    package = _package(tmp_path / "renaming", manifest + '\n[workflow.instantiate]\nfile = "hook"\n')
    for name, text in (("run", RUNNER), ("hook", _RENAMING_HOOK), ("collect", "#!/bin/sh\ncat >/dev/null\n")):
        (package / name).write_text(text, encoding="utf-8")
        (package / name).chmod(0o755)
    install(ws, package)
    with pytest.raises(Exception):  # noqa: B017 - whatever the failed hook raises, the staging failed
        list(new_jobs(ws, "tests.renaming", iter([moving(source, "x")]), parameters={"script": {"start": "succeed"}}))
    # The file cannot go back (it is not where it landed), so the build holding it is kept, its release standing.
    assert ready(ws) == [] and not source.exists()
    (kept,) = [path for path in tmp_entries(ws) if ".submit." in path.name]
    assert (kept / "build" / "files" / "renamed.dat").read_bytes() == test_input_moves._FRAMES
    assert "kept the unfinished payload" in caplog.text
    assert [release.path for release in read_job_releases(origin)] == ["run/traj.dat"]


def scratch_name(fill: str) -> str:
    return f"{fill * 32}.submit.{'a' * 16}"


def test_a_marker_is_placed_below_every_scratch_shaped_component(tmp_path: Path) -> None:
    ws = raw_workspace(tmp_path)
    # The recorded target has an earlier tmp/<scratch-shaped>/build/ in its root path, then the real one.
    outer = Path("/elsewhere") / "tmp" / scratch_name("0") / "build" / "ws" / ".httk-workspace" / "tmp"
    recorded = outer / scratch_name("1") / "build" / "files" / "x.dat"
    home = tmp_path / "home.dat"
    home.write_text("home\n", encoding="utf-8")  # home too: only finding the landed file keeps the scratch
    with cli_owner(ws) as owner:
        scratch = owner.scratch("submit")
        (scratch / "build" / "files").mkdir(parents=True)
        (scratch / "build" / "files" / "x.dat").write_text("landed\n", encoding="utf-8")
        assert scaffold_module._moved(owner, write_marker(scratch, {"from": str(home), "to": str(recorded)})) is True
        (scratch / f"{scaffold_module._MOVED_PREFIX}{'0' * 64}.json").unlink()


def test_a_placed_marker_whose_file_is_neither_landed_nor_home_keeps_its_scratch(tmp_path: Path) -> None:
    ws = raw_workspace(tmp_path)
    recorded = Path("/old") / ".httk-workspace" / "tmp" / scratch_name("2") / "build" / "files" / "x.dat"
    with cli_owner(ws) as owner:
        scratch = owner.scratch("submit")
        (scratch / "build").mkdir()
        # The file may be in the build under another name: kept for the operator.
        document = {"from": str(tmp_path / "gone.dat"), "to": str(recorded)}
        assert scaffold_module._moved(owner, write_marker(scratch, document)) is True
        assert _kernel.reconcile_scratch(owner, scratch) is False and (scratch / "build").is_dir()
        (scratch / f"{scaffold_module._MOVED_PREFIX}{'0' * 64}.json").unlink()
