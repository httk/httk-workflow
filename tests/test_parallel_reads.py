"""Pooled reads (``io.workers``): ``job list`` pages and per-state counts read with 8 workers give what 1 gives.

The row documents are faked from each job's tag (its step, or a damaged ``state.json`` or ``job.json``, or an
unexpected error), and every fake read sleeps a little, so that with 8 workers several reads overlap.
"""

import json
import logging
import threading
import time
import types
import uuid
from collections import Counter
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _fs
from httk.workflow.introspection import JOB_STATES, _reading, list_jobs
from httk.workflow.monitor.data import WorkspaceView
from httk.workflow.workflow_cli import command

STEPS = ("relax", "scf", "dos")
OWNER = "0" * 32


class Reads:
    """The fake documents' reads: of which jobs, how many at once at most."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.keys: list[str] = []
        self.running = 0
        self.peak = 0
        self.delay = 0.002

    def enter(self, key: str) -> None:
        with self.lock:
            self.keys.append(key)
            self.running += 1
            self.peak = max(self.peak, self.running)

    def leave(self) -> None:
        with self.lock:
            self.running -= 1


def _tag_of(ref: Any) -> str:
    return str(ref.job_key).partition("--")[0]


@pytest.fixture(autouse=True)
def _no_read_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests compare the reads of repeated listings: nothing may be served from the row or location cache."""

    monkeypatch.setattr(_reading, "_remember", lambda *_args: None)


@pytest.fixture
def reads(monkeypatch: pytest.MonkeyPatch) -> Reads:
    record = Reads()

    def fake_state(ref: Any) -> tuple[Any, str | None]:
        record.enter(ref.job_key)
        try:
            time.sleep(record.delay)
            tag = _tag_of(ref)
            if "explode" in tag:
                raise RuntimeError(f"unexpected failure reading {ref.job_key}")
            if "badstate" in tag:
                return None, f"{ref.job_key}: state.json is damaged"
            return types.SimpleNamespace(phase={"kind": "idle"}, tag=tag, unlocked=None), None
        finally:
            record.leave()

    def fake_job(ref: Any) -> tuple[Any, str | None]:
        tag = _tag_of(ref) if ref.state != "owned" else "owned-relax"
        if "badjob" in tag:
            return None, f"{ref.job_key}: job.json is damaged"
        step = next((step for step in STEPS if step in tag), None)
        return types.SimpleNamespace(step=step, placement=None if ref.placement else PurePosixPath("own")), None

    monkeypatch.setattr(_reading, "read_state", fake_state)
    monkeypatch.setattr(_reading, "read_job", fake_job)
    monkeypatch.setattr(_reading, "current_step", lambda job, _doc: job.step)
    return record


def _job(workspace: Workspace, state: str, placement: str, tag: str) -> None:
    directory = workspace.jobs.joinpath(state, *placement.split("/"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{tag}--{uuid.uuid4()}~p500~{_fs.fresh_token()}").mkdir()


@pytest.fixture
def workspace(tmp_path: Path) -> Workspace:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for index in range(240):
        state = JOB_STATES[index % 4]
        tag = f"t{index:03d}-{STEPS[index % 3]}"
        if index % 17 == 5:
            tag += "-badstate"
        if index % 19 == 7:
            tag += "-badjob"
        _job(workspace, state, f"p{index % 3}/q{index % 2}", tag)
    owned = workspace.jobs / "owned" / OWNER
    owned.mkdir(parents=True)
    for _ in range(5):
        (owned / f"{uuid.uuid4()}~p500~{_fs.fresh_token()}~ready").mkdir()
    return workspace


def _with_workers(workspace: Workspace, workers: int) -> Workspace:
    workspace.configure_settings({"io.workers": workers})
    assert workspace.io_workers == workers
    return workspace


QUERIES: list[dict[str, Any]] = [
    {},
    {"limit": 1},
    {"limit": 7},
    {"limit": 50},
    {"limit": 1000},
    {"states": ("waiting", "owned"), "limit": 9},
    {"placement_prefix": "p1", "limit": 11},
    {"tag_contains": "scf", "limit": 5},
    {"tag_contains": "bad", "limit": 3},
    {"tag_contains": "scf"},
    {"steps": ("relax",), "limit": 6},
    {"steps": ("dos", "scf"), "limit": 40},
    {"steps": ("relax",)},
    {"steps": ("relax",), "tag_contains": "-relax", "limit": 4},
]


def _pages(
    workspace: Workspace, query: dict[str, Any], caplog: pytest.LogCaptureFixture, reads: Reads
) -> Iterator[tuple[Any, Counter[str]]]:
    """Every page of *query*, following the cursors, with the warnings each logged, and the jobs each read."""

    after = None
    while True:
        caplog.clear()
        reads.keys.clear()
        page = list_jobs(workspace, after=after, **query)
        yield (page.jobs, page.next_after, [record.getMessage() for record in caplog.records]), Counter(reads.keys)
        if page.next_after is None:
            return
        after = page.next_after


@pytest.mark.parametrize("query", QUERIES, ids=[json.dumps(query, sort_keys=True) for query in QUERIES])
def test_pages_with_8_workers_are_those_with_1(
    workspace: Workspace, reads: Reads, query: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.introspection._reading")
    sequential = list(_pages(_with_workers(workspace, 1), query, caplog, reads))
    assert reads.peak == 1
    pooled = list(_pages(_with_workers(workspace, 8), query, caplog, reads))
    assert [page for page, _ in pooled] == [page for page, _ in sequential]
    assert sum(len(page[0]) for page, _ in sequential) > 0
    for (_, before), (_, after) in zip(sequential, pooled, strict=True):
        if "steps" in query:
            # A step filter may read up to one chunk (64) beyond a page, never a job twice in one page.
            assert after >= before and max(after.values()) == 1 and after.total() - before.total() <= 63
        else:
            assert after == before  # exactly the rows' documents, as before
    if query.get("limit", 2) > 1:
        assert reads.peak >= 2


def test_damaged_documents_give_the_same_rows_and_warnings(
    workspace: Workspace, reads: Reads, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.introspection._reading")
    outcomes = []
    for workers in (1, 8):
        caplog.clear()
        page = list_jobs(_with_workers(workspace, workers), steps=STEPS, limit=500)
        warned = [record.getMessage() for record in caplog.records]
        outcomes.append((page.jobs, page.next_after, warned))
        assert any("state.json is damaged" in message for message in warned)
        assert any("job.json is damaged" in message for message in warned)
    assert outcomes[0] == outcomes[1]
    rows = list_jobs(_with_workers(workspace, 8)).jobs
    damaged = [row for row in rows if "-bad" in row["job_key"].partition("--")[0]]
    assert damaged and all(row["step"] is None for row in damaged)


def test_an_unexpected_read_error_surfaces_only_where_the_loop_reaches_it(workspace: Workspace, reads: Reads) -> None:
    _job(workspace, "failed", "zz", "zzz-explode")  # the last of the 61 failed jobs in listing order
    for workers in (1, 8):
        _with_workers(workspace, workers)
        # Pages that stop before it never raise, even when a chunk read it ahead.
        page = list_jobs(workspace, states=("failed",), limit=59)
        assert len(page.jobs) == 59 and page.next_after is not None
        assert list_jobs(workspace, states=("failed",), steps=("relax",), limit=1).jobs
        with pytest.raises(RuntimeError, match="unexpected failure reading zzz-explode"):
            list_jobs(workspace, states=("failed",))
    assert reads.peak >= 2


def test_per_state_counts_with_8_workers_are_those_with_1(
    workspace: Workspace, reads: Reads, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root)

    def counted(*argv: str) -> Any:
        assert command(["job", "list", "--workspace", name, "--json", "--counts", "--limit", "1", *argv], context) == 0
        return json.loads(capsys.readouterr().out)["counts"]

    found = []
    for workers in (1, 8):
        _with_workers(workspace, workers)
        found.append(
            (
                counted(),
                counted("--state", "owned", "--state", "ready", "--placement", "p1"),
                WorkspaceView(Workspace(workspace.root), refresh_interval=0).counts(),
                WorkspaceView(Workspace(workspace.root), refresh_interval=0).counts(("waiting", "owned"), "p2"),
            )
        )
    assert found[0] == found[1]
    every, selected, monitor, monitor_selected = found[0]
    expected = {state: 0 for state in JOB_STATES} | {"ready": 60, "waiting": 60, "paused": 60, "failed": 60, "owned": 5}
    assert every == monitor == expected and list(every) == list(monitor) == list(JOB_STATES)
    assert selected == {"owned": 0, "ready": 20} and list(selected) == ["owned", "ready"]
    assert monitor_selected == {"waiting": 20, "owned": 0} and list(monitor_selected) == ["waiting", "owned"]


def test_a_listing_error_surfaces_only_where_the_loop_reaches_it(
    workspace: Workspace, reads: Reads, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_stream = _reading._stream_after

    def breaking(*args: Any) -> Iterator[Any]:
        for position, ref in enumerate(real_stream(*args)):
            if position == 30:
                raise OSError("the listing broke")
            yield ref

    monkeypatch.setattr(_reading, "_stream_after", breaking)
    outcomes = []
    for workers in (1, 8):
        _with_workers(workspace, workers)
        reads.keys.clear()
        # Every 3rd ready job is at "relax": a page of 2 ends long before the 30th job, which a chunk of 64 passes.
        page = list_jobs(workspace, states=("ready",), steps=("relax",), limit=2)
        with pytest.raises(OSError, match="the listing broke"):
            list_jobs(workspace, states=("ready",), steps=("relax",))
        with pytest.raises(OSError, match="the listing broke"):
            list_jobs(workspace, states=("ready",), limit=40)
        outcomes.append((page.jobs, page.next_after, list_jobs(workspace, states=("ready",), limit=29).jobs))
    assert outcomes[0] == outcomes[1] and len(outcomes[0][0]) == 2 and len(outcomes[0][2]) == 29


def test_the_read_ahead_never_takes_a_job_past_the_scan_limit(
    workspace: Workspace, reads: Reads, monkeypatch: pytest.MonkeyPatch
) -> None:
    reads.delay = 0
    drawn = [0]

    def endless(*_args: Any) -> Iterator[Any]:
        for position in range(20_000):
            drawn[0] += 1
            yield types.SimpleNamespace(
                job_key=f"t{position:05d}-none--{uuid.UUID(int=position)}",
                job_id=str(uuid.UUID(int=position)),
                state="ready",
                cursor=f"p/t{position:05d}",
                placement=PurePosixPath("p"),
                priority=500,
                token="x",
                owner_id=None,
                path=Path(f"/nowhere/{position}"),
            )

    monkeypatch.setattr(_reading, "_stream_after", endless)
    for workers in (1, 8):
        _with_workers(workspace, workers)
        drawn[0] = 0
        page = list_jobs(workspace, steps=("relax",), limit=1)  # no job matches: the 10,000 scan limit ends it
        assert page.jobs == [] and page.next_after == "ready:p/t09999"
        assert drawn[0] == 10_000
