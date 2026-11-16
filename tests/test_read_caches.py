"""A long-lived reader (the far-side ``workflow session``) does not re-read unchanged jobs or re-walk to find them.

Each test drives one :func:`~httk.workflow.workflow_cli._session.serve` call; a callable between two requests
changes the workspace after the earlier response was written, as another process would.
"""

import io
import json
import logging
import types
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any, TextIO, cast

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _fs, _kernel
from httk.workflow._state import Release, StateDoc
from httk.workflow.introspection import _reading
from httk.workflow.workflow_cli._session import serve
from test_moving import WORKFLOW
from v3_helpers import cli_owner, job_mapping, submit_mapping

DOCUMENTS = frozenset({"job.json", "state.json"})


class Opens:
    """The job documents read through ``_fs.read_bounded``, by job directory name."""

    def __init__(self) -> None:
        self.names: list[str] = []

    def take(self) -> list[str]:
        names, self.names = self.names, []
        return names


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop the row cache's clock, so no row expires on a slow worker (the expiry test moves its own clock)."""

    monkeypatch.setattr(_reading, "time", types.SimpleNamespace(monotonic=lambda: 1000.0))


@pytest.fixture
def opens(monkeypatch: pytest.MonkeyPatch) -> Opens:
    record = Opens()
    real = _fs.read_bounded

    def spy(src: _fs.Loc, limit: int, *, nonblock: bool = False) -> bytes | None:
        if src.path.name in DOCUMENTS:
            record.names.append(src.path.parent.name)
        return real(src, limit, nonblock=nonblock)

    monkeypatch.setattr(_fs, "read_bounded", spy)
    return record


@pytest.fixture(params=[1, 8], ids=["workers1", "workers8"])
def workspace(request: pytest.FixtureRequest, tmp_path: Path) -> Workspace:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.configure_settings({"io.workers": request.param})
    register_ws(CLIContext("httk", tmp_path), workspace.root, "station")
    return workspace


def _serve(opens: Opens, *steps: dict[str, Any] | Callable[[], object]) -> tuple[list[dict[str, Any]], list[list[str]]]:
    """Serve the request steps in one session, running each callable step between two requests.

    :return: The answers, and the job documents each request read.
    """

    reads: list[list[str]] = []

    def lines() -> Iterator[str]:
        opens.take()  # the setup's reads
        for step in steps:
            if callable(step):
                step()
                opens.take()  # the change's own reads
            else:
                yield json.dumps(step) + "\n"
                reads.append(opens.take())  # resumed only once the response was written

    responses = io.StringIO()
    assert serve(cast(TextIO, lines()), responses) == 0
    answers = [json.loads(line) for line in responses.getvalue().splitlines()[1:]]
    assert all(answer["returncode"] == 0 for answer in answers), answers
    return answers, reads


def _list(*options: str) -> dict[str, Any]:
    return {"id": "list", "argv": ["httk", "job", "list", "--json", "--workspace", "station", *options]}


def _show(job_id: str) -> dict[str, Any]:
    return {"id": "show", "argv": ["httk", "job", "show", "--json", job_id, "--no-children", "--workspace", "station"]}


def _rows(answer: dict[str, Any]) -> list[dict[str, Any]]:
    return json.loads(answer["stdout"])["jobs"]


def _submit(workspace: Workspace, tag: str) -> _kernel.JobRef:
    """A job as submitted: ``ready``, ``job.json`` only."""

    return submit_mapping(workspace, job_mapping(WORKFLOW, {"start": "succeed"}, tag=tag))


def _moved(
    workspace: Workspace, ref: _kernel.JobRef, state: str, step: str | None = "start", **updates: object
) -> _kernel.JobRef:
    """Claim *ref* and release it to *state* (a fresh name), at *step* (``None``: no activation yet)."""

    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = StateDoc.empty(owned.job_id)
        if step is not None:
            doc = doc.next_activation(step, "initial")
        return owned.release(doc.updated(**updates) if updates else doc, Release(state, 500))


def _row(rows: list[dict[str, Any]], job_id: str) -> dict[str, Any]:
    return next(row for row in rows if row["job_id"] == job_id)


def test_a_repeated_page_opens_no_job_document(workspace: Workspace, opens: Opens) -> None:
    fresh = _submit(workspace, "fresh")  # no state.json yet
    placed = [_moved(workspace, _submit(workspace, f"job{index}"), "paused", "relax") for index in range(4)]
    names = sorted(ref.path.name for ref in (fresh, *placed))

    answers, reads = _serve(
        opens,
        _list("--counts", "--limit", "50"),
        _list("--counts", "--limit", "50"),
        _list("--step", "relax"),
        _list("--step", "relax"),
    )

    first, second, filtered, again = (_rows(answer) for answer in answers)
    assert sorted(reads[0]) == sorted(names * 2)
    assert reads[1:] == [[], [], []], "neither the second page nor a --step filter re-read a cached row"
    assert first == second and len(first) == 5
    assert _row(first, fresh.job_id)["step"] == "start" and _row(first, placed[0].job_id)["step"] == "relax"
    assert sorted(row["job_key"] for row in filtered) == sorted(ref.job_key for ref in placed) and filtered == again
    assert sorted(Path(path).name for path in _reading._ROWS if Path(path).is_relative_to(workspace.jobs)) == names


class Tenure:
    """Claim a job in one step of a session and release it to ``failed`` at step ``scf`` in a later one.

    :param workspace: The workspace.
    :param ref: The unowned job.
    """

    def __init__(self, workspace: Workspace, ref: _kernel.JobRef) -> None:
        self.workspace, self.ref = workspace, ref
        self.owners = ExitStack()
        self.owned: list[_kernel.OwnedJob] = []
        self.released: list[_kernel.JobRef] = []

    def claim(self) -> None:
        claimed = _kernel.claim(self.workspace, self.owners.enter_context(cli_owner(self.workspace)), self.ref)
        assert claimed is not None
        self.owned.append(claimed)

    def release(self) -> None:
        doc = StateDoc.empty(self.ref.job_id).next_activation("scf", "initial")
        self.released.append(self.owned[0].release(doc, Release("failed", 500)))
        self.owners.close()


def test_a_moved_job_is_read_again_in_its_new_state_and_owned_jobs_are_never_cached(
    workspace: Workspace, opens: Opens
) -> None:
    ref = _moved(workspace, _submit(workspace, "mover"), "paused", "relax")
    other = _moved(workspace, _submit(workspace, "still"), "paused", "relax")
    tenure = Tenure(workspace, ref)

    answers, reads = _serve(opens, _list(), tenure.claim, _list(), _list(), tenure.release, _list())

    before, claimed_once, claimed_twice, after = (_row(_rows(answer), ref.job_id) for answer in answers)
    assert (before["state"], before["step"]) == ("paused", "relax")
    assert claimed_once["state"] == claimed_twice["state"] == "owned" and claimed_twice["step"] == "relax"
    assert (after["state"], after["step"]) == ("failed", "scf")
    # The owned job is read on each of its pages (job.json twice: its placement too); the unchanged job never again.
    owned_name = tenure.owned[0].path.name
    assert sorted(reads[0]) == sorted([ref.path.name, other.path.name] * 2)
    assert reads[1] == reads[2] and set(reads[1]) == {owned_name} and len(reads[1]) >= 2
    assert reads[3] == [tenure.released[0].path.name] * 2
    assert not any(Path(path).is_relative_to(workspace.jobs / "owned") for path in _reading._ROWS)


def test_an_unlocked_job_is_read_every_time_and_shows_an_edited_initial_step(
    workspace: Workspace, opens: Opens
) -> None:
    ref = _moved(workspace, _submit(workspace, "debug"), "paused", None, unlocked={"at": "2026-11-16T00:00:00+00:00"})
    job_file = ref.path / "job.json"

    def edit() -> None:
        mapping = json.loads(job_file.read_bytes())
        mapping["initial_step"] = "edited"
        job_file.write_text(json.dumps(mapping), encoding="utf-8")

    answers, reads = _serve(opens, _list(), _list(), edit, _list())

    assert reads == [[ref.path.name] * 2] * 3
    assert [_row(_rows(answer), ref.job_id)["step"] for answer in answers] == ["start", "start", "edited"]
    assert str(ref.path) not in _reading._ROWS


@pytest.mark.parametrize("document", ["state.json", "job.json"])
def test_a_damaged_document_is_not_cached(
    workspace: Workspace, opens: Opens, document: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.introspection._reading")
    ref = _moved(workspace, _submit(workspace, "fragile"), "paused", "relax")
    path = ref.path / document
    intact = path.read_bytes()
    path.write_bytes(b"{not json")

    answers, reads = _serve(
        opens, _list(), _list("--step", "relax"), _list("--step", "relax"), lambda: path.write_bytes(intact), _list()
    )

    damaged, warned, warned_again, repaired = answers
    assert _row(_rows(damaged), ref.job_id)["step"] is None
    assert _rows(warned) == _rows(warned_again) == []
    warnings = [record.getMessage() for record in caplog.records]
    assert len(warnings) == 2 and all(f"{ref.job_key}: " in message for message in warnings), warnings
    assert all("left out of the --step listing" in message for message in warnings)
    assert _row(_rows(repaired), ref.job_id)["step"] == "relax"
    assert reads == [[ref.path.name] * 2] * 4


class Walks:
    """The exhaustive walks of :func:`httk.workflow._kernel.locate` and the whole-workspace listings of a resolution,
    and the job directories whose ``lstat`` fails."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.job_ids: list[str] = []
        self.failing: set[Path] = set()
        self.listings = 0
        locate, lstat, iter_jobs = _kernel.locate, _fs.lstat, _reading.iter_jobs

        def walking(*args: Any, **options: Any) -> _kernel.JobRef | None:
            if options.get("exhaustive"):
                self.job_ids.append(args[1])
            return locate(*args, **options)

        def failing(target: _fs.Loc) -> Any:
            if target.path in self.failing:
                raise PermissionError(13, "Permission denied", str(target.path))
            return lstat(target)

        def listing(*args: Any, **options: Any) -> Iterator[_kernel.JobRef]:
            self.listings += 1
            return iter_jobs(*args, **options)

        monkeypatch.setattr(_kernel, "locate", walking)
        monkeypatch.setattr(_reading, "iter_jobs", listing)
        monkeypatch.setattr(_fs, "lstat", failing)

    def expect(self, *job_ids: str) -> Callable[[], None]:
        def check() -> None:
            assert self.job_ids == list(job_ids)

        return check


def test_job_show_resolves_a_remembered_location_without_the_walk(
    workspace: Workspace, opens: Opens, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _moved(workspace, _submit(workspace, "shown"), "paused", "relax")
    walks = Walks(monkeypatch)
    moved: list[_kernel.JobRef] = []
    job_id = ref.job_id

    answers, _reads = _serve(
        opens,
        _show(job_id),
        walks.expect(job_id),
        _show(ref.job_key),
        walks.expect(job_id),  # resolved from the remembered path
        lambda: moved.append(_moved(workspace, ref, "failed", "scf")),
        _show(job_id),
        walks.expect(job_id, job_id),  # the remembered path was gone: walked, and found the new one
        lambda: walks.failing.add(moved[0].path),
        _show(job_id),
        walks.expect(job_id, job_id, job_id),  # observing the remembered path failed: walked
        walks.failing.clear,
        _show(job_id),
    )

    assert walks.job_ids == [job_id] * 3 and walks.listings == 0  # a found job id never lists the workspace
    documents = [json.loads(answer["stdout"])[0] for answer in answers]
    assert [document["state"] for document in documents] == ["paused", "paused", "failed", "failed", "failed"]
    assert {document["job_id"] for document in documents} == {job_id}


def test_a_listed_job_is_shown_without_the_walk(
    workspace: Workspace, opens: Opens, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _moved(workspace, _submit(workspace, "listed"), "paused", "relax")
    walks = Walks(monkeypatch)
    (_listed, shown), _reads = _serve(opens, _list(), _show(ref.job_id))
    assert walks.job_ids == [] and json.loads(shown["stdout"])[0]["job_id"] == ref.job_id


def test_the_caches_are_cleared_whole_past_their_bound(
    workspace: Workspace, opens: Opens, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_reading, "_CACHE_LIMIT", 2)
    refs = [_moved(workspace, _submit(workspace, f"job{index}"), "paused", "relax") for index in range(5)]

    answers, reads = _serve(opens, _list(), _list(), lambda: _moved(workspace, refs[0], "failed", "scf"), _list())

    first, second, third = (_rows(answer) for answer in answers)
    assert len(_reading._ROWS) <= 2 and len(_reading._LOCATIONS) <= 2
    assert first == second and len(first) == 5
    # Cleared rows are read again (how many stay depends on how far the pooled read drew ahead of the clearing).
    assert len(reads[0]) == 10 and reads[1]
    assert (_row(third, refs[0].job_id)["state"], _row(third, refs[0].job_id)["step"]) == ("failed", "scf")


def _edit(path: Path, change: Callable[[dict[str, Any]], object]) -> None:
    """Edit a JSON document in place, as a person would."""

    document = json.loads(path.read_bytes())
    change(document)
    path.write_text(json.dumps(document), encoding="utf-8")


def test_rows_expire_so_hand_edits_show_within_a_minute(
    workspace: Workspace, opens: Opens, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr(_reading, "time", types.SimpleNamespace(monotonic=lambda: now[0]))
    fresh = _submit(workspace, "fresh")  # never activated: its step is job.json's initial step, not yet pinned
    placed = _moved(workspace, _submit(workspace, "placed"), "paused", "relax")

    def edit() -> None:
        _edit(fresh.path / "job.json", lambda job: job.update(initial_step="edited"))
        _edit(placed.path / "state.json", lambda state: state["activation"].update(step="scf"))
        now[0] += _reading._ROW_TTL  # the accepted window: still the cached rows

    def expire() -> None:
        now[0] += 0.5

    answers, reads = _serve(opens, _list(), edit, _list(), expire, _list())

    steps = [(_row(rows, fresh.job_id)["step"], _row(rows, placed.job_id)["step"]) for rows in map(_rows, answers)]
    assert steps == [("start", "relax"), ("start", "relax"), ("edited", "scf")]
    assert reads[1] == [] and sorted(reads[2]) == sorted([fresh.path.name, placed.path.name] * 2)


def test_a_filtered_page_over_cached_and_uncached_rows_is_the_one_without_caches(
    tmp_path: Path, opens: Opens, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.configure_settings({"io.workers": 8})
    for index in range(40):
        ref = _submit(workspace, f"{'ab'[index % 2]}{index:02d}")
        if index % 3:
            _moved(workspace, ref, "paused", "relax" if index % 4 < 2 else "scf")
    queries: list[dict[str, Any]] = [
        {"tag_contains": "a", "steps": ("relax",), "limit": 3},
        {"tag_contains": "b", "steps": ("start", "scf"), "limit": 4},
        {"steps": ("relax",), "limit": 5},
        {"tag_contains": "a", "limit": 4},
    ]

    def pages(query: dict[str, Any]) -> list[tuple[list[dict[str, Any]], str | None]]:
        found, after = [], None
        while True:
            page = _reading.list_jobs(workspace, after=after, **query)
            found.append((page.jobs, page.next_after))
            if (after := page.next_after) is None:
                return found

    with monkeypatch.context() as uncached:
        uncached.setattr(_reading, "_remember", lambda *_args: None)
        expected = [pages(query) for query in queries]
    uncached_reads = len(opens.take())
    _reading.list_jobs(workspace, tag_contains="1")  # caches the rows of a01, b10, ..., a31
    cached = {Path(path).name for path in _reading._ROWS}
    assert 0 < len(cached) < 40
    opens.take()

    assert [pages(query) for query in queries] == expected
    assert all(len(found) > 1 for found in expected)
    reads = opens.take()
    assert len(reads) < uncached_reads and not cached & set(reads), "a cached row is never read"


def test_a_remembered_owned_location_falls_back_to_the_walk_after_its_release(
    workspace: Workspace, opens: Opens, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _moved(workspace, _submit(workspace, "owned"), "paused", "relax")
    tenure = Tenure(workspace, ref)
    walks = Walks(monkeypatch)

    answers, _reads = _serve(
        opens,
        tenure.claim,
        _list(),  # remembers the owned location
        _show(ref.job_id),
        walks.expect(),
        tenure.release,
        _show(ref.job_id),
    )

    assert walks.job_ids == [ref.job_id]
    shown = [json.loads(answer["stdout"])[0] for answer in answers[1:]]
    assert [document["state"] for document in shown] == ["owned", "failed"]
    assert _reading._LOCATIONS[(str(workspace.root), ref.job_id)][0] == str(tenure.released[0].path)
