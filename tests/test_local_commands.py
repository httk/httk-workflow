"""Local many-job commands without O(N²) work (plan WORKFLOW_LOCAL_COMMANDS, L1).

One location index per command, one order snapshot of ``requests/`` per chunk, no second verification of the
bytes this process posted, chunked posting with the withdrawal of unserved requests, and no needless reinstall:
each with its parity to the serial code it replaces, and a pin of the unchanged fsync sequence of one job.

The pins marked "HEAD 90a13ee" were recorded by running these tests against that commit's sources with
``HTTK_TEST_DUMP_PINS`` set to a directory, which makes them write what they observe instead of comparing it.
"""

import contextlib
import dataclasses
import json
import logging
import os
import re
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path, PurePosixPath

import pytest
from httk.core.identity import ensure_identity_key, write_identity_config

import v3_helpers as v3
from httk.workflow import Workspace, _kernel, _requests, _store, removal
from httk.workflow._state import Release, StateDoc
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.introspection import _reading
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command
from test_cli_job_request import _context, _job, _new_workspace

_DUMP = os.environ.get("HTTK_TEST_DUMP_PINS")


@pytest.fixture(autouse=True)
def _identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    write_identity_config(
        {"identities": {"tester": {"name": "Test User", "email": "tester@example.test"}}, "default_identity": "tester"}
    )
    ensure_identity_key("tester")


def _pinned(name: str, observed: object, expected: object) -> None:
    """Compare with the pin, or write what was observed when recording pins."""

    if _DUMP:
        Path(_DUMP).mkdir(parents=True, exist_ok=True)
        Path(_DUMP, f"{name}.json").write_text(json.dumps(observed, indent=1), encoding="utf-8")
        return
    assert observed == expected


def _requests_left(workspace: Workspace) -> int:
    directory = workspace.control / "requests"
    return len([name for name in os.listdir(directory) if name.endswith(".json")]) if directory.is_dir() else 0


def _at(workspace: Workspace, ref: _kernel.JobRef, state: str, priority: int = 500) -> _kernel.JobRef:
    """Move an unowned job to *state* at *priority* as a CLI owner would (a new name, so a new path)."""

    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = owned.read_state() or StateDoc.empty(owned.job_id).next_activation("start", "initial")
        return owned.release(doc, Release(state, priority))


@contextlib.contextmanager
def _held(workspace: Workspace, ref: _kernel.JobRef) -> Iterator[None]:
    """Hold a job in another owner for the duration, then give it back where it was."""

    with v3.cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        try:
            yield
        finally:
            owned.give_back()


def _normalizer(labels: dict[str, str]) -> Callable[[str], list[str]]:
    """Normalize command output: each job id by its label, every other UUID as ``R``."""

    uuid = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

    def normalize(text: str) -> list[str]:
        return [uuid.sub(lambda match: labels.get(match.group(), "R"), line) for line in text.splitlines()]

    return normalize


def _states(workspace: Workspace, jobs: dict[str, _kernel.JobRef]) -> dict[str, str | None]:
    found = {
        label: _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
        for label, ref in jobs.items()
    }
    return {label: None if ref is None else f"{ref.state}/p{ref.priority}" for label, ref in found.items()}


# -- outcome parity with the serial code (HEAD 90a13ee) -------------------------------------------------------------


_CANCEL_PARITY: dict[str, object] = {
    'prefix': {
        'status': 1,
        'stdout': [
            'ready1\tR\tapplied\tcancel applied; state=cancelled',
            'ready2\tR\tapplied\tcancel applied; state=cancelled',
            'done\tR\trefused\tthe job is already cancelled; state=cancelled',
            'held\tR\tqueued\tanother owner holds the job; the request applies at its next boundary; state=owned',
            'moving\tR\tapplied\tcancel applied; state=cancelled',
        ],
        'states': {
            'ready1': 'cancelled/p500',
            'ready2': 'cancelled/p500',
            'done': 'cancelled/p500',
            'held': 'ready/p500',
            'moving': 'cancelled/p400',
        },
        'requests_left': 1,
    },
    'full': {
        'status': 1,
        'stdout': [
            'ready1\tR\tapplied\tcancel applied; state=cancelled',
            'ready2\tR\tapplied\tcancel applied; state=cancelled',
            'done\tR\trefused\tthe job is already cancelled; state=cancelled',
            'held\tR\tqueued\tanother owner holds the job; the request applies at its next boundary; state=owned',
            'moving\tR\tapplied\tcancel applied; state=cancelled',
        ],
        'states': {
            'ready1': 'cancelled/p500',
            'ready2': 'cancelled/p500',
            'done': 'cancelled/p500',
            'held': 'ready/p500',
            'moving': 'cancelled/p400',
        },
        'requests_left': 1,
    },
    'state': {
        'status': 1,
        'stdout': [
            'done\tR\trefused\tthe job is already cancelled; state=cancelled',
            'held\tR\tqueued\tanother owner holds the job; the request applies at its next boundary; state=owned',
            'moving\tR\tapplied\tcancel applied; state=cancelled',
            'ready1\tR\tapplied\tcancel applied; state=cancelled',
            'ready2\tR\tapplied\tcancel applied; state=cancelled',
        ],
        'states': {
            'ready1': 'cancelled/p500',
            'ready2': 'cancelled/p500',
            'done': 'cancelled/p500',
            'held': 'ready/p500',
            'moving': 'cancelled/p400',
        },
        'requests_left': 1,
    },
}


@pytest.mark.parametrize("selector", ["prefix", "full", "state"])
def test_job_request_cancel_outcomes_match_the_serial_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, selector: str
) -> None:
    workspace, name = _new_workspace(tmp_path)
    jobs = {
        label: _job(tmp_path, workspace, f"par-{label}") for label in ("ready1", "ready2", "done", "held", "moving")
    }
    jobs["done"] = _at(workspace, jobs["done"], "cancelled")
    labels = {ref.job_id: label for label, ref in jobs.items()}
    if selector == "prefix":
        chosen = [f"par-{label}" for label in jobs] + [jobs["ready1"].job_id]  # one job named twice
    elif selector == "full":
        chosen = [ref.job_id for ref in jobs.values()] + [jobs["ready2"].job_id]
    else:
        chosen = ["--state", "ready", jobs["done"].job_id, jobs["held"].job_id]
    selected = job_cli._selected_jobs

    def moving_after_selection(*args: object, **kwargs: object) -> list[_kernel.JobRef]:
        refs = selected(*args, **kwargs)  # type: ignore[arg-type]
        _at(workspace, v3.find(workspace, jobs["moving"].job_id), "ready", 400)  # a manager moves it meanwhile
        return refs

    monkeypatch.setattr(job_cli, "_selected_jobs", moving_after_selection)
    with _held(workspace, jobs["held"]):
        status = command(
            ["job", "request", "cancel", "--workspace", name, "--reason", "parity", *chosen], _context(tmp_path)
        )
        output = capsys.readouterr()
    observed = {
        "status": status,
        "stdout": _normalizer(labels)(output.out),
        "states": _states(workspace, jobs),
        "requests_left": _requests_left(workspace),
    }
    _pinned(f"cancel-{selector}", observed, _CANCEL_PARITY[selector])


_DELETE_PARITY: dict[str, object] = {
    'prefix': {
        'status': 1,
        'stdout': [
            'par-gone1--gone1\tcancelled\tremoved',
            'par-gone2--gone2\tcancelled\tremoved',
            'par-ready--ready\tready\trefused\tonly terminal or paused jobs are deleted, not ready',
            'par-held--held\towned\tqueued\tanother owner holds the job; the request applies at its next boundary',
            'par-moving--moving\tcancelled\tremoved',
            'removed 3 of 5 job(s)',
        ],
        'states': {'gone1': None, 'gone2': None, 'ready': 'ready/p500', 'held': 'cancelled/p500', 'moving': None},
        'requests_left': 1,
    },
    'full': {
        'status': 1,
        'stdout': [
            'par-gone1--gone1\tcancelled\tremoved',
            'par-gone2--gone2\tcancelled\tremoved',
            'par-ready--ready\tready\trefused\tonly terminal or paused jobs are deleted, not ready',
            'par-held--held\towned\tqueued\tanother owner holds the job; the request applies at its next boundary',
            'par-moving--moving\tcancelled\tremoved',
            'removed 3 of 5 job(s)',
        ],
        'states': {'gone1': None, 'gone2': None, 'ready': 'ready/p500', 'held': 'cancelled/p500', 'moving': None},
        'requests_left': 1,
    },
}


@pytest.mark.parametrize("selector", ["prefix", "full"])
def test_job_delete_outcomes_match_the_serial_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, selector: str
) -> None:
    workspace, name = _new_workspace(tmp_path)
    jobs = {label: _job(tmp_path, workspace, f"par-{label}") for label in ("gone1", "gone2", "ready", "held", "moving")}
    for label in ("gone1", "gone2", "held", "moving"):
        jobs[label] = _at(workspace, jobs[label], "cancelled")
    labels = {ref.job_id: label for label, ref in jobs.items()}
    if selector == "prefix":
        chosen = [f"par-{label}" for label in jobs] + [jobs["gone1"].job_id]
    else:
        chosen = [ref.job_id for ref in jobs.values()] + [jobs["gone2"].job_id]
    resolve = job_cli._resolve_selectors

    def moving_after_selection(*args: object, **kwargs: object) -> list[_kernel.JobRef]:
        refs = resolve(*args, **kwargs)  # type: ignore[arg-type]
        _at(workspace, v3.find(workspace, jobs["moving"].job_id), "cancelled", 400)
        return refs

    monkeypatch.setattr(job_cli, "_resolve_selectors", moving_after_selection)
    with _held(workspace, jobs["held"]):
        status = command(["job", "delete", "--workspace", name, "--force", *chosen], _context(tmp_path))
        output = capsys.readouterr()
    observed = {
        "status": status,
        "stdout": _normalizer(labels)(output.out),
        "states": _states(workspace, jobs),
        "requests_left": _requests_left(workspace),
    }
    _pinned(f"delete-{selector}", observed, _DELETE_PARITY[selector])


_UNKNOWN_PARITY: dict[str, object] = {
    'request': {'status': 2, 'stdout': [], 'state': 'ready/p500', 'requests_left': 0},
    'delete': {'status': 2, 'stdout': [], 'state': 'ready/p500', 'requests_left': 0},
}


def test_an_unknown_selector_posts_nothing(tmp_path: Path, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    ready = _job(tmp_path, workspace, "par-known")
    unknown = "00000000-0000-4000-8000-000000000000"
    observed: dict[str, object] = {}
    for verb in (["request", "cancel", "--reason", "parity"], ["delete", "--force"]):
        try:
            status: object = command(["job", *verb, "--workspace", name, ready.job_id, unknown], _context(tmp_path))
        except ValueError as exc:
            status = f"ValueError: {exc}".replace(str(workspace.root), "WS")
        output = capsys.readouterr()
        observed[verb[0]] = {
            "status": status,
            "stdout": _normalizer({ready.job_id: "known"})(output.out),
            "state": _states(workspace, {"known": ready})["known"],
            "requests_left": _requests_left(workspace),
        }
    _pinned("unknown", observed, _UNKNOWN_PARITY)


_DUPLICATE_PARITY: dict[str, object] = {
    'outcomes': [[True, None, 'applied'], [False, 'the job was not found', 'uncertain']],
    'requests_left': 0,
}


def test_remove_jobs_given_one_job_twice_matches_the_serial_code(tmp_path: Path) -> None:
    workspace, _name = _new_workspace(tmp_path)
    done = _at(workspace, _job(tmp_path, workspace, "par-twice"), "cancelled")
    report = removal.remove_jobs(workspace, [done, done])
    observed = [
        [outcome.removed, outcome.reason, None if outcome.request is None else outcome.request.status]
        for outcome in report.outcomes
    ]
    _pinned("duplicate", {"outcomes": observed, "requests_left": _requests_left(workspace)}, _DUPLICATE_PARITY)


# -- the location index ----------------------------------------------------------------------------------------------


@pytest.fixture
def exhaustive_locates(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count the exhaustive :func:`~httk.workflow._kernel.locate` calls by job id."""

    counts: dict[str, int] = {}
    original = _kernel.locate

    def counting(workspace: object, job_id: str, **options: object) -> _kernel.JobRef | None:
        if options.get("exhaustive"):
            counts[job_id] = counts.get(job_id, 0) + 1
        return original(workspace, job_id, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel, "locate", counting)
    return counts


@pytest.mark.parametrize("backlog", [False, True])
def test_a_stale_job_costs_at_most_one_exhaustive_locate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, exhaustive_locates: dict[str, int], backlog: bool
) -> None:
    workspace, name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, f"idx-{number}") for number in range(6)]
    stale = {refs[1].job_id, refs[4].job_id}
    if backlog:
        # An older pending request on every job: served first, through the job's own move.
        for ref in refs:
            _requests.post(
                workspace,
                action="set_priority",
                job_id=ref.job_id,
                placement=ref.placement or "",
                operator="tester",
                reason="older",
                priority=321,
                created_at="2026-01-01T00:00:00Z",
            )
    resolve = job_cli._resolve_selectors

    def moving_after_selection(*args: object, **kwargs: object) -> list[_kernel.JobRef]:
        selected = resolve(*args, **kwargs)  # type: ignore[arg-type]
        for job_id in stale:
            _at(workspace, v3.find(workspace, job_id), "ready", 400)
        exhaustive_locates.clear()  # the selection's own lookups are not the serving's
        return selected

    monkeypatch.setattr(job_cli, "_resolve_selectors", moving_after_selection)
    chosen = [ref.job_id for ref in refs]
    assert command(["job", "request", "cancel", "--workspace", name, "--reason", "r", *chosen], _context(tmp_path)) == 0
    assert capsys.readouterr().out.count("\tapplied\t") == 6
    assert {job_id: count for job_id, count in exhaustive_locates.items() if count} == dict.fromkeys(stale, 1)
    for ref in refs:
        current = v3.find(workspace, ref.job_id)
        assert current.state == "cancelled" and current.priority == (
            321 if backlog else 400 if ref.job_id in stale else 500
        )


def test_full_id_selectors_share_one_listing(tmp_path: Path, exhaustive_locates: dict[str, int]) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, f"sel-{number}") for number in range(4)]
    owned_ref = refs[3]
    with _held(workspace, owned_ref):
        index = _reading._location_index(workspace)
        resolved = _reading._resolve_selectors(workspace, tmp_path, [ref.job_id for ref in refs], index)
    assert [ref.job_id for ref in resolved] == [ref.job_id for ref in refs]
    assert resolved[3].state == _kernel.OWNED  # an owned entry is answered by the walk, as before
    assert exhaustive_locates == {owned_ref.job_id: 1}


def test_an_entry_is_trusted_only_after_an_lstat(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "lstat")
    fallbacks: list[str] = []
    original = _kernel.locate

    def counting(workspace: object, job_id: str, **options: object) -> _kernel.JobRef | None:
        fallbacks.append(job_id)
        return original(workspace, job_id, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel, "locate", counting)
    index = _kernel.LocationIndex(workspace)
    assert index.locate(ref.job_id, placement_hint=None, exhaustive=True) == ref
    assert fallbacks == []
    moved = _at(workspace, ref, "paused")  # the listed directory is gone
    fallbacks.clear()
    assert index.locate(ref.job_id, placement_hint=None, exhaustive=True) == moved
    assert fallbacks == [ref.job_id]
    assert index.locate(ref.job_id, placement_hint=ref.placement) == moved
    assert fallbacks == [ref.job_id]


def test_a_full_id_a_walk_finds_missing_is_not_resolved_from_an_older_listing(tmp_path: Path) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first, second = (_at(workspace, _job(tmp_path, workspace, f"ab-{tag}"), "cancelled") for tag in ("a", "b"))
    resolver = _reading.JobSelectorResolver(workspace, tmp_path)
    assert resolver.resolve_one(first.job_id) == [first]  # takes the listing, which holds the second job too
    removal.remove_jobs(workspace, [second])  # another command deletes it
    with pytest.raises(ValueError, match="no job in .* matches"):
        resolver.resolve_one(second.job_id)


# -- one verification of the bytes this process posted ---------------------------------------------------------------


@pytest.fixture
def verifications(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Record every request signature verification."""

    calls: list[object] = []
    original = _requests.operator_key

    def counting(document: object) -> str | None:
        calls.append(document)
        return original(document)  # type: ignore[arg-type]

    monkeypatch.setattr(_requests, "operator_key", counting)
    return calls


def test_posted_bytes_are_verified_once_and_changed_bytes_again(tmp_path: Path, verifications: list[object]) -> None:
    from httk.core.identity import identity_key_paths

    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "memo")
    seed = identity_key_paths("tester")[0]
    path = _requests.post(
        workspace,
        action="pause",
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="t",
        reason="r",
        seed_path=seed,
    )
    assert len(verifications) == 1  # at posting
    assert _requests.parse(path).action == "pause"
    assert len(verifications) == 1  # the same bytes: not verified again
    document = json.loads(path.read_bytes())
    path.write_text(json.dumps(document, indent=1), encoding="utf-8")  # the same document, other bytes
    assert _requests.parse(path).action == "pause"
    assert len(verifications) == 2
    document["reason"] = "tampered"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(FormatError, match="signature"):
        _requests.parse(path)


def test_a_job_request_verifies_each_signature_once(tmp_path: Path, capsys, verifications: list[object]) -> None:
    workspace, name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, f"once-{number}") for number in range(3)]
    verifications.clear()
    chosen = [ref.job_id for ref in refs]
    assert command(["job", "request", "pause", "--workspace", name, "--reason", "r", *chosen], _context(tmp_path)) == 0
    assert capsys.readouterr().out.count("\tapplied\t") == 3
    assert len(verifications) == 3


# -- chunks and withdrawal --------------------------------------------------------------------------------------------


def test_requests_are_posted_and_served_in_chunks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, f"chunk-{number}") for number in range(5)]
    events: list[str] = []
    post, serve_one = _kernel.post_request, removal._serve_one

    def posting(*args: object, **kwargs: object) -> Path:
        events.append("post")
        return post(*args, **kwargs)  # type: ignore[arg-type]

    def serving(*args: object, **kwargs: object) -> object:
        events.append("serve")
        return serve_one(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel, "post_request", posting)
    monkeypatch.setattr(removal, "_serve_one", serving)
    monkeypatch.setattr(removal, "_CHUNK_JOBS", 2)
    chosen = [ref.job_id for ref in refs]
    assert command(["job", "request", "cancel", "--workspace", name, "--reason", "r", *chosen], _context(tmp_path)) == 0
    assert events == ["post", "post", "serve", "serve"] * 2 + ["post", "serve"]
    assert capsys.readouterr().out.count("\tapplied\t") == 5
    events.clear()
    with v3.cli_owner(workspace) as owner:
        outcomes = removal.request_jobs(
            workspace, owner, [v3.find(workspace, ref.job_id) for ref in refs], "delete", "r"
        )
    assert [outcome.status for outcome in outcomes] == ["applied"] * 5
    assert events == ["post", "post", "serve", "serve"] * 2 + ["post", "serve"]
    assert removal._CHUNK_JOBS == 2 and len(list(removal._chunks([(str(n), n) for n in range(512)]))) == 256


def test_the_default_chunk_holds_256_jobs() -> None:
    chunks = list(removal._chunks([(str(number), number) for number in range(600)]))
    assert [len(chunk) for chunk in chunks] == [256, 256, 88]
    # A job named again starts a new chunk: its second request is posted after the first was served.
    assert [[job for job, _ in chunk] for chunk in removal._chunks([("a", 1), ("b", 2), ("a", 3)])] == [
        ["a", "b"],
        ["a"],
    ]


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Record request-file unlinks and every fsync, in order, by path."""

    events: list[tuple[str, str]] = []
    remove, fsync = _requests._fs.remove_file, os.fsync

    def removing(target: object, *, durable: bool) -> None:
        events.append(("unlink" if not durable else "unlink+fsync", str(target.path)))  # type: ignore[attr-defined]
        remove(target, durable=durable)  # type: ignore[arg-type]

    def fsyncing(descriptor: int) -> None:
        events.append(("fsync", os.readlink(f"/proc/self/fd/{descriptor}")))
        fsync(descriptor)

    monkeypatch.setattr(_requests._fs, "remove_file", removing)
    monkeypatch.setattr(os, "fsync", fsyncing)
    return events


def _is(descriptor: int, path: str) -> bool:
    return os.readlink(f"/proc/self/fd/{descriptor}") == path


def _moves_into(journal: list[tuple[str, str]], directory: Path) -> list[int]:
    """The journal positions of the fsyncs of *directory*: a move into it, or out of it, was made durable."""

    return [index for index, (kind, path) in enumerate(journal) if kind == "fsync" and path == str(directory)]


def test_an_uncertain_outcome_withdraws_that_request_while_holding_its_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, journal: list[tuple[str, str]], caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"unc-{number}"), "cancelled") for number in range(3)]
    serve = removal.serve

    def failing(workspace: Workspace, owner: _kernel.Owner, ref: _kernel.JobRef, **options: object) -> bool:
        if ref.job_id == refs[1].job_id:
            raise WorkflowError("cannot read the job")  # before anything of the request's application
        return serve(workspace, owner, ref, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "serve", failing)
    caplog.set_level(logging.INFO, logger="httk.workflow.manager")
    journal.clear()
    report = removal.remove_jobs(workspace, refs)
    assert [outcome.removed for outcome in report.outcomes] == [True, False, True]
    request = report.outcomes[1].request
    assert request is not None and request.status == "uncertain"
    assert _requests_left(workspace) == 0  # withdrawn: no delete outlives the command unseen
    assert v3.find(workspace, refs[1].job_id).state == "cancelled"
    owned = workspace.jobs / "owned"
    unlinks = [index for index, (kind, path) in enumerate(journal) if kind == "unlink" and request.request_id in path]
    assert len(unlinks) == 1
    # Claimed from cancelled (both directories fsynced), unlinked while held, requests/ fsynced once, given back.
    requests = _moves_into(journal, workspace.control / "requests")
    claims = [index for index, (kind, path) in enumerate(journal) if kind == "fsync" and path.startswith(str(owned))]
    assert [index for index in requests if index > unlinks[0]] and min(
        i for i in requests if i > unlinks[0]
    ) == unlinks[0] + 1
    back = _moves_into(journal, refs[1].path.parent)
    assert back[-2] < unlinks[0] < back[-1]  # the claim's fsync before the unlink, the give-back's after
    assert claims
    assert f"request {request.request_id} for job {refs[1].job_id} withdrawn while held by this command" in caplog.text


def test_a_request_whose_job_cannot_be_held_is_kept_and_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"gone-{number}"), "cancelled") for number in range(2)]
    aside = tmp_path / "aside"
    snapshot = removal._order_snapshot

    def vanishing(*args: object, **kwargs: object) -> object:
        taken = snapshot(*args, **kwargs)  # type: ignore[arg-type]
        v3.find(workspace, refs[1].job_id).path.rename(aside)  # out of sight (in transit, say): it cannot be held
        return taken

    monkeypatch.setattr(removal, "_order_snapshot", vanishing)
    report = removal.remove_jobs(workspace, refs)
    request = report.outcomes[1].request
    assert request is not None and (request.status, request.explanation) == (
        "uncertain",
        "job disappeared before the request was observed",
    )
    assert _requests_left(workspace) == 1
    assert f"request {request.request_id} for job {refs[1].job_id} was not withdrawn (the job was not found)" in (
        caplog.text
    )
    assert "it may still be applied" in caplog.text


def test_a_request_another_owner_is_applying_is_never_withdrawn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first, second = (_at(workspace, _job(tmp_path, workspace, f"race-{tag}"), "cancelled") for tag in ("a", "b"))
    apply = removal.apply_requests
    other: list[tuple[_kernel.Owner, _kernel.OwnedJob]] = []

    def racing(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == first.job_id:
            # Meanwhile another owner claimed the second job and replaced its state; it pauses before its barrier.
            owner = _kernel.register_owner(workspace, kind="cli", label="other", allocation=None, advertised={})
            held = _kernel.claim(workspace, owner, v3.find(workspace, second.job_id))
            assert held is not None
            held.write_state((held.read_state() or StateDoc.empty(held.job_id)).updated(owner_id=owner.owner_id))
            other.append((owner, held))
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", racing)
    with pytest.raises(KeyboardInterrupt):
        removal.remove_jobs(workspace, [first, second])
    left = sorted(name.split(".")[0] for name in os.listdir(workspace.control / "requests") if name.endswith(".json"))
    assert left == [second.job_id]  # the other owner's request is still there for it
    assert f"for job {second.job_id} was not withdrawn (another owner holds the job); it may still be applied" in (
        caplog.text
    )
    owner, held = other[0]
    with owner:
        held.give_back()


def test_an_uncertain_request_whose_application_began_stays_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, journal: list[tuple[str, str]], caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, f"began-{number}") for number in range(2)]

    def failing(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> _kernel.JobRef:
        # After the request's run-log record, its first durable mutation: its effect may have begun.
        owned.give_back()
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(removal, "release", failing)
    with v3.cli_owner(workspace) as owner:
        outcomes = removal.request_jobs(workspace, owner, refs[:1], "cancel", "r")
    assert outcomes[0].status == "uncertain"
    assert _requests_left(workspace) == 1
    assert not [event for event in journal if event[0] == "unlink"]
    assert f"request {outcomes[0].request_id} for job {refs[0].job_id} stays pending" in caplog.text


def test_an_interrupt_withdraws_the_unserved_requests_of_its_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, journal: list[tuple[str, str]], caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"int-{number}"), "cancelled") for number in range(4)]
    apply = removal.apply_requests

    def interrupted(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == refs[1].job_id:
            raise KeyboardInterrupt  # held, before anything of the request's application
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", interrupted)
    caplog.set_level(logging.INFO, logger="httk.workflow.manager")
    journal.clear()
    with pytest.raises(KeyboardInterrupt):
        removal.remove_jobs(workspace, refs)
    assert _kernel.locate(workspace, refs[0].job_id, placement_hint=None, exhaustive=True) is None
    # The job being served and the rest of the chunk are given back unchanged, without an armed delete.
    assert [v3.find(workspace, ref.job_id).state for ref in refs[1:]] == ["cancelled"] * 3
    assert _requests_left(workspace) == 0
    assert not list((workspace.control / "owners").iterdir())
    unlinks = [index for index, (kind, _path) in enumerate(journal) if kind == "unlink"]
    assert len(unlinks) == 3
    requests = [index for index in _moves_into(journal, workspace.control / "requests") if index > unlinks[-1]]
    assert requests and requests[0] == unlinks[-1] + 1  # one fsync of requests/ for the three
    for ref in refs[1:]:
        # Each job goes back only after that fsync (int-1 was held by the serving, the others claimed for it).
        assert _moves_into(journal, ref.path.parent)[-1] > requests[0]
    assert f"job {refs[1].job_id} was being served when the command stopped" in caplog.text
    assert all(re.search(rf"request \S+ for job {ref.job_id} withdrawn while held", caplog.text) for ref in refs[1:])


@pytest.mark.parametrize("failure", ["parent fsync", "interrupt"])
def test_a_post_that_published_and_then_raised_is_withdrawn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"pub-{number}"), "cancelled") for number in range(3)]
    requests = str(workspace.control / "requests")
    posts: list[Path] = []
    post, fsync = _kernel.post_request, os.fsync

    def posting(workspace: Workspace, document: dict[str, object]) -> Path:
        posts.append(post(workspace, document))
        if failure == "interrupt" and len(posts) == 2:
            raise KeyboardInterrupt  # right after the second request was published
        return posts[-1]

    def fsyncing(descriptor: int) -> None:
        second_named = any(name.startswith(refs[1].job_id) for name in os.listdir(requests))
        if failure == "parent fsync" and len(posts) == 1 and second_named and _is(descriptor, requests):
            raise OSError(5, "Input/output error")  # the second request was renamed into place, then this
        fsync(descriptor)

    monkeypatch.setattr(_kernel, "post_request", posting)
    monkeypatch.setattr(os, "fsync", fsyncing)
    with pytest.raises(OSError if failure == "parent fsync" else KeyboardInterrupt):
        removal.remove_jobs(workspace, refs)
    assert _requests_left(workspace) == 0
    assert [v3.find(workspace, ref.job_id).state for ref in refs] == ["cancelled"] * 3


def test_a_failed_post_withdraws_what_its_chunk_posted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"post-{number}"), "cancelled") for number in range(3)]
    post = _kernel.post_request
    posted: list[Path] = []

    def failing(*args: object, **kwargs: object) -> Path:
        if len(posted) == 2:
            raise OSError(28, "No space left on device")
        posted.append(post(*args, **kwargs))  # type: ignore[arg-type]
        return posted[-1]

    monkeypatch.setattr(_kernel, "post_request", failing)
    with pytest.raises(OSError, match="No space"):
        removal.remove_jobs(workspace, refs)
    assert _requests_left(workspace) == 0
    assert [v3.find(workspace, ref.job_id).state for ref in refs] == ["cancelled"] * 3


def test_a_job_that_cannot_be_read_is_uncertain_and_the_chunk_goes_on(tmp_path: Path) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"read-{number}"), "cancelled") for number in range(2)]
    # An owned reference, as a selector finds a held job: its placement is read from job.json, which is gone.
    owned = _kernel.JobRef.from_path(
        workspace.jobs / "owned" / ("0" * 32) / f"{refs[0].path.name}~cancelled", state=_kernel.OWNED, owner_id="0" * 32
    )
    report = removal.remove_jobs(workspace, [owned, refs[1]])
    request = report.outcomes[0].request
    assert request is not None and request.status == "uncertain" and "cannot be read" in request.explanation
    assert report.outcomes[1].removed


def test_an_interrupt_reports_the_outcomes_known_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, f"known-{number}") for number in range(3)]
    apply = removal.apply_requests
    reported: list[list[str]] = []

    def interrupted(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == refs[1].job_id:
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", interrupted)
    with pytest.raises(KeyboardInterrupt), v3.cli_owner(workspace) as owner:
        removal.request_jobs(
            workspace,
            owner,
            refs,
            "pause",
            "r",
            report=lambda outcomes: reported.append([f"{o.job_id} {o.status}" for o in outcomes]),
        )
    assert reported == [[f"{refs[0].job_id} applied"]]


def test_local_continue_tree_withdraws_its_unserved_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"tree-{number}"), "paused") for number in range(2)]
    apply = removal.apply_requests

    def interrupted(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == refs[0].job_id:
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", interrupted)
    identity = job_cli._resolve_request_identity(None)
    index = _reading._location_index(workspace)
    with pytest.raises(KeyboardInterrupt):
        job_cli._continue_tree(
            workspace, refs, reason="r", force=False, keep_budget=False, identity=identity, index=index
        )
    assert _requests_left(workspace) == 0  # withdrawn while held; the second was never posted
    assert [v3.find(workspace, ref.job_id).state for ref in refs] == ["paused", "paused"]
    monkeypatch.setattr(removal, "apply_requests", apply)
    assert (
        job_cli._continue_tree(
            workspace, refs, reason="r", force=False, keep_budget=False, identity=identity, index=index
        )
        == 0
    )
    assert capsys.readouterr().out.count("\tapplied\t") == 2
    assert [v3.find(workspace, ref.job_id).state for ref in refs] == ["ready", "ready"]


def test_a_queued_request_is_not_withdrawn(tmp_path: Path) -> None:
    workspace, _name = _new_workspace(tmp_path)
    done = _at(workspace, _job(tmp_path, workspace, "queued"), "cancelled")
    with _held(workspace, done):
        report = removal.remove_jobs(workspace, [done])
        assert report.outcomes[0].request is not None and report.outcomes[0].request.status == "queued"
        assert _requests_left(workspace) == 1


# -- the fsync sequence of one job ------------------------------------------------------------------------------------


_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_HEX32 = re.compile(r"(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])")
_TOKEN = re.compile(r"(?<![a-z2-7])[a-z2-7]{16}(?![a-z2-7])")


def _normalized(paths: Sequence[str], root: Path) -> list[str]:
    """Each path relative to *root*, every UUID and owner id by first appearance, every fresh token as ``T``."""

    labels: dict[str, str] = {}

    def label(prefix: str) -> Callable[[re.Match[str]], str]:
        def named(match: re.Match[str]) -> str:
            if match.group() not in labels:
                labels[match.group()] = f"{prefix}{sum(1 for value in labels.values() if value[0] == prefix)}"
            return labels[match.group()]

        return named

    normalized = []
    for path in paths:
        text = os.path.relpath(path, root) if path.startswith(str(root)) else f"<outside>/{Path(path).name}"
        text = _UUID.sub(label("U"), text)
        text = _HEX32.sub(label("O"), text)
        normalized.append(_TOKEN.sub("T", text))
    return normalized


@pytest.fixture
def fsyncs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record the path of every ``fsync`` and ``fdatasync`` descriptor, in call order."""

    recorded: list[str] = []
    for name in ("fsync", "fdatasync"):
        original = getattr(os, name)

        def wrapper(descriptor: int, _original: Callable[[int], None] = original) -> None:
            try:
                recorded.append(os.readlink(f"/proc/self/fd/{descriptor}"))
            except OSError:
                recorded.append("<unknown>")
            _original(descriptor)

        monkeypatch.setattr(os, name, wrapper)
    return recorded


_SEQUENCE: dict[str, list[str]] = {
    'cancel': [
        '.httk-workspace/requests/.U0.U1.json.T.tmp',
        '.httk-workspace/requests',
        '.httk-workspace/owners',
        '.httk-workspace/owners/O0/.owner.json.T.tmp',
        '.httk-workspace/owners/O0',
        'jobs/owned',
        'jobs/ready/project/seq-1',
        'jobs/owned/O0',
        'jobs/owned/O0/seq-1--U0~p500~T~ready',
        'jobs/owned/O0/seq-1--U0~p500~T~ready/logs',
        'jobs/owned/O0/seq-1--U0~p500~T~ready/logs/runlog.jsonl',
        'jobs/owned/O0/seq-1--U0~p500~T~ready/logs/runlog.jsonl',
        'jobs/owned/O0/seq-1--U0~p500~T~ready/logs/runlog.jsonl',
        'jobs/owned/O0/seq-1--U0~p500~T~ready/.state.json.T.tmp',
        'jobs/owned/O0/seq-1--U0~p500~T~ready',
        '.httk-workspace/requests',
        'jobs/cancelled',
        'jobs/cancelled/project',
        'jobs/owned/O0',
        'jobs/cancelled/project/seq-1',
        '.httk-workspace/owners/O0',
        '.httk-workspace/owners/O0',
    ],
    'delete': [
        '.httk-workspace/owners',
        '.httk-workspace/owners/O0/.owner.json.T.tmp',
        '.httk-workspace/owners/O0',
        '.httk-workspace/requests/.U0.U1.json.T.tmp',
        '.httk-workspace/requests',
        'jobs/owned',
        'jobs/cancelled/project/seq-1',
        'jobs/owned/O0',
        'jobs/owned/O0/seq-1--U0~p500~T~cancelled/logs/runlog.jsonl',
        'jobs/owned/O0/seq-1--U0~p500~T~cancelled/logs/runlog.jsonl',
        '.httk-workspace/tmp',
        'jobs/owned/O0',
        '.httk-workspace/tmp/O0.trash.T',
        '.httk-workspace/requests',
        '.httk-workspace/tmp',
        '.httk-workspace/owners/O0',
        '.httk-workspace/owners/O0',
    ],
}


def test_the_fsync_sequence_of_one_job_is_unchanged(tmp_path: Path, fsyncs: list[str], capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "seq-1")
    context = _context(tmp_path)
    fsyncs.clear()
    assert command(["job", "request", "cancel", "--workspace", name, "--reason", "r", ref.job_id], context) == 0
    cancel = _normalized(fsyncs, workspace.root)
    fsyncs.clear()
    assert command(["job", "delete", "--workspace", name, "--force", ref.job_id], context) == 0
    delete = _normalized(fsyncs, workspace.root)
    capsys.readouterr()
    _pinned("fsync-sequence", {"cancel": cancel, "delete": delete}, _SEQUENCE)


# -- no needless reinstall -------------------------------------------------------------------------------------------


def _installed_at(workspace: Workspace) -> dict[str, object]:
    return {found.id: found.record["installed_at"] for found in _store.list_installed(workspace)}


@pytest.fixture
def git_repository(tmp_path: Path) -> Iterator[tuple[Path, str]]:
    """A repository holding the workflow ``tests.git.relax`` in ``relax/``, with its first commit."""

    import shutil

    from httk.workflow import git_workflows
    from test_git_workflows import _repository

    if shutil.which("git") is None:
        pytest.skip("git is not available")
    git_workflows._reset_fetched_workflow_cache()
    yield _repository(tmp_path / "repo", {"relax": "tests.git.relax"})
    git_workflows._reset_fetched_workflow_cache()


def _new_installed(workspace: Workspace, uri: str) -> str:
    """``job new --install`` of *uri* in a fresh fetch cache; the id of the workflow the job runs."""

    from httk.workflow import git_workflows
    from httk.workflow.scaffold import new_job

    git_workflows._reset_fetched_workflow_cache()
    return new_job(workspace, uri, install=True).workflow


def test_job_new_install_keeps_an_installation_it_would_only_repeat(
    tmp_path: Path, git_repository: tuple[Path, str]
) -> None:
    from test_git_workflows import _git
    from test_job_creation import workspace_at

    root, first = git_repository
    workspace = workspace_at(tmp_path / "workspace")
    uri = f"git+file://{root}@{first}#relax"
    assert _new_installed(workspace, f"git+file://{root}@main#relax") == uri
    installed = _installed_at(workspace)
    assert _new_installed(workspace, f"git+file://{root}@main#relax") == uri
    assert _installed_at(workspace) == installed  # the same reference, sources and newest: kept

    # Another reference to the same commit is recorded by a reinstall (NAME@REF selects by it).
    _git(root, "branch", "other")
    assert _new_installed(workspace, f"git+file://{root}@other#relax") == uri
    assert _installed_at(workspace) != installed
    found = _store.lookup(workspace, uri)
    assert found is not None and found.record["reference"] == "other"


def test_job_new_install_reinstalls_other_sources_or_an_older_installation(
    tmp_path: Path, git_repository: tuple[Path, str]
) -> None:
    from test_git_workflows import _commit, _git
    from test_job_creation import workspace_at

    root, first = git_repository
    workspace = workspace_at(tmp_path / "workspace")
    main = f"git+file://{root}@main#relax"
    uri = f"git+file://{root}@{first}#relax"
    assert _new_installed(workspace, main) == uri
    found = _store.lookup(workspace, uri)
    assert found is not None
    # Recorded from other sources than these: installed again, recording these.
    record = {**found.record, "tree_sha256": "0" * 64}
    (found.directory / "install.json").write_text(json.dumps(record), encoding="utf-8")
    installed = _installed_at(workspace)
    assert _new_installed(workspace, main) == uri
    assert _installed_at(workspace) != installed
    again = _store.lookup(workspace, uri)
    assert again is not None and again.record["tree_sha256"] == _store.source_digest(again.package)

    # Not the newest of its short name any more: installed again, so the short name selects it as before.
    second = _commit(root, "two")
    assert _new_installed(workspace, main) == f"git+file://{root}@{second}#relax"
    _git(root, "reset", "-q", "--hard", first)
    installed = _installed_at(workspace)
    assert _new_installed(workspace, main) == uri
    assert _installed_at(workspace)[uri] != installed[uri]
    newest = _store.lookup(workspace, "tests.git.relax")
    assert newest is not None and newest.id == uri


@pytest.mark.parametrize("damage", ["modified", "deleted"])
def test_job_new_install_reinstalls_an_installation_whose_files_changed(
    tmp_path: Path, git_repository: tuple[Path, str], damage: str
) -> None:
    from test_job_creation import workspace_at

    root, first = git_repository
    workspace = workspace_at(tmp_path / "workspace")
    main = f"git+file://{root}@main#relax"
    uri = f"git+file://{root}@{first}#relax"
    assert _new_installed(workspace, main) == uri
    found = _store.lookup(workspace, uri)
    assert found is not None
    installed = _installed_at(workspace)
    target = found.installed_package / "collect.py"
    if damage == "modified":
        target.write_text(target.read_text(encoding="utf-8") + "# edited\n", encoding="utf-8")
    else:
        target.unlink()
    # install.json is left as it was: only the installed tree tells.
    assert _new_installed(workspace, main) == uri
    assert _installed_at(workspace) != installed
    again = _store.lookup(workspace, uri)
    assert again is not None and _store.source_digest(again.installed_package) == again.record["tree_sha256"]


def _parented(tmp_path: Path, workspace: Workspace, parent: _kernel.JobRef, tag: str, placement: str) -> _kernel.JobRef:
    """Submit a job of the demo workflow spawned by *parent*."""

    installed = _store.lookup(workspace, "demo") or v3.install(workspace, tmp_path / "package")
    mapping = v3.job_mapping(installed, {"start": "succeed"}, tag=tag, placement=placement)
    mapping["parent"] = {
        "workspace_id": workspace.workspace_id,
        "job_id": parent.job_id,
        "job_key": parent.job_key,
        "placement": str(parent.placement),
        "activation_id": "00000000-0000-4000-8000-000000000001",
        "spawn_id": "00000000-0000-4000-8000-000000000002",
    }
    return v3.submit_mapping(workspace, mapping)


def test_one_command_lists_its_selection_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    parent = _job(tmp_path, workspace, "list-parent", "lists/parent")
    child = _parented(tmp_path, workspace, parent, "list-child", "lists/children/child")
    others = [_job(tmp_path, workspace, f"list-{number}", f"lists/children/other-{number}") for number in range(3)]
    listings: list[object] = []
    locates: list[tuple[str, object]] = []
    iter_jobs, locate = _reading.iter_jobs, _kernel.locate

    def listing(*args: object, **kwargs: object) -> Iterator[_kernel.JobRef]:
        listings.append((args[1:], kwargs))
        return iter_jobs(*args, **kwargs)  # type: ignore[arg-type]

    def locating(workspace: object, job_id: str, **options: object) -> _kernel.JobRef | None:
        locates.append((job_id, options.get("exhaustive", False)))
        return locate(workspace, job_id, **options)  # type: ignore[arg-type]

    monkeypatch.setattr(_reading, "iter_jobs", listing)
    monkeypatch.setattr(_kernel, "locate", locating)
    # --state selection, a full-id selector and the ancestors of an unlock, in one command.
    argv = ["job", "request", "unlock", "--workspace", name, "--reason", "r", "--state", "ready"]
    argv += ["--placement", "lists/children", child.job_id]
    assert command(argv, _context(tmp_path)) == 0
    output = capsys.readouterr().out
    assert output.count("\tapplied\t") == 4  # the child and the three others
    assert f"{parent.job_id}\tunlocked (ancestor)" in output  # and, marker only, the parent
    # One listing of the --state selection, shared with serving; the parent, outside it, is probed once at its
    # placement (as before) and then known.
    assert listings == [((("ready",),), {"placement_prefix": "lists/children"})]
    assert locates == [(parent.job_id, False)]
    assert {ref.job_id for ref in others} | {child.job_id, parent.job_id} <= {
        line.split("\t")[0] for line in output.splitlines()
    }


def test_a_state_selection_traverses_only_its_states_and_placement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    workspace, name = _new_workspace(tmp_path)
    targets = [_job(tmp_path, workspace, f"scope-{number}", f"target/s{number}") for number in range(3)]
    _job(tmp_path, workspace, "scope-other", "other/s")
    failed = _at(workspace, _job(tmp_path, workspace, "scope-failed", "archive/f"), "failed")
    visited: list[Path] = []
    subdirectories = _kernel._subdirectories

    def recording(directory: Path) -> list[str]:
        visited.append(directory)
        return subdirectories(directory)

    monkeypatch.setattr(_kernel, "_subdirectories", recording)
    blocked = workspace.jobs / "failed"
    blocked.chmod(0)  # an unrelated, unreadable state must not matter
    try:
        argv = ["job", "request", "cancel", "--workspace", name, "--reason", "r", "--state", "ready"]
        assert command([*argv, "--placement", "target"], _context(tmp_path)) == 0
    finally:
        blocked.chmod(0o755)
    assert capsys.readouterr().out.count("\tapplied\t") == 3
    scope = workspace.jobs / "ready" / "target"
    assert visited and all(directory.is_relative_to(scope) for directory in visited)
    assert [v3.find(workspace, ref.job_id).state for ref in targets] == ["cancelled"] * 3
    assert v3.find(workspace, failed.job_id).state == "failed"


def test_an_interrupt_prints_the_ancestors_already_unlocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    workspace, name = _new_workspace(tmp_path)
    grandparent = _job(tmp_path, workspace, "anc-grand", "anc/grand")
    parent = _parented(tmp_path, workspace, grandparent, "anc-parent", "anc/parent")
    child = _parented(tmp_path, workspace, parent, "anc-child", "anc/child")
    apply = removal.apply_requests

    def interrupted(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == grandparent.job_id:
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", interrupted)
    with pytest.raises(KeyboardInterrupt):
        command(["job", "unlock", "--workspace", name, child.job_id], _context(tmp_path))
    lines = capsys.readouterr().out.splitlines()
    assert lines == [f"{child.job_id}\tunlocked", f"{parent.job_id}\tunlocked (ancestor)"]


def test_a_withdrawal_keeps_an_exchange_delete_a_dead_owner_began(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first = _at(workspace, _job(tmp_path, workspace, "xch-a"), "cancelled")
    exchanged = _job(tmp_path, workspace, "xch-b")
    exchange_name = "11111111-2222-4333-8444-555555555555"
    with v3.cli_owner(workspace) as owner:
        assert (
            _kernel.claim_exchange_name(
                workspace, owner, exchange_name, exchanged.job_id, exchanged.placement or PurePosixPath(), nonce="n1"
            )
            is _kernel.ExchangeClaim.WON
        )
        owned = _kernel.claim(workspace, owner, exchanged)
        assert owned is not None
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial")
        doc = dataclasses.replace(doc, origin="exchange", exchange_name=exchange_name)
        exchanged = owned.release(doc, Release("cancelled", 500))
    apply = removal.apply_requests

    def dying_owner_then_interrupt(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == first.job_id:
            # Another owner began this command's delete of the exchange job: it dropped the exchange index and died;
            # recovery returned the job unowned, its delete request still pending. Then this command is interrupted.
            with v3.cli_owner(workspace) as other:
                held = _kernel.claim(workspace, other, v3.find(workspace, exchanged.job_id))
                assert held is not None
                _kernel.drop_exchange_index(other, exchange_name, exchanged.job_id, "n1")
                held.give_back()
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", dying_owner_then_interrupt)
    with pytest.raises(KeyboardInterrupt):
        removal.remove_jobs(workspace, [first, exchanged])
    assert _kernel.exchange_index(workspace, exchange_name) is None
    left = [name for name in os.listdir(workspace.control / "requests") if name.startswith(exchanged.job_id)]
    assert len(left) == 1  # kept: it is what finishes the delete
    request_id = left[0].split(".")[1]
    assert f"request {request_id} for job {exchanged.job_id} kept: a previous owner may have begun applying it" in (
        caplog.text
    )
    monkeypatch.setattr(removal, "apply_requests", apply)
    current = v3.find(workspace, exchanged.job_id)
    (outcome,) = removal.serve_requests(workspace, [(current, workspace.control / "requests" / left[0])])
    assert outcome.status == "applied"  # the next serving finishes it
    assert _kernel.locate(workspace, exchanged.job_id, placement_hint=None, exhaustive=True) is None


def _withdrawing_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Workspace, list[_kernel.JobRef], list[str]]:
    """Two cancelled jobs whose delete is uncertain before it began, so both requests are withdrawn."""

    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"seal-{number}"), "cancelled") for number in range(2)]

    def failing(workspace: Workspace, owner: _kernel.Owner, ref: _kernel.JobRef, **options: object) -> bool:
        raise WorkflowError("cannot read the job")

    monkeypatch.setattr(removal, "serve", failing)
    withdrawn: list[str] = []
    remove = removal._fs.remove_file

    def removing(target: object, *, durable: bool) -> None:
        if not durable and "/requests/" in str(target.path):  # type: ignore[attr-defined]
            withdrawn.append(str(target.path))  # type: ignore[attr-defined]
        remove(target, durable=durable)  # type: ignore[arg-type]

    monkeypatch.setattr(removal._fs, "remove_file", removing)
    return workspace, refs, withdrawn


def test_an_interrupt_after_the_first_unlink_seals_it_before_any_give_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, journal: list[tuple[str, str]], caplog
) -> None:
    workspace, refs, withdrawn = _withdrawing_two(tmp_path, monkeypatch)
    remove = removal._fs.remove_file

    def interrupting(target: object, *, durable: bool) -> None:
        remove(target, durable=durable)  # type: ignore[arg-type]
        if withdrawn:
            raise KeyboardInterrupt  # right after the first withdrawal's unlink

    monkeypatch.setattr(removal._fs, "remove_file", interrupting)
    journal.clear()
    with pytest.raises(KeyboardInterrupt):
        removal.remove_jobs(workspace, refs)
    unlink = next(index for index, (kind, path) in enumerate(journal) if kind == "unlink" and path == withdrawn[0])
    seal = _moves_into(journal, workspace.control / "requests")
    back = _moves_into(journal, refs[0].path.parent)
    assert seal[-1] == unlink + 1 and back[-1] > seal[-1]  # unlink, fsync of requests/, then the give-back
    assert v3.find(workspace, refs[0].job_id).state == "cancelled"
    left = [name for name in os.listdir(workspace.control / "requests") if name.endswith(".json")]
    assert [name.split(".")[0] for name in left] == [refs[1].job_id]  # the second was not reached: kept, reported
    remainder = left[0].split(".")[1]
    assert f"request {remainder} for job {refs[1].job_id}: its withdrawal was cut short" in caplog.text


@pytest.mark.parametrize("failure", ["interrupt", "error"])
def test_a_barrier_that_fails_keeps_the_jobs_held_and_fail_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, journal: list[tuple[str, str]], failure: str
) -> None:
    workspace, refs, withdrawn = _withdrawing_two(tmp_path, monkeypatch)
    requests = str(workspace.control / "requests")
    fsync = os.fsync

    def failing(descriptor: int) -> None:
        if withdrawn and _is(descriptor, requests):
            journal.append(("failed fsync", requests))
            raise KeyboardInterrupt if failure == "interrupt" else OSError(5, "Input/output error")
        fsync(descriptor)

    monkeypatch.setattr(os, "fsync", failing)
    journal.clear()
    with pytest.raises(KeyboardInterrupt if failure == "interrupt" else OSError):
        removal.remove_jobs(workspace, refs)
    assert len(withdrawn) == 2
    # Nothing went back: both jobs stay held for recovery, and the owner fail-stopped with its owner.json.
    assert [v3.find(workspace, ref.job_id).state for ref in refs] == [_kernel.OWNED] * 2
    (owner,) = _kernel.list_owners(workspace)
    assert (workspace.control / "owners" / owner.owner_id / "owner.json").is_file()
    # After the failed barrier no job directory moved (no give-back, and the owner's exit touched nothing).
    barrier = journal.index(("failed fsync", requests))
    assert journal[barrier + 1 :] == []


def _sealed(tmp_path: Path, workspace: Workspace, tag: str) -> _kernel.JobRef:
    """A succeeded job with a seal."""

    ref = _at(workspace, _job(tmp_path, workspace, tag), "succeeded")
    with v3.cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, ref, "seal", "s") is None
    return v3.find(workspace, ref.job_id)


@pytest.mark.parametrize("action", ["unseal", "unlock"])
def test_a_withdrawal_keeps_a_request_a_dead_owner_began_applying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog, action: str
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    if action == "unseal":
        first, second = (_sealed(tmp_path, workspace, f"began-{tag}") for tag in ("a", "b"))
    else:
        first, second = (_job(tmp_path, workspace, f"began-{tag}") for tag in ("a", "b"))
    apply = removal.apply_requests

    def dying_owner_then_interrupt(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == first.job_id:
            # Another owner began applying this command's request for the second job: its run-log record, then (for
            # unseal) the seal document's removal; it died before its state, and recovery returned the job.
            (name,) = [n for n in os.listdir(workspace.control / "requests") if n.startswith(second.job_id)]
            with v3.cli_owner(workspace) as other:
                held = _kernel.claim(workspace, other, v3.find(workspace, second.job_id))
                assert held is not None
                held.append_log("request_applied", detail={"request_id": name.split(".")[1], "action": action})
                if action == "unseal":
                    held.discard_subtree(".httk-job/seal.json")
                held.give_back()
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", dying_owner_then_interrupt)
    with pytest.raises(KeyboardInterrupt), v3.cli_owner(workspace) as owner:
        removal.request_jobs(workspace, owner, [first, second], action, "r", copy=False)
    left = [name for name in os.listdir(workspace.control / "requests") if name.endswith(".json")]
    assert [name.split(".")[0] for name in left] == [second.job_id]  # kept: it is what finishes the application
    request_id = left[0].split(".")[1]
    assert f"request {request_id} for job {second.job_id} kept: an owner began applying it" in caplog.text
    monkeypatch.setattr(removal, "apply_requests", apply)
    (outcome,) = removal.serve_requests(
        workspace, [(v3.find(workspace, second.job_id), workspace.control / "requests" / left[0])]
    )
    assert outcome.status == "applied"  # the next serving finishes it


def test_an_unreadable_run_log_keeps_the_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"log-{number}"), "cancelled") for number in range(2)]

    def failing(workspace: Workspace, owner: _kernel.Owner, ref: _kernel.JobRef, **options: object) -> bool:
        raise WorkflowError("cannot read the job")

    def unreadable(*args: object, **kwargs: object) -> object:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(removal, "serve", failing)
    monkeypatch.setattr(removal._fs, "read_tail", unreadable)
    removal.remove_jobs(workspace, refs)
    assert _requests_left(workspace) == 2
    assert caplog.text.count("kept: the job's run log cannot be read") == 2


def test_a_failed_withdrawal_reports_the_outcomes_known_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _name = _new_workspace(tmp_path)
    refs = [_at(workspace, _job(tmp_path, workspace, f"rep-{number}"), "cancelled") for number in range(2)]
    serve, fsync = removal.serve, os.fsync
    requests = str(workspace.control / "requests")
    withdrawing: list[bool] = []

    def failing(workspace: Workspace, owner: _kernel.Owner, ref: _kernel.JobRef, **options: object) -> bool:
        if ref.job_id == refs[1].job_id:
            withdrawing.append(True)
            raise WorkflowError("cannot read the job")
        return serve(workspace, owner, ref, **options)  # type: ignore[arg-type]

    def barrier(descriptor: int) -> None:
        if withdrawing and _is(descriptor, requests):
            raise OSError(5, "Input/output error")  # the normal end-of-chunk withdrawal's barrier
        fsync(descriptor)

    monkeypatch.setattr(removal, "serve", failing)
    monkeypatch.setattr(os, "fsync", barrier)
    reported: list[list[str]] = []
    with pytest.raises(OSError, match="Input/output"), v3.cli_owner(workspace) as owner:
        removal.request_jobs(
            workspace,
            owner,
            refs,
            "delete",
            "r",
            report=lambda outcomes: reported.append([f"{o.job_id} {o.status}" for o in outcomes]),
        )
    assert reported == [[f"{refs[0].job_id} applied", f"{refs[1].job_id} uncertain"]]


def test_a_withdrawal_keeps_a_cancel_of_a_job_returned_with_a_running_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    first, second = (_job(tmp_path, workspace, f"run-{tag}") for tag in ("a", "b"))
    apply = removal.apply_requests

    def dead_manager_then_interrupt(owned: _kernel.OwnedJob, *args: object, **kwargs: object) -> object:
        if owned.job_id == first.job_id:
            # A manager ran the second job's attempt, saw this command's cancel and signalled the attempt (a cancel is
            # logged only at the attempt's commit), then died; recovery returned the job, its attempt still running.
            with v3.cli_owner(workspace) as manager:
                held = _kernel.claim(workspace, manager, v3.find(workspace, second.job_id))
                assert held is not None
                doc = (
                    StateDoc.empty(held.job_id)
                    .next_activation("start", "initial")
                    .next_attempt("launch", unclean=False)
                )
                assert doc.attempt is not None
                held.write_state(doc.with_phase("running", str(doc.attempt["id"])))
                held.give_back()
            raise KeyboardInterrupt
        return apply(owned, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(removal, "apply_requests", dead_manager_then_interrupt)
    with pytest.raises(KeyboardInterrupt), v3.cli_owner(workspace) as owner:
        removal.request_jobs(workspace, owner, [first, second], "cancel", "r")
    left = [name for name in os.listdir(workspace.control / "requests") if name.endswith(".json")]
    assert [name.split(".")[0] for name in left] == [second.job_id]  # kept: the cancel may already be in effect
    request_id = left[0].split(".")[1]
    assert f"request {request_id} for job {second.job_id} kept: the job had a running attempt" in caplog.text


def test_an_interrupt_in_the_withdrawal_scan_names_every_request_left(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    workspace, refs, _withdrawn = _withdrawing_two(tmp_path, monkeypatch)
    requests = workspace.control / "requests"
    lstat = removal._fs.lstat
    scanning: list[bool] = []

    def interrupted(target: object) -> object:
        if target.path.parent == requests:  # type: ignore[attr-defined]
            scanning.append(True)
            raise KeyboardInterrupt  # the withdrawal's first look at a request file
        return lstat(target)  # type: ignore[arg-type]

    monkeypatch.setattr(removal._fs, "lstat", interrupted)
    with pytest.raises(KeyboardInterrupt):
        removal.remove_jobs(workspace, refs)
    assert scanning
    left = sorted(name for name in os.listdir(requests) if name.endswith(".json"))
    assert len(left) == 2
    for name in left:
        job_id, request_id = name.split(".")[:2]
        assert f"request {request_id} for job {job_id}: its withdrawal was cut short (KeyboardInterrupt)" in (
            caplog.text
        )
