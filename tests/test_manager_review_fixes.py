"""Regression tests of the manager, commit, request and removal layers found by the phase-F review."""

import json
import logging
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _children, _kernel, _requests, _store, manager, removal
from httk.workflow._job import JobDefinition
from httk.workflow._state import StateDoc
from httk.workflow.errors import FormatError

pytestmark = pytest.mark.slow

_UNREADABLE = pytest.mark.skipif(os.geteuid() == 0, reason="root reads files whatever their mode")


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "package")


def _until(condition: Callable[[], object], seconds: float = 60.0) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(0.02)


def _request_id(path: Path) -> str:
    return path.name.split(".")[1]


def _post(
    ws: Workspace, ref: _kernel.JobRef, action: str, *, priority: int | None = None, request_id: str | None = None
) -> str:
    assert ref.placement is not None
    path = _requests.post(
        ws,
        action=action,
        job_id=ref.job_id,
        placement=ref.placement,
        operator="t",
        reason="r",
        priority=priority,
        request_id=request_id,
    )
    return _request_id(path)


def _pending(ws: Workspace) -> list[str]:
    return sorted(path.name for path in (ws.control / "requests").glob("*.json"))


# -- B1: an unreadable trusted file is job damage -----------------------------------------------------------------


@_UNREADABLE
@pytest.mark.parametrize("name", ["job.json", "state.json"])
def test_an_unreadable_trusted_file_is_damage_and_spares_both_managers(
    ws: Workspace, installed: _store.Installed, name: str, caplog: pytest.LogCaptureFixture
) -> None:
    hostile = h.submit(ws, installed, {"start": "pause"}, placement="project/hostile")
    h.run(ws)
    paused = h.find(ws, hostile.job_id)
    # What a stray process of the job leaves behind: the owner can no longer read its own file.
    (paused.path / name).chmod(0)
    continued = _post(ws, paused, "continue")
    healthy = h.submit(ws, installed, {"start": "succeed"}, placement="project/healthy")

    with (
        caplog.at_level(logging.ERROR, logger="httk.workflow"),
        TaskManager(ws, heartbeat_interval=0.01) as first,
        TaskManager(ws, heartbeat_interval=0.01) as second,
    ):
        for _ in range(4):
            first.tick()
            second.tick()
        second.run_until_idle(timeout=60)
        first.run_until_idle(timeout=60)
        assert not first.owner.owned() and not second.owner.owned()

    assert h.find(ws, healthy.job_id).state == "succeeded"
    if name == "job.json":
        (entry,) = (ws.control / "quarantine").iterdir()
        assert "job.json is damaged" in json.loads((entry / "reason.json").read_text(encoding="utf-8"))["reason"]
        assert _kernel.locate(ws, hostile.job_id, placement_hint=None, exhaustive=True) is None
        return
    failed = h.find(ws, hostile.job_id)
    doc = h.state_of(failed)
    assert failed.state == "failed" and doc.failure is not None and doc.failure["code"] == "protocol_error"
    # B6: the pending request the damage made undecidable is dropped on the record, not silently.
    dropped = [entry for entry in doc.history_tail if entry["event"] == "request_dropped"]
    assert [(entry["request_id"], entry["action"]) for entry in dropped] == [(continued, "continue")]
    assert not _pending(ws)
    (record,) = [item for item in caplog.records if getattr(item, "event", None) == "job_damaged"]
    assert record.__dict__["dropped_requests"] == [{"request_id": continued, "action": "continue"}]
    assert f"dropped continue request {continued}" in record.getMessage()


@_UNREADABLE
@pytest.mark.parametrize("name", ["job.json", "state.json"])
def test_a_cli_request_on_an_unreadable_job_quarantines_it(
    ws: Workspace, installed: _store.Installed, name: str
) -> None:
    hostile = h.submit(ws, installed, {"start": "pause"}, placement="project/hostile")
    h.run(ws)
    paused = h.find(ws, hostile.job_id)
    (paused.path / name).chmod(0)

    (outcome,) = removal.remove_jobs(ws, [paused]).outcomes
    assert not outcome.removed and outcome.reason is not None and outcome.reason.startswith("quarantined")
    (entry,) = (ws.control / "quarantine").iterdir()
    assert (entry / "entry" / name).exists()
    assert _kernel.locate(ws, hostile.job_id, placement_hint=None, exhaustive=True) is None
    # The CLI owner returned everything and closed.
    assert not list((ws.jobs / "owned").glob("*/*"))


# -- B2: a failed transaction is not re-applied -------------------------------------------------------------------


def test_a_conflicting_transaction_fails_the_job_once_and_requests_still_apply(
    ws: Workspace, installed: _store.Installed
) -> None:
    submitted = h.submit(
        ws,
        installed,
        {"start": "succeed"},
        parameters={"put": {"start": {"x/y": "staged"}}},
        members={"x": "a file where the transaction stages a directory"},
        retry_policy={"maximum_attempts_per_activation": 3},
    )
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    doc = h.state_of(failed)
    assert failed.state == "failed" and doc.failure is not None and doc.failure["code"] == "data_conflict"
    assert not list(failed.path.glob("attempts/*/txn")), "the unapplied staging must not reach a later commit"
    history = len(doc.failure_history)

    _post(ws, failed, "set_priority", priority=7)
    h.run(ws)
    again = h.find(ws, submitted.job_id)
    assert (again.state, again.priority) == ("failed", 7)
    assert len(h.state_of(again).failure_history) == history

    # The operator repairs the payload; continue relaunches the job, whose new transaction applies.
    (again.path / "x").unlink()
    _post(ws, again, "continue")
    h.run(ws)
    done = h.find(ws, submitted.job_id)
    assert done.state == "succeeded"
    assert (done.path / "x" / "y").read_text(encoding="utf-8") == "staged"
    assert h.events(done).count("launched") == 2


# -- B3: job.json is pinned by its digest ---------------------------------------------------------------------------


def test_an_attempt_that_rewrites_its_job_definition_fails_with_protocol_error(
    ws: Workspace, installed: _store.Installed
) -> None:
    mapping = h.job_mapping(installed, {"start": "succeed"}, placement="project/rewriter")
    rewritten = json.dumps(mapping)  # a valid definition, but other bytes
    mapping["parameters"] = {**mapping["parameters"], "files": {"start": {"../job.json": rewritten}}}  # type: ignore[dict-item]
    submitted = h.submit_mapping(ws, mapping)
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    doc = h.state_of(failed)
    assert failed.state == "failed" and doc.failure is not None
    assert (doc.failure["code"], doc.failure["message"]) == ("protocol_error", "the attempt rewrote job.json")


def test_a_rewritten_job_definition_fails_the_job_at_its_next_claim(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "pause"}, placement="project/pinned")
    h.run(ws)
    paused = h.find(ws, submitted.job_id)
    assert h.state_of(paused).job_digest == JobDefinition.from_path(paused.path / "job.json").digest
    text = (paused.path / "job.json").read_text(encoding="utf-8")
    (paused.path / "job.json").write_text(json.dumps(json.loads(text), indent=1), encoding="utf-8")
    _post(ws, paused, "set_priority", priority=9)
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    doc = h.state_of(failed)
    assert failed.state == "failed" and doc.failure is not None and doc.failure["code"] == "protocol_error"
    assert "changed since the job's first activation" in str(doc.failure["message"])
    # The request still applied (from failed), so nothing keeps re-failing the job.
    assert failed.priority == 9 and not _pending(ws)


def test_a_state_document_without_a_job_digest_still_reads() -> None:
    mapping = StateDoc.empty(str(uuid.uuid4())).as_mapping()
    del mapping["job_digest"]
    assert StateDoc.from_mapping(mapping).job_digest is None
    with pytest.raises(FormatError):
        StateDoc.from_mapping({**mapping, "job_digest": "not-a-digest"})


# -- B4, B7: request order and the waiting parent ---------------------------------------------------------------------


def test_a_waiting_parent_is_not_paused() -> None:
    job = JobDefinition.from_mapping(h.job_mapping(("local:demo", "demo"), {"start": "succeed"}))
    doc = StateDoc.empty(job.id).next_activation("start", "initial")
    request_id = str(uuid.uuid4())
    document = {
        "format": _requests.REQUEST_FORMAT,
        "format_version": _requests.REQUEST_FORMAT_VERSION,
        "request_id": request_id,
        "job_id": job.id,
        "placement": "project/0",
        "action": "pause",
        "operator": "t",
        "reason": "r",
        "created_at": "2026-10-10T00:00:00+00:00",
    }
    request = _requests.Request(request_id, job.id, job.placement, "pause", document, Path("x"))
    new, effect = _requests.apply(job, doc, "waiting", 500, request)
    assert isinstance(effect, _requests.Drop) and "waiting on its children" in effect.reason
    assert new.applied_requests == (request.request_id,)


def test_requests_apply_in_the_order_they_were_posted(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"}, placement="project/ordered")
    # The continue sorts first by id, but the pause was posted first: pause, then continue, must win.
    _post(ws, submitted, "pause", request_id="ffffffff-ffff-4fff-bfff-ffffffffffff")
    time.sleep(0.01)
    _post(ws, submitted, "continue", request_id="00000000-0000-4000-8000-000000000000")
    with h.cli_owner(ws) as owner:
        assert removal.serve(ws, owner, h.find(ws, submitted.job_id))
        assert h.find(ws, submitted.job_id).state == "paused"
        assert removal.serve(ws, owner, h.find(ws, submitted.job_id))
    assert h.find(ws, submitted.job_id).state == "ready"
    assert not _pending(ws)


# -- B5: a boundary release records the commit's own request ---------------------------------------------------------


def test_a_boundary_release_records_the_commits_own_cancel(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "sleep"}, placement="project/sleeper")
    with TaskManager(ws, heartbeat_interval=0.01, cancel_grace_seconds=1.0) as task_manager:
        _until(lambda: task_manager.tick() is not None and any((ws.jobs / "owned").glob("*/*/run/sleeping")))
        priority = _post(ws, submitted, "set_priority", priority=7)
        time.sleep(0.01)
        cancel = _post(ws, submitted, "cancel")
        _until(lambda: task_manager.tick() is not None and not task_manager.running_attempts)
        task_manager.run_until_idle(timeout=30)
    done = h.find(ws, submitted.job_id)
    assert (done.state, done.priority) == ("cancelled", 7)
    assert {priority, cancel} <= set(h.state_of(done).applied_requests)
    assert not _pending(ws)


# -- B8: a job whose commit keeps failing is given back -----------------------------------------------------------


def test_a_commit_that_keeps_failing_is_given_back_unchanged(ws: Workspace, installed: _store.Installed) -> None:
    spawn = {"children": [{"label": "c", "script": {"start": "succeed"}, "placement": "blocked/c0"}]}
    parent = h.submit(
        ws, installed, {"start": "spawn", "gather": "succeed"}, placement="project/parent", parameters={"spawn": spawn}
    )
    # A non-directory where the child's placement must go: every publication of the child fails.
    (ws.jobs / "ready").mkdir(parents=True, exist_ok=True)
    (ws.jobs / "ready" / "blocked").write_text("in the way", encoding="utf-8")
    key = parent.job_key
    with TaskManager(ws, heartbeat_interval=0.01) as task_manager:
        _until(
            lambda: task_manager.tick() is not None and task_manager._failures.get(key, 0) > manager._GIVE_BACK_AFTER
        )
        assert any("given back" in text for text in task_manager._reported.values())
        assert h.find(ws, parent.job_id).state == "ready"
        assert h.state_of(h.find(ws, parent.job_id)).commit is not None, "the intent is kept for the next claimant"
        (ws.jobs / "ready" / "blocked").unlink()
        task_manager.run_until_idle(timeout=60)
        assert key not in task_manager._failures
    assert h.find(ws, parent.job_id).state == "succeeded"


# -- B9: runner-chosen child ids are checked -------------------------------------------------------------------------


def test_a_child_id_that_names_an_existing_job_is_refused(ws: Workspace, installed: _store.Installed) -> None:
    # The ids are looked up at the placements involved: the children's and the parent's.
    existing = h.submit(ws, installed, {"start": "succeed"}, placement="project/parent/c0")
    submitted = h.submit(ws, installed, {"start": "succeed"}, placement="project/parent")
    with h.cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, submitted)
        assert owned is not None
        parent = JobDefinition.from_path(owned.path / "job.json")
        try:
            for child_id, refused in ((existing.job_id, True), (parent.id, True), (str(uuid.uuid4()), False)):
                outcome = owned.path / "attempts" / str(uuid.uuid4()) / "outcome.ready"
                key = f"c--{child_id}"
                child = h.job_mapping(installed, {"start": "succeed"}, tag="c", placement="project/parent/c0")
                child["id"] = child_id
                child["parent"] = {
                    "workspace_id": ws.workspace_id,
                    "job_id": parent.id,
                    "job_key": parent.job_key,
                    "placement": "project/parent",
                    "activation_id": str(uuid.uuid4()),
                    "spawn_id": str(uuid.uuid4()),
                }
                (outcome / "children" / "jobs" / key).mkdir(parents=True)
                (outcome / "children" / "jobs" / key / "job.json").write_text(json.dumps(child), encoding="utf-8")
                entry = {"job_key": key, "label": "c", "placement": "project/parent/c0"}
                spawn = {"format": _children.SPAWN_FORMAT, "format_version": 2, "children": [entry]}
                (outcome / "children" / "spawn.json").write_text(json.dumps(spawn), encoding="utf-8")
                arguments = (owned, parent, StateDoc.empty(parent.id), outcome)
                if refused:
                    with pytest.raises(FormatError, match="is already the job"):
                        _children.validate_children(*arguments, workspace_id=ws.workspace_id)
                else:
                    (plan,) = _children.validate_children(*arguments, workspace_id=ws.workspace_id)
                    assert plan.job_id == child_id
        finally:
            owned.give_back()


# -- C7, replay: nothing is posted for a missing job, and a replayed commit logs once ---------------------------------


def test_no_request_is_posted_for_a_job_that_is_not_found(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"}, placement="project/gone")
    stale = h.find(ws, submitted.job_id)
    with h.cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, stale)
        assert owned is not None
        owned.discard()
        assert removal.request_now(ws, owner, stale, "delete", "test") == "the job was not found"
    assert not _pending(ws)
