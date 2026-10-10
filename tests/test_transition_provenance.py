"""Provenance across transitions: ``state.json`` carries a job's origin through every move, and every move is logged.

Each owner records what it did in the job's own run log (``logs/runlog.jsonl``)
and in the bounded ``history_tail`` of ``state.json``. The exchange origin of a
job survives every manager transition and is inherited by its children. A
damaged ``state.json`` is never guessed at: the job fails, and the requests it
had already applied stay applied.
"""

import json
import time
import uuid
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _kernel, _requests, _store
from httk.workflow._state import StateDoc, encode_state
from test_crash_injection import _all_jobs, _log

pytestmark = pytest.mark.slow


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "demo")


_SPAWN = {"children": [{"label": "only", "script": {"start": "succeed"}}]}


def _imported(ws: Workspace, installed: _store.Installed, exchange_name: str) -> _kernel.JobRef:
    """Submit a job whose state.json carries the exchange origin, as an adopter publishes one."""

    mapping = h.job_mapping(installed, {"start": "spawn", "gather": "succeed"}, parameters={"spawn": _SPAWN})
    state = StateDoc.empty(str(mapping["id"])).updated(origin="exchange", exchange_name=exchange_name)
    return h.submit_mapping(ws, mapping, members={"state.json": encode_state(state).decode("utf-8")})


def test_provenance_survives_every_manager_transition(ws: Workspace, installed: _store.Installed) -> None:
    exchange_name = str(uuid.uuid4())
    imported = _imported(ws, installed, exchange_name)
    h.run(ws)

    finished = h.find(ws, imported.job_id)
    assert finished.state == "succeeded"
    doc = h.state_of(finished)
    # Claimed, waiting, claimed for the join, ready, claimed again and succeeded: the origin survived all of it.
    assert (doc.origin, doc.exchange_name) == ("exchange", exchange_name)
    assert [(entry["from"], entry["to"]) for entry in doc.history_tail if entry["event"] == "released"] == [
        ("ready", "waiting"),
        ("waiting", "ready"),
        ("ready", "succeeded"),
    ]
    # Its child inherited the origin when it was published.
    (child,) = [ref for ref in _all_jobs(ws) if ref.job_id != imported.job_id]
    child_doc = h.state_of(child)
    assert (child_doc.origin, child_doc.exchange_name) == ("exchange", exchange_name)


def test_a_job_without_provenance_is_local(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    h.run(ws)
    doc = h.state_of(h.find(ws, submitted.job_id))
    assert (doc.origin, doc.exchange_name) == ("local", None)


def test_every_transition_is_in_the_run_log_and_the_history_tail(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "advance:finish", "finish": "succeed"})
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60)
        owner = manager.manager_id
    done = h.find(ws, submitted.job_id)
    log = _log(done)
    # Every line names its owner and time; attempt lines name their attempt, activation and step.
    assert all(line["owner_id"] == owner and isinstance(line["at"], str) for line in log)
    started = [line for line in log if line["event"] == "attempt_started"]
    assert [line["step"] for line in started] == ["start", "finish"]
    assert len({line["attempt_id"] for line in started}) == 2 and len({line["activation_id"] for line in started}) == 2
    committed = [line for line in log if line["event"] == "committed"]
    assert [(line["to"], line["detail"]) for line in committed] == [("ready", "advance"), ("succeeded", "succeed")]
    assert [line["attempt_id"] for line in committed] == [line["attempt_id"] for line in started]
    released = [line for line in log if line["event"] == "released"]
    assert [(line["from"], line["to"]) for line in released] == [("ready", "ready"), ("ready", "succeeded")]
    # The bounded history in state.json tells the same story of moves, with the owner of each.
    history = [entry for entry in h.state_of(done).history_tail if entry["event"] == "released"]
    assert [(entry["from"], entry["to"], entry["owner_id"]) for entry in history] == [
        ("ready", "ready", owner),
        ("ready", "succeeded", owner),
    ]


def test_a_damaged_state_json_fails_the_job_and_keeps_its_applied_requests_skipped(
    ws: Workspace, installed: _store.Installed
) -> None:
    request_id = str(uuid.uuid4())
    mapping = h.job_mapping(installed, {"start": "succeed"}, pool="nobody")
    # The last owner applied a continue request and crashed before it deleted the file; then the document was
    # damaged. Only its applied_requests member still reads.
    damaged = {"format": "garbage", "job_id": mapping["id"], "applied_requests": [request_id]}
    submitted = h.submit_mapping(ws, mapping, members={"state.json": json.dumps(damaged)})
    _requests.post(
        ws,
        action="continue",
        job_id=submitted.job_id,
        placement="project/0",
        operator="o",
        reason="r",
        request_id=request_id,
    )
    h.run(ws)

    failed = h.find(ws, submitted.job_id)
    assert failed.state == "failed"
    doc = h.state_of(failed)
    assert doc.failure is not None and doc.failure["code"] == "protocol_error"
    # Nothing was decided from the damaged document, and the request it had applied never applied again.
    assert not [line for line in _log(failed) if line["event"] == "request_applied"]
    assert not list((ws.control / "requests").iterdir())


def test_a_claim_loser_learns_it_lost_at_once(tmp_path: Path) -> None:
    slow = Workspace.initialize(tmp_path / "slow", durable=False, policy={"visibility_deadline_seconds": 30.0})
    slow_installed = h.install(slow, tmp_path / "slow-demo")
    ready = h.submit(slow, slow_installed, {"start": "succeed"})
    with h.cli_owner(slow) as winner, h.cli_owner(slow) as loser:
        won = _kernel.claim(slow, winner, ready)
        assert won is not None
        # The loser still holds the ready reference it read before the winner moved the job.
        started = time.monotonic()
        assert _kernel.claim(slow, loser, ready) is None
        assert time.monotonic() - started < 10.0
        assert not loser.owned()
        won.give_back()
