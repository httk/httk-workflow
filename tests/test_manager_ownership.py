"""Which jobs and requests a manager accepts: provenance, real directories, and decisions from the owned copy."""

import json
import os
import uuid
from pathlib import Path

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _kernel, _manager_scheduling, _requests, _store
from test_crash_injection import _log

pytestmark = pytest.mark.slow


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "demo")


def test_a_job_without_this_users_provenance_is_never_claimed_or_served(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    legitimate = h.submit(ws, installed, {"start": "succeed"}, placement="project/legitimate")
    forged = h.submit(ws, installed, {"start": "succeed"}, placement="project/forged")
    _requests.post(ws, action="pause", job_id=forged.job_id, placement="project/forged", operator="o", reason="r")
    real = os.lstat

    def lstat(path: str | os.PathLike[str], *arguments: object, **keywords: object) -> os.stat_result:
        result = real(path, *arguments, **keywords)  # type: ignore[arg-type]
        if Path(path) in (forged.path, forged.path / "job.json"):
            # Planted by another user: the provenance check (not an authentication) refuses it.
            fields = list(result[:10])
            fields[4] = result.st_uid + 1
            return os.stat_result(fields)
        return result

    monkeypatch.setattr(os, "lstat", lstat)
    h.run(ws)
    monkeypatch.undo()

    assert h.find(ws, legitimate.job_id).state == "succeeded"
    untouched = h.find(ws, forged.job_id)
    assert untouched.path == forged.path and sorted(os.listdir(untouched.path)) == ["job.json"]
    # Its request was not served either.
    assert len(list((ws.control / "requests").iterdir())) == 1


def test_a_symlinked_job_directory_is_never_claimed(ws: Workspace, installed: _store.Installed, tmp_path: Path) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"})
    saved = tmp_path / "saved"
    submitted.path.rename(saved)
    submitted.path.symlink_to(saved, target_is_directory=True)
    h.run(ws)
    # Neither the link nor its target was claimed, written or moved.
    assert submitted.path.is_symlink() and os.readlink(submitted.path) == str(saved)
    assert sorted(os.listdir(saved)) == ["job.json"]
    assert not list((ws.jobs / "owned").glob("*/*"))
    assert not [ref for state in _kernel.UNOWNED_STATES if state != "ready" for ref in _kernel.list_jobs(ws, state)]


def test_a_job_definition_changed_after_the_eligibility_read_decides_from_the_owned_copy(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The read before a claim is only a hint: eligibility is decided again from the claimed directory."""

    submitted = h.submit(ws, installed, {"start": "succeed"})
    real = _manager_scheduling.read_ready
    swapped: list[str] = []

    def read_then_swap(manager: TaskManager, ref: _kernel.JobRef) -> object:
        result = real(manager, ref)
        if not swapped:
            # Between the eligibility read and the claim, the definition starts asking for another pool.
            document = json.loads((ref.path / "job.json").read_text(encoding="utf-8"))
            document["claim"]["pool"] = "elsewhere"
            (ref.path / "job.json").write_text(json.dumps(document), encoding="utf-8")
            swapped.append(ref.job_key)
        return result

    monkeypatch.setattr(_manager_scheduling, "read_ready", read_then_swap)
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.tick()
        assert swapped and manager.running_attempts == 0
    returned = h.find(ws, submitted.job_id)
    assert returned.state == "ready"
    # Claimed, found ineligible from its own job.json, and returned without an attempt.
    assert h.events(returned) == ["claimed", "released"]
    assert not (returned.path / "attempts").exists() and not (returned.path / "run").exists()
    assert h.state_of(returned).attempt is None


def test_a_request_whose_job_id_disagrees_with_its_name_is_never_applied(
    ws: Workspace, installed: _store.Installed, caplog: pytest.LogCaptureFixture
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"}, pool="nobody")
    request_id = str(uuid.uuid4())
    document = {
        "format": _requests.REQUEST_FORMAT,
        "format_version": _requests.REQUEST_FORMAT_VERSION,
        "request_id": request_id,
        "job_id": str(uuid.uuid4()),
        "placement": "project/0",
        "action": "pause",
        "operator": "tester",
        "reason": "test",
        "created_at": "2026-10-09T00:00:00+00:00",
    }
    # Filed under the job's id, but naming another job: a misnamed request is malformed, whatever it says.
    misnamed = ws.control / "requests" / f"{submitted.job_id}.{request_id}.json"
    misnamed.parent.mkdir(parents=True, exist_ok=True)
    misnamed.write_text(json.dumps(document), encoding="utf-8")
    with caplog.at_level("ERROR", logger="httk.workflow"):
        h.run(ws)
    current = h.find(ws, submitted.job_id)
    assert current.path == submitted.path and current.state == "ready"
    assert not (current.path / "logs").exists()
    # It stays for collection to quarantine; nothing applied it.
    assert misnamed.exists()
    assert any("ignoring malformed request" in record.getMessage() for record in caplog.records)


def test_a_request_for_a_job_is_applied_from_the_owned_copy_and_recorded(
    ws: Workspace, installed: _store.Installed
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"}, pool="nobody")
    request = _requests.post(
        ws,
        action="set_priority",
        job_id=submitted.job_id,
        placement="project/0",
        operator="o",
        reason="r",
        priority=25,
    )
    h.run(ws)
    current = h.find(ws, submitted.job_id)
    assert current.state == "ready" and current.priority == 25
    assert not request.exists()
    (applied,) = [line for line in _log(current) if line["event"] == "request_applied"]
    assert applied["detail"] == {"request_id": request.name.split(".")[1], "action": "set_priority"}
