"""Immediate serving of published operator requests."""

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from httk.core.identity import ensure_identity_key, write_identity_config

import v3_helpers as v3
from httk.workflow import Workspace, _kernel, _store, seals
from httk.workflow import _requests as request_files
from httk.workflow import removal as removal_module
from httk.workflow._state import Release, StateDoc, encode_state, read_state_unowned
from httk.workflow.errors import WorkflowError
from httk.workflow.removal import serve_requests
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command
from test_cli_job_request import _context, _job, _new_workspace, _request_args, _requests


@pytest.fixture(autouse=True)
def _identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    write_identity_config(
        {"identities": {"tester": {"name": "Test User", "email": "tester@example.test"}}, "default_identity": "tester"}
    )
    ensure_identity_key("tester")


def _applied_state(workspace: Workspace, job_id: str) -> list[dict[str, object]]:
    ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
    assert ref is not None
    doc, _damage = read_state_unowned(ref.path / "state.json")
    assert doc is not None
    return [dict(entry) for entry in doc.history_tail if entry.get("event") == "request_applied"]


def test_idle_pause_is_applied_without_a_manager_even_when_waiting(tmp_path: Path, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "immediate-pause")

    assert command(_request_args(name, ref.job_id) + ["--wait", "--timeout", "0"], _context(tmp_path)) == 0
    output = capsys.readouterr().out
    assert f"{ref.job_id}\t" in output and "\tapplied\t" in output and "state=paused" in output
    assert _requests(workspace) == []
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "paused"


def test_cancel_continue_override_and_detach_apply_without_a_manager(tmp_path: Path, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "control-actions")
    assert command(_request_args(name, ref.job_id), _context(tmp_path)) == 0
    capsys.readouterr()
    assert command(_request_args(name, ref.job_id, action="continue"), _context(tmp_path)) == 0
    capsys.readouterr()
    assert command(_request_args(name, ref.job_id), _context(tmp_path)) == 0
    capsys.readouterr()
    assert (
        command(
            _request_args(name, ref.job_id, action="override_step") + ["--override-step", "start"], _context(tmp_path)
        )
        == 0
    )
    capsys.readouterr()
    assert command(_request_args(name, ref.job_id, action="cancel"), _context(tmp_path)) == 0
    capsys.readouterr()
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "cancelled"

    parent = _job(tmp_path, workspace, "parent-for-detach")
    installed = _store.lookup(workspace, "demo")
    assert installed is not None
    child_mapping = v3.job_mapping(installed, {"start": "succeed"}, tag="child-for-detach")
    child_mapping["parent"] = {
        "workspace_id": workspace.workspace_id,
        "job_id": parent.job_id,
        "job_key": parent.job_key,
        "placement": str(parent.placement),
        "activation_id": str(uuid4()),
        "spawn_id": str(uuid4()),
    }
    child = v3.submit_mapping(workspace, child_mapping)
    assert command(_request_args(name, child.job_id, action="detach"), _context(tmp_path)) == 0
    output = capsys.readouterr().out
    assert "\tapplied\t" in output
    detached = _kernel.locate(workspace, child.job_id, placement_hint=child.placement, exhaustive=True)
    assert detached is not None
    doc, _damage = read_state_unowned(detached.path / "state.json")
    assert doc is not None and doc.detached is not None


def test_immediate_batch_reports_applied_and_refused_per_job(tmp_path: Path, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    finished = _job(tmp_path, workspace, "batch-finished")
    from httk.workflow.manager import TaskManager

    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=60)
    ready = _job(tmp_path, workspace, "batch-ready")

    assert command(_request_args(name, ready.job_id, finished.job_id), _context(tmp_path)) == 1
    output = capsys.readouterr().out
    assert f"{ready.job_id}\t" in output and "\tapplied\t" in output
    assert f"{finished.job_id}\t" in output and "\trefused\tthe job is already succeeded" in output
    assert not _requests(workspace)


def test_preceding_request_is_applied_before_the_new_request(tmp_path: Path, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "request-order")
    earlier = request_files.post(
        workspace,
        action="set_priority",
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="tester",
        reason="older request",
        priority=77,
        created_at="2026-01-01T00:00:00Z",
    )

    assert command(_request_args(name, ref.job_id), _context(tmp_path)) == 0
    capsys.readouterr()
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "paused" and current.priority == 77
    doc, _damage = read_state_unowned(current.path / "state.json")
    assert doc is not None
    applied = [entry["request_id"] for entry in doc.history_tail if entry.get("event") == "request_applied"]
    assert applied.index(earlier.name.split(".")[1]) < len(applied) - 1
    assert not _requests(workspace)


def test_same_timestamp_requests_use_request_id_order(tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "request-id-order")
    created_at = "2026-01-01T00:00:00Z"
    earlier = request_files.post(
        workspace,
        action="set_priority",
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="tester",
        reason="same timestamp, earlier id",
        priority=77,
        request_id="00000000-0000-4000-8000-000000000001",
        created_at=created_at,
    )
    monkeypatch.setattr(job_cli.uuid, "uuid4", lambda: UUID("00000000-0000-4000-8000-000000000002"))
    monkeypatch.setattr(job_cli, "utc_now", lambda: created_at)

    assert command(_request_args(name, ref.job_id), _context(tmp_path)) == 0
    capsys.readouterr()
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "paused" and current.priority == 77
    assert earlier.name.split(".")[1] in {entry.get("request_id") for entry in _applied_state(workspace, ref.job_id)}
    assert not _requests(workspace)


def test_live_owner_leaves_request_queued_and_keeps_its_file(tmp_path: Path, capsys) -> None:
    workspace, name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "owned-request")

    with _kernel.register_owner(workspace, kind="manager", label="live", allocation=None, advertised={}) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        assert command(_request_args(name, ref.job_id), _context(tmp_path)) == 0
        output = capsys.readouterr().out
        assert "\tqueued\tanother owner holds the job" in output
        assert len(_requests(workspace)) == 1
        owned.give_back()


def test_lost_claim_is_queued_and_a_later_arrival_is_outside_the_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "claim-race")
    published = request_files.post(
        workspace, action="pause", job_id=ref.job_id, placement=ref.placement or "", operator="tester", reason="r"
    )
    real_claim = _kernel.claim

    def lose_claim(*args, **kwargs):
        return None

    monkeypatch.setattr(_kernel, "claim", lose_claim)
    (outcome,) = serve_requests(workspace, [(ref, published)])
    assert outcome.status == "queued" and _requests(workspace)
    monkeypatch.setattr(_kernel, "claim", real_claim)

    def claim_then_publish(*args, **kwargs):
        owned = real_claim(*args, **kwargs)
        request_files.post(
            workspace,
            action="cancel",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="tester",
            reason="arrived after snapshot",
        )
        return owned

    monkeypatch.setattr(_kernel, "claim", claim_then_publish)
    (outcome,) = serve_requests(workspace, [(ref, published)])
    assert outcome.status == "applied"
    assert len(_requests(workspace)) == 1
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "paused"


def test_manager_applied_request_is_recovered_from_history_before_and_after_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    before_snapshot = _job(tmp_path, workspace, "manager-before-snapshot")
    first = request_files.post(
        workspace,
        action="pause",
        job_id=before_snapshot.job_id,
        placement=before_snapshot.placement or "",
        operator="tester",
        reason="r",
    )
    from httk.workflow.manager import TaskManager

    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=60)
    (outcome,) = serve_requests(workspace, [(before_snapshot, first)])
    assert outcome.status == "applied" and outcome.explanation == "pause applied"

    after_snapshot = _job(tmp_path, workspace, "manager-after-snapshot")
    second = request_files.post(
        workspace,
        action="pause",
        job_id=after_snapshot.job_id,
        placement=after_snapshot.placement or "",
        operator="tester",
        reason="r",
    )
    real_serve = removal_module.serve

    def manager_wins(*args, **kwargs):
        with TaskManager(workspace) as manager:
            manager.run_until_idle(timeout=60)
        return True

    monkeypatch.setattr(removal_module, "serve", manager_wins)
    (outcome,) = serve_requests(workspace, [(after_snapshot, second)])
    assert outcome.status == "applied" and outcome.explanation == "pause applied"
    monkeypatch.setattr(removal_module, "serve", real_serve)


@pytest.mark.parametrize("interrupted", ["phase", "commit", "pending_release"])
def test_unfinished_state_stays_queued_for_reconciliation(tmp_path: Path, interrupted: str) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, f"unfinished-{interrupted}")
    with _kernel.register_owner(workspace, kind="cli", label="crash fixture", allocation=None, advertised={}) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = owned.read_state() or StateDoc.empty(ref.job_id)
        if interrupted == "phase":
            doc = doc.updated(phase={"kind": "running", "attempt_id": "0123456789abcdef0123456789abcdef"})
        elif interrupted == "commit":
            doc = doc.with_commit({"action": "continue"})
        else:
            doc = doc.with_release(Release(owned.from_state, owned.from_priority))
        owned.write_state(doc)
        owned.give_back()
    if interrupted == "pending_release":
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None
        observed_doc, _damage = read_state_unowned(current.path / "state.json")
        assert observed_doc is not None
        (current.path / "state.json").write_bytes(
            encode_state(observed_doc.with_release(Release("paused", current.priority)))
        )
    published = request_files.post(
        workspace, action="pause", job_id=ref.job_id, placement=ref.placement or "", operator="tester", reason="r"
    )

    (outcome,) = serve_requests(workspace, [(ref, published)])
    assert outcome.status == "queued" and "reconciliation" in outcome.explanation
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None
    observed_doc, _damage = read_state_unowned(current.path / "state.json")
    assert observed_doc is not None
    if interrupted == "phase":
        assert observed_doc.phase["kind"] == "running"
    elif interrupted == "commit":
        assert doc.commit == {"action": "continue"}
    else:
        assert doc.release_to is not None


def test_failed_seal_and_eject_are_reported_as_refused_with_jobs_returned(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, name = _new_workspace(tmp_path)
    sealed = _job(tmp_path, workspace, "seal-fails")
    ejected = _job(tmp_path, workspace, "eject-fails")
    from httk.workflow.manager import TaskManager

    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=60)
    assert command(["job", "unseal", "--workspace", name, "--force", sealed.job_id], _context(tmp_path)) == 0
    capsys.readouterr()
    monkeypatch.setattr(seals, "seal_payload", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad payload")))
    assert command(_request_args(name, sealed.job_id, action="seal"), _context(tmp_path)) == 1
    assert "\trefused\tbad payload" in capsys.readouterr().out
    assert v3.find(workspace, sealed.job_id).state == "succeeded"

    # Make this job terminal before ejecting, and occupy the requested destination name.
    ready = _job(tmp_path, workspace, "eject-ready")
    assert command(_request_args(name, ready.job_id), _context(tmp_path)) == 0
    capsys.readouterr()
    destination = tmp_path / "occupied"
    destination.mkdir()
    (destination / ejected.job_key).mkdir()
    (destination / ejected.job_key / "occupied").write_text("occupied", encoding="utf-8")
    assert (
        command(
            _request_args(name, ejected.job_id, action="eject") + ["--destination", str(destination)],
            _context(tmp_path),
        )
        == 1
    )
    assert "\trefused\teject failed" in capsys.readouterr().out
    assert _kernel.locate(workspace, ejected.job_id, placement_hint=ejected.placement, exhaustive=True) is not None
    assert not _requests(workspace)


@pytest.mark.parametrize("preceding", [False, True])
def test_lost_claim_that_deletes_job_reports_uncertainty_for_the_requested_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, preceding: bool
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "disappearing-winner")
    if preceding:
        request_files.post(
            workspace,
            action="pause",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="test",
            reason="prior",
            created_at="2020-01-01T00:00:00Z",
        )
    path = request_files.post(
        workspace, action="pause", job_id=ref.job_id, placement=ref.placement or "", operator="test", reason="requested"
    )

    def competing_delete(*_args, **_kwargs):
        with _kernel.register_owner(workspace, kind="cli", label="competitor", allocation=None, advertised={}) as owner:
            current = v3.find(workspace, ref.job_id)
            held = _kernel.claim(workspace, owner, current)
            assert held is not None
            held.discard()
        return False

    monkeypatch.setattr(removal_module, "serve", competing_delete)
    (result,) = serve_requests(workspace, [(ref, path)])
    assert result.status == "uncertain"
    assert result.request_id == path.name.split(".")[1]
    assert _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True) is None


def _post(workspace: Workspace, ref: _kernel.JobRef, action: str, created_at: str) -> Path:
    return request_files.post(
        workspace,
        action=action,
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="tester",
        reason="r",
        created_at=created_at,
    )


def test_requests_apply_in_the_order_of_their_instants_not_their_timestamp_strings(tmp_path: Path) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "instant-order")
    # 00:00Z, written with an offset, precedes 01:00Z although its string sorts after it.
    pause = _post(workspace, ref, "pause", "2026-01-01T02:00:00+02:00")
    cancel = _post(workspace, ref, "cancel", "2026-01-01T01:00:00.000000Z")

    (outcome,) = serve_requests(workspace, [(ref, cancel)])
    assert (outcome.status, outcome.state) == ("applied", "cancelled")
    applied = [entry["request_id"] for entry in _applied_state(workspace, ref.job_id)]
    assert applied == [pause.name.split(".")[1], cancel.name.split(".")[1]]


def test_a_preceding_request_that_cannot_be_served_gives_the_requested_one_its_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "preceding-fails")
    _post(workspace, ref, "pause", "2026-01-01T00:00:00Z")
    target = _post(workspace, ref, "cancel", "2026-01-01T00:00:01Z")

    def fail(*_args: object, **_kwargs: object) -> bool:
        raise WorkflowError("cannot read the job")

    monkeypatch.setattr(removal_module, "serve", fail)
    (outcome,) = serve_requests(workspace, [(ref, target)])
    assert (outcome.status, outcome.explanation) == ("uncertain", "cannot read the job")
    assert len(_requests(workspace)) == 2


def test_a_job_needing_reconciliation_is_claimed_once_for_all_its_preceding_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "reconcile-once")
    with _kernel.register_owner(workspace, kind="cli", label="crash fixture", allocation=None, advertised={}) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = (owned.read_state() or StateDoc.empty(ref.job_id)).with_commit({"action": "continue"})
        owned.write_state(doc)
        owned.give_back()
    for second in range(3):
        _post(workspace, ref, "pause", f"2026-01-01T00:00:0{second}Z")
    target = _post(workspace, ref, "cancel", "2026-01-01T00:01:00Z")
    claims: list[object] = []
    real_claim = _kernel.claim

    def counting_claim(*args, **kwargs):
        claims.append(args[2])
        return real_claim(*args, **kwargs)

    monkeypatch.setattr(_kernel, "claim", counting_claim)
    (outcome,) = serve_requests(workspace, [(ref, target)])
    assert (outcome.status, outcome.explanation) == ("queued", "job needs reconciliation")
    assert len(claims) == 1 and len(_requests(workspace)) == 4
