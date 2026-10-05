"""Durability and corruption tests for the private daemon ledger."""

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from httk.workflow import _daemon_state as state_module
from httk.workflow._daemon_protocol import Request, Response, request_digest
from httk.workflow._daemon_state import CapacityError, ConflictError, Ledger

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"


def _request(
    number: int,
    operation: str = "health",
    *,
    profile: str | None = None,
    handle: str | None = None,
) -> Request:
    return Request(
        f"{number:032x}",
        WORKSPACE_ID,
        operation,
        profile=profile,
        handle=handle,
        enrollment_id=ENROLLMENT_ID,
        configuration_digest="0" * 64 if operation == "start_manager" else None,
    )


def _response(request: Request, outcome: str, **fields: str) -> Response:
    return Response(
        request.request_id,
        request.workspace_id,
        request.enrollment_id,
        request_digest(request),
        outcome,
        **fields,
    )


def test_initialize_is_exclusive_and_second_writer_is_locked(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True), pytest.raises(OSError, match="locked"):
        Ledger(state, WORKSPACE_ID, ENROLLMENT_ID)
    with pytest.raises(FileExistsError):
        Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True)
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID):
        pass


def test_missing_corrupt_partial_and_wrong_identity_fail_closed(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    missing.mkdir()
    with pytest.raises(FileNotFoundError):
        Ledger(missing, WORKSPACE_ID, ENROLLMENT_ID)

    corrupt = tmp_path / "corrupt"
    corrupt.mkdir()
    (corrupt / "ledger.sqlite3").write_bytes(b"not sqlite")
    with pytest.raises(sqlite3.DatabaseError):
        Ledger(corrupt, WORKSPACE_ID, ENROLLMENT_ID)

    partial = tmp_path / "partial"
    partial.mkdir()
    sqlite3.connect(partial / "ledger.sqlite3").close()
    with pytest.raises(sqlite3.DatabaseError):
        Ledger(partial, WORKSPACE_ID, ENROLLMENT_ID)

    state = tmp_path / "identity"
    state.mkdir()
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True):
        pass
    with pytest.raises(sqlite3.DatabaseError, match="identity"):
        Ledger(state, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", ENROLLMENT_ID)
    with pytest.raises(sqlite3.DatabaseError, match="identity"):
        Ledger(state, WORKSPACE_ID, "f" * 32)


def test_replay_conflict_and_canonical_persisted_response(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    request = _request(1)
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        first = ledger.admit(request)
        assert first.state == "received"
        response = _response(request, "ready")
        finished = ledger.finish(request.request_id, response)
        assert finished.response == response
        assert ledger.admit(request) == finished

        changed = Request(
            request.request_id,
            WORKSPACE_ID,
            "start_manager",
            profile="cpu",
            enrollment_id=ENROLLMENT_ID,
            configuration_digest="1" * 64,
        )
        with pytest.raises(ConflictError):
            ledger.admit(changed)
        assert ledger.admit(request) == finished

    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID) as ledger:
        assert ledger.admit(request).response == response


def test_lookup_request_is_read_only_and_validates_identifiers(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    request = _request(1)
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        assert ledger.lookup_request(request.request_id) is None
        admitted = ledger.admit(request)
        assert ledger.lookup_request(request.request_id) == admitted
        assert ledger.lookup_request(f"{2:032x}") is None
        with pytest.raises(ValueError, match="identifier"):
            ledger.lookup_request("invalid")


def test_ledger_refuses_signed_responses(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    request = _request(1)
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        ledger.admit(request)
        signed = replace(_response(request, "ready"), operator_key="operator", signature="signature")
        with pytest.raises(ValueError, match="unsigned"):
            ledger.finish(request.request_id, signed)


@pytest.mark.parametrize("old_version", [1, 2])
def test_old_ledger_versions_are_preserved_and_refused(tmp_path: Path, old_version: int) -> None:
    state = tmp_path / "state"
    state.mkdir()
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True):
        pass
    database = state / "ledger.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute(f"PRAGMA user_version={old_version}")
    connection.commit()
    connection.close()
    old_ledger = database.read_bytes()

    with pytest.raises(sqlite3.DatabaseError, match="preserve this state.*reconcile.*new enrollment"):
        Ledger(state, WORKSPACE_ID, ENROLLMENT_ID)
    assert database.read_bytes() == old_ledger


def test_submission_intent_recovers_to_uncertain_without_scheduler_identity(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    request = _request(1, "start_manager", profile="cpu")
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        admitted = ledger.admit(request)
        assert admitted.handle is not None
        submitting = ledger.begin_submission(request.request_id)
        assert submitting.state == "submitting"

    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID) as ledger:
        ledger.recover()
        recovered = ledger.admit(request)
        assert recovered.state == "uncertain"
        assert recovered.handle is not None
        assert recovered.job_id is None
        assert recovered.cluster is None
        assert recovered.response == _response(
            request,
            "uncertain",
            handle=recovered.handle,
            reason="submission_unconfirmed",
        )
        with pytest.raises(ValueError, match="received"):
            ledger.begin_submission(request.request_id)


def test_confirmed_submission_requires_protected_scheduler_identity(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    request = _request(1, "start_manager", profile="cpu")
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        entry = ledger.begin_submission(ledger.admit(request).request.request_id)
        assert entry.handle is not None
        response = _response(request, "submitted", handle=entry.handle)
        with pytest.raises(ValueError, match="scheduler identity"):
            ledger.finish(request.request_id, response, job_id="0", cluster="cluster-1")
        finished = ledger.finish(request.request_id, response, job_id="123", cluster="cluster-1")
        assert (finished.state, finished.job_id, finished.cluster) == ("submitted", "123", "cluster-1")
        assert ledger.lookup_manager(entry.handle) == finished


def test_record_and_cumulative_submission_quotas_replay_before_capacity(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    start = _request(1, "start_manager", profile="cpu")
    health = _request(2)
    extra = _request(3)
    with Ledger(
        state,
        WORKSPACE_ID,
        ENROLLMENT_ID,
        initialize=True,
        max_records=2,
        max_submissions=1,
    ) as ledger:
        admitted = ledger.admit(start)
        assert ledger.admit(start) == admitted
        with pytest.raises(CapacityError, match="submission"):
            ledger.admit(
                Request(
                    f"{4:032x}",
                    WORKSPACE_ID,
                    "start_manager",
                    profile="cpu",
                    enrollment_id=ENROLLMENT_ID,
                    configuration_digest="1" * 64,
                )
            )
        ledger.admit(health)
        assert ledger.admit(health).request == health
        with pytest.raises(CapacityError, match="record"):
            ledger.admit(extra)


def test_corrupt_stored_wire_and_identity_fail_closed(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    request = _request(1)
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        ledger.admit(request)
    connection = sqlite3.connect(state / "ledger.sqlite3")
    connection.execute("UPDATE requests SET request=? WHERE request_id=?", (b"{}", request.request_id))
    connection.commit()
    connection.close()
    with pytest.raises(sqlite3.DatabaseError, match="stored request"):
        Ledger(state, WORKSPACE_ID, ENROLLMENT_ID)


def test_close_is_idempotent_and_releases_lock(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    ledger = Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True)
    ledger.close()
    ledger.close()
    with pytest.raises(ValueError, match="closed"):
        ledger.admit(_request(1))
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID):
        pass


def test_close_marks_descriptors_closed_before_reverse_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state = tmp_path / "state"
    state.mkdir()
    ledger = Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True)
    lock_fd = ledger._lock_fd
    directory_fd = ledger._directory_fd
    original_close = state_module.os.close
    closed: list[int] = []

    def record_close(descriptor: int) -> None:
        assert ledger._lock_fd == -1
        if descriptor == directory_fd:
            assert ledger._directory_fd == -1
        closed.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(state_module.os, "close", record_close)
    ledger.close()
    assert closed == [lock_fd, directory_fd]


def test_managers_lists_only_manager_starts_with_ledger_state(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    start = _request(2, "start_manager", profile="cpu")
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True) as ledger:
        ledger.admit(_request(1))
        handle = ledger.admit(start).handle
        assert handle is not None
        row = {"handle": handle, "profile": "cpu", "request_id": start.request_id}
        assert ledger.managers() == [{**row, "state": "received", "job_id": None}]
        ledger.begin_submission(start.request_id)
        ledger.finish(start.request_id, _response(start, "submitted", handle=handle), job_id="7", cluster="c")
        assert ledger.managers() == [{**row, "state": "submitted", "job_id": "7"}]
