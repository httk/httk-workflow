"""Foreground daemon broker behavior and ordering tests."""

import argparse
import base64
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from httk.core.identity import identity_public_key

from httk.workflow import _daemon_exchange as exchange_module
from httk.workflow import _daemon_service as service_module
from httk.workflow._daemon_activation import activation_document
from httk.workflow._daemon_auth import sign_request, verify_response
from httk.workflow._daemon_exchange import ExchangeMover
from httk.workflow._daemon_keys import initialize_response_seed, response_public_key, response_seed_path
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import ApprovedLauncher, Policy
from httk.workflow._daemon_protocol import Request, Response, decode_response, encode_request
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import Observation, SchedulerError, SlurmGateway, Submission, UncertainSubmission
from httk.workflow._daemon_state import Ledger

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"


class RecordingGateway(SlurmGateway):
    """Record scheduler calls while retaining the concrete gateway API."""

    def __init__(self, policy: Policy) -> None:
        self.policy = policy
        self.submissions: list[tuple[ApprovedLauncher, str]] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.cancellations: list[tuple[str, str, str]] = []
        self.submit_error = False
        self.status_error = False
        self.cancel_error = False
        self.stop: threading.Event | None = None

    def check(self) -> None:
        pass

    def submit(self, launcher: ApprovedLauncher, handle: str) -> Submission:
        self.submissions.append((launcher, handle))
        if self.stop is not None:
            self.stop.set()
        if self.submit_error:
            raise SchedulerError("test submit failure")
        return Submission("42", self.policy.cluster)

    def status(self, job_id: str, cluster: str, handle: str) -> str:
        self.statuses.append((job_id, cluster, handle))
        if self.status_error:
            raise SchedulerError("test status failure")
        return "RUNNING"

    def cancel(self, job_id: str, cluster: str, handle: str) -> None:
        self.cancellations.append((job_id, cluster, handle))
        if self.cancel_error:
            raise SchedulerError("test cancel failure")


def _seed(path: Path, byte: int) -> Path:
    if not path.exists():
        path.write_bytes(base64.b64encode(bytes([byte]) * 32) + b"\n")
        path.chmod(0o600)
    return path


def _operator_seed(tmp_path: Path) -> Path:
    return _seed(tmp_path / "operator.seed", 7)


def _policy(tmp_path: Path, *, max_records: int = 64, max_submissions: int = 8) -> Policy:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
    operator_key = identity_public_key(_operator_seed(tmp_path))
    assert operator_key is not None
    return Policy(
        workspace=tmp_path / "site/workspace",
        workspace_id=WORKSPACE_ID,
        enrollment_id=ENROLLMENT_ID,
        exchange=tmp_path / "site/exchange",
        state=tmp_path / "state",
        snapshots=tmp_path / "snapshots",
        bwrap=runtime / "bwrap",
        python=runtime / "python",
        sbatch=broker / "sbatch",
        squeue=broker / "squeue",
        scancel=broker / "scancel",
        cluster="cluster-1",
        launchers=(ApprovedLauncher("cpu", (("manager.confine", "bwrap"), ("slurm.cpus_per_task", "2")), "d" * 64),),
        authorized_keys=(operator_key,),
        max_records=max_records,
        max_submissions=max_submissions,
        poll_seconds=0.05,
    )


def _request(
    tmp_path: Path,
    number: int,
    operation: str = "health",
    *,
    seed: Path | None = None,
    now: int | None = None,
    lifetime: int = 3600,
    workspace_id: str = WORKSPACE_ID,
    enrollment_id: str = ENROLLMENT_ID,
    profile: str | None = None,
    handle: str | None = None,
    configuration_digest: str | None = None,
    bundle: str | None = None,
) -> Request:
    if operation == "start_manager" and configuration_digest is None:
        configuration_digest = _policy(tmp_path).configuration_digest(profile) if profile == "cpu" else "0" * 64
    request = Request(
        f"{number:032x}",
        workspace_id,
        operation,
        profile=profile,
        handle=handle,
        enrollment_id=enrollment_id,
        configuration_digest=configuration_digest,
        bundle=bundle,
    )
    return sign_request(
        request,
        seed_path=_operator_seed(tmp_path) if seed is None else seed,
        now=now,
        lifetime=lifetime,
    )


def _open(tmp_path: Path, policy: Policy, *, initialize: bool = True) -> tuple[ExitStack, Broker, Ledger]:
    for path in (policy.requests, policy.responses, policy.state):
        path.mkdir(parents=True, exist_ok=True)
    if initialize:
        initialize_response_seed(policy.state)
    stack = ExitStack()
    ledger = stack.enter_context(
        Ledger(
            policy.state,
            policy.workspace_id,
            policy.enrollment_id,
            initialize=initialize,
            max_records=policy.max_records,
            max_submissions=policy.max_submissions,
        )
    )
    requests = stack.enter_context(MailboxDirectory(policy.requests))
    responses = stack.enter_context(MailboxDirectory(policy.responses))
    broker = Broker(
        policy,
        RecordingGateway(policy),
        ledger,
        requests,
        responses,
        exchange=_mover(policy),
        response_seed=response_seed_path(policy.state),
    )
    return stack, broker, ledger


def _mover(policy: Policy) -> ExchangeMover:
    return ExchangeMover(policy.root, policy.exchange.name, policy.workspace.name, policy.enrollment_id)


def _publish(broker: Broker, request: Request) -> str:
    name = f"{request.request_id}.json"
    broker.requests.replace(name, encode_request(request))
    return name


def _read(broker: Broker, request: Request) -> Response:
    response = decode_response(broker.responses.read(f"{request.request_id}.json"))
    verify_response(response, response_public_key(broker.response_seed))
    return response


def _capture_run_error(arguments: argparse.Namespace, errors: list[BaseException]) -> None:
    try:
        service_module._run(arguments)
    except BaseException as exc:
        errors.append(exc)


def test_health_start_status_and_cancel_use_protected_identities(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        health = _request(tmp_path, 1)
        _publish(broker, health)
        assert broker.process_once(threading.Event()) == 1
        assert _read(broker, health).outcome == "ready"

        start = _request(tmp_path, 2, "start_manager", profile="cpu")
        _publish(broker, start)
        broker.process_once(threading.Event())
        submitted = _read(broker, start)
        assert submitted.outcome == "submitted" and submitted.handle is not None
        assert gateway.submissions[0][0] == policy.launcher("cpu")

        status = _request(tmp_path, 3, "manager_status", handle=submitted.handle)
        _publish(broker, status)
        broker.process_once(threading.Event())
        assert _read(broker, status).scheduler_state == "RUNNING"
        assert gateway.statuses == [("42", policy.cluster, submitted.handle)]

        cancel = _request(tmp_path, 4, "cancel_manager", handle=submitted.handle)
        _publish(broker, cancel)
        broker.process_once(threading.Event())
        assert _read(broker, cancel).outcome == "cancel_requested"
        assert gateway.cancellations == [("42", policy.cluster, submitted.handle)]
        calls = gateway.submissions + gateway.statuses + gateway.cancellations
        assert str(broker.response_seed) not in repr(calls)
    finally:
        stack.close()


def test_completed_old_configuration_replays_but_new_and_received_old_starts_refuse(tmp_path: Path) -> None:
    old_policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, old_policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        completed = _request(tmp_path, 1, "start_manager", profile="cpu")
        _publish(broker, completed)
        broker.process_once(threading.Event())
        original_response = _read(broker, completed)
        assert original_response.outcome == "submitted"
        assert len(gateway.submissions) == 1

        current_policy = replace(old_policy, launchers=(replace(old_policy.launcher("cpu"), digest="e" * 64),))
        broker.policy = current_policy
        gateway.policy = current_policy
        _publish(broker, completed)
        broker.process_once(threading.Event())
        assert _read(broker, completed) == original_response
        assert len(gateway.submissions) == 1

        stale = _request(
            tmp_path,
            2,
            "start_manager",
            profile="cpu",
            configuration_digest=old_policy.configuration_digest("cpu"),
        )
        _publish(broker, stale)
        broker.process_once(threading.Event())
        assert _read(broker, stale).reason == "stale_configuration"
        assert len(gateway.submissions) == 1

        recovered = _request(
            tmp_path,
            3,
            "start_manager",
            profile="cpu",
            configuration_digest=old_policy.configuration_digest("cpu"),
        )
        ledger.admit(recovered)
        _publish(broker, recovered)
        broker.process_once(threading.Event())
        assert _read(broker, recovered).reason == "stale_configuration"
        assert ledger.lookup_request(recovered.request_id).state == "refused"  # type: ignore[union-attr]
        assert len(gateway.submissions) == 1
    finally:
        stack.close()


def test_service_rechecks_protected_activation_after_acquiring_ledger_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _policy(tmp_path)
    for path in (policy.state, policy.requests, policy.responses):
        path.mkdir(parents=True)
    initialize_response_seed(policy.state)
    with Ledger(policy.state, policy.workspace_id, policy.enrollment_id, initialize=True):
        pass
    selected = tmp_path / "snapshots/selected.json"
    replacement = tmp_path / "snapshots/replacement.json"
    (policy.state / "active.json").write_text(json.dumps(activation_document(selected, policy)), encoding="utf-8")
    (policy.state / "active.json").chmod(0o600)
    checks: list[None] = []

    class Gateway(RecordingGateway):
        def __init__(self, configured: Policy) -> None:
            super().__init__(configured)

        def check(self) -> None:
            checks.append(None)

    monkeypatch.setattr(service_module, "_SNAPSHOT_POLICY", selected)
    monkeypatch.setattr(service_module, "_STATE_DIRECTORY", policy.state)
    monkeypatch.setattr(service_module, "_ROOT_DIRECTORY", policy.root)
    monkeypatch.setattr(service_module, "SlurmGateway", Gateway)
    monkeypatch.setattr(service_module.signal, "signal", lambda *_args: None)
    arguments = argparse.Namespace(policy=selected, policy_source=selected, check=True, once=False)
    loaded = threading.Event()
    resume = threading.Event()
    errors: list[BaseException] = []

    def load_selected(_path: Path) -> Policy:
        loaded.set()
        assert resume.wait(5)
        return policy

    monkeypatch.setattr(service_module, "load_policy", load_selected)
    startup = threading.Thread(target=lambda: _capture_run_error(arguments, errors))
    startup.start()
    assert loaded.wait(5)
    (policy.state / "active.json").write_text(json.dumps(activation_document(replacement, policy)), encoding="utf-8")
    (policy.state / "active.json").chmod(0o600)
    resume.set()
    startup.join(5)
    assert not startup.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], ValueError)
    assert "stale or mismatched" in str(errors[0])
    assert checks == []
    (policy.state / "active.json").write_text(json.dumps(activation_document(selected, policy)), encoding="utf-8")
    (policy.state / "active.json").chmod(0o600)
    monkeypatch.setattr(service_module, "load_policy", lambda _path: policy)
    service_module._run(arguments)
    assert checks == [None]


def test_refusals_scheduler_failures_conflicts_and_capacity_are_signed(tmp_path: Path) -> None:
    policy = _policy(tmp_path, max_records=3, max_submissions=2)
    stack, broker, ledger = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        invalid = _request(tmp_path, 1, "start_manager", profile="missing")
        _publish(broker, invalid)
        broker.process_once(threading.Event())
        assert (_read(broker, invalid).outcome, _read(broker, invalid).reason) == (
            "refused",
            "invalid_configuration",
        )

        gateway.submit_error = True
        uncertain = _request(tmp_path, 2, "start_manager", profile="cpu")
        _publish(broker, uncertain)
        broker.process_once(threading.Event())
        assert (_read(broker, uncertain).outcome, _read(broker, uncertain).reason) == (
            "uncertain",
            "submission_unconfirmed",
        )
        _publish(broker, uncertain)
        broker.process_once(threading.Event())
        assert len(gateway.submissions) == 1

        admitted = _request(tmp_path, 3)
        _publish(broker, admitted)
        broker.process_once(threading.Event())
        conflict = _request(tmp_path, 3, "start_manager", profile="cpu")
        _publish(broker, conflict)
        broker.process_once(threading.Event())
        assert _read(broker, conflict).reason == "request_conflict"
        assert ledger.lookup_request(admitted.request_id) is not None

        capacity = _request(tmp_path, 4)
        _publish(broker, capacity)
        broker.process_once(threading.Event())
        assert (_read(broker, capacity).outcome, _read(broker, capacity).reason) == ("busy", "capacity")
    finally:
        stack.close()


def test_wrong_workspace_and_enrollment_are_signed_without_admission(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    try:
        requests = (
            _request(tmp_path, 1, workspace_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            _request(tmp_path, 2, enrollment_id="f" * 32),
        )
        for request, reason in zip(requests, ("wrong_workspace", "wrong_enrollment"), strict=True):
            _publish(broker, request)
            broker.process_once(threading.Event())
            assert _read(broker, request).reason == reason
            assert ledger.lookup_request(request.request_id) is None
    finally:
        stack.close()


def test_status_cancel_failure_contracts_and_unknown_manager(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        unknown = _request(tmp_path, 1, "manager_status", handle="f" * 32)
        _publish(broker, unknown)
        broker.process_once(threading.Event())
        assert (_read(broker, unknown).outcome, _read(broker, unknown).reason) == (
            "refused",
            "unknown_manager",
        )

        start = _request(tmp_path, 2, "start_manager", profile="cpu")
        _publish(broker, start)
        broker.process_once(threading.Event())
        handle = _read(broker, start).handle
        assert handle is not None

        gateway.status_error = True
        status = _request(tmp_path, 3, "manager_status", handle=handle)
        _publish(broker, status)
        broker.process_once(threading.Event())
        assert (_read(broker, status).scheduler_state, _read(broker, status).detail) == ("UNKNOWN", None)

        gateway.cancel_error = True
        cancel = _request(tmp_path, 4, "cancel_manager", handle=handle)
        _publish(broker, cancel)
        broker.process_once(threading.Event())
        assert (_read(broker, cancel).outcome, _read(broker, cancel).reason, _read(broker, cancel).detail) == (
            "refused",
            "scheduler_unavailable",
            "test cancel failure",
        )
    finally:
        stack.close()


def test_unsigned_forged_and_unlisted_requests_are_discarded_before_action(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        unsigned = Request(f"{1:032x}", WORKSPACE_ID, "health", enrollment_id=ENROLLMENT_ID)
        forged = replace(_request(tmp_path, 2), expires_at=1)
        unlisted = _request(tmp_path, 3, seed=_seed(tmp_path / "other.seed", 9))
        for request in (unsigned, forged, unlisted):
            publication = _publish(broker, request)
            broker.process_once(threading.Event())
            with pytest.raises(FileNotFoundError):
                broker.requests.read(publication)
            with pytest.raises(FileNotFoundError):
                broker.responses.read(publication)
            assert ledger.lookup_request(request.request_id) is None
        assert not (gateway.submissions or gateway.statuses or gateway.cancellations)
    finally:
        stack.close()


def test_malformed_and_mismatched_publications_are_discarded(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    try:
        malformed = f"{1:032x}.json"
        broker.requests.replace(malformed, b"{}")
        request = _request(tmp_path, 2)
        mismatch = f"{3:032x}.json"
        broker.requests.replace(mismatch, encode_request(request))
        assert broker.process_once(threading.Event()) == 2
        for publication in (malformed, mismatch):
            with pytest.raises(FileNotFoundError):
                broker.requests.read(publication)
        assert ledger.lookup_request(request.request_id) is None
    finally:
        stack.close()


def test_revoked_key_cannot_replay_until_reauthorized(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    request = _request(tmp_path, 1)
    try:
        _publish(broker, request)
        broker.process_once(threading.Event())
        assert _read(broker, request).outcome == "ready"
        broker.responses.remove(f"{request.request_id}.json")
        revoked = Broker(
            replace(policy, authorized_keys=()),
            RecordingGateway(policy),
            ledger,
            broker.requests,
            broker.responses,
            exchange=broker.exchange,
            response_seed=broker.response_seed,
        )
        publication = _publish(revoked, request)
        revoked.process_once(threading.Event())
        with pytest.raises(FileNotFoundError):
            revoked.responses.read(publication)
        _publish(broker, request)
        broker.process_once(threading.Event())
        assert _read(broker, request).outcome == "ready"
    finally:
        stack.close()


def test_stale_new_request_is_durably_refused_and_replays_after_restart(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    request = _request(tmp_path, 1, now=1, lifetime=60)
    _publish(broker, request)
    broker.process_once(threading.Event())
    assert (_read(broker, request).outcome, _read(broker, request).reason) == ("refused", "request_expired")
    stored = ledger.lookup_request(request.request_id)
    assert stored is not None and stored.response is not None
    broker.responses.remove(f"{request.request_id}.json")
    stack.close()

    restart_stack, restarted, _ = _open(tmp_path, policy, initialize=False)
    try:
        _publish(restarted, request)
        restarted.process_once(threading.Event())
        assert (_read(restarted, request).outcome, _read(restarted, request).reason) == (
            "refused",
            "request_expired",
        )
    finally:
        restart_stack.close()


def test_received_request_left_by_crash_is_refused_when_expired(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    request = _request(tmp_path, 1, now=1, lifetime=60)
    ledger.admit(request)
    _publish(broker, request)
    stack.close()
    restart_stack, restarted, restarted_ledger = _open(tmp_path, policy, initialize=False)
    try:
        restarted.process_once(threading.Event())
        response = _read(restarted, request)
        assert (response.outcome, response.reason) == ("refused", "request_expired")
        stored = restarted_ledger.lookup_request(request.request_id)
        assert stored is not None and stored.response == replace(response, operator_key=None, signature=None)
    finally:
        restart_stack.close()


@pytest.mark.parametrize("operation", ["start_manager", "manager_status", "cancel_manager"])
def test_clock_advance_before_scheduler_action_refuses_without_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        handle: str | None = None
        if operation != "start_manager":
            start = _request(tmp_path, 1, "start_manager", profile="cpu")
            _publish(broker, start)
            broker.process_once(threading.Event())
            handle = _read(broker, start).handle
        checks = 0

        def advance_clock(_request_value: Request, *, max_age: int) -> None:
            nonlocal checks
            assert max_age == policy.request_max_age
            checks += 1
            if checks == 2:
                raise ValueError("expired")

        monkeypatch.setattr(service_module, "check_request_time", advance_clock)
        request = (
            _request(tmp_path, 2, operation, profile="cpu")
            if operation == "start_manager"
            else _request(tmp_path, 2, operation, handle=handle)
        )
        _publish(broker, request)
        broker.process_once(threading.Event())
        assert (_read(broker, request).outcome, _read(broker, request).reason) == (
            "refused",
            "request_expired",
        )
        assert checks == 2 and not gateway.statuses and not gateway.cancellations
        assert len(gateway.submissions) == (0 if operation == "start_manager" else 1)
    finally:
        stack.close()


def test_completed_replay_bypasses_time_check_after_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    request = _request(tmp_path, 1)
    _publish(broker, request)
    broker.process_once(threading.Event())
    broker.responses.remove(f"{request.request_id}.json")
    stack.close()

    def refuse_time(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("completed replay must not recheck time")

    monkeypatch.setattr(service_module, "check_request_time", refuse_time)
    restart_stack, restarted, _ = _open(tmp_path, policy, initialize=False)
    try:
        _publish(restarted, request)
        restarted.process_once(threading.Event())
        assert _read(restarted, request).outcome == "ready"
    finally:
        restart_stack.close()


def test_commit_precedes_publish_and_restart_does_not_resubmit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    request = _request(tmp_path, 1, "start_manager", profile="cpu")
    publication = _publish(broker, request)

    def fail_publish(_name: str, _data: bytes) -> None:
        stored = ledger.lookup_request(request.request_id)
        assert stored is not None and stored.response is not None
        raise OSError("injected response failure")

    monkeypatch.setattr(broker.responses, "replace", fail_publish)
    with pytest.raises(OSError, match="response failure"):
        broker.process_once(threading.Event())
    assert broker.requests.read(publication) == encode_request(request)
    assert len(gateway.submissions) == 1
    stack.close()

    restart_stack, restarted, _ = _open(tmp_path, policy, initialize=False)
    try:
        restarted.process_once(threading.Event())
        assert _read(restarted, request).outcome == "submitted"
        assert isinstance(restarted.gateway, RecordingGateway) and not restarted.gateway.submissions
    finally:
        restart_stack.close()


def test_broker_validates_response_seed_before_admission(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    seed = broker.response_seed
    try:
        seed.unlink()
        with pytest.raises(FileNotFoundError):
            Broker(
                policy,
                RecordingGateway(policy),
                ledger,
                broker.requests,
                broker.responses,
                exchange=broker.exchange,
                response_seed=seed,
            )
        seed.write_text("bad\n", encoding="ascii")
        seed.chmod(0o600)
        with pytest.raises(ValueError, match="canonical base64"):
            Broker(
                policy,
                RecordingGateway(policy),
                ledger,
                broker.requests,
                broker.responses,
                exchange=broker.exchange,
                response_seed=seed,
            )
        assert ledger.lookup_request(f"{1:032x}") is None
    finally:
        stack.close()


def test_scheduler_failure_detail_is_signed_logged_and_replayed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    message = "sbatch exited 1: sbatch: error: Invalid account or account/partition combination"

    def refuse(_launcher: ApprovedLauncher, _handle: str) -> Submission:
        raise UncertainSubmission(message)

    monkeypatch.setattr(broker.gateway, "submit", refuse)
    try:
        start = _request(tmp_path, 1, "start_manager", profile="cpu")
        with caplog.at_level(logging.INFO, logger="httk.workflow"):
            _publish(broker, start)
            broker.process_once(threading.Event())
        response = _read(broker, start)
        assert (response.outcome, response.reason, response.detail) == ("uncertain", "submission_unconfirmed", message)
        failures = [
            record
            for record in caplog.records
            if record.name == service_module.__name__ and record.levelno == logging.WARNING
        ]
        assert len(failures) == 1 and message in failures[0].getMessage()
        assert f"daemon_request request_id={start.request_id} operation=start_manager outcome=uncertain" in caplog.text
        _publish(broker, start)
        broker.process_once(threading.Event())
        assert _read(broker, start) == response
    finally:
        stack.close()


def test_requests_and_confirmed_submissions_are_logged(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    try:
        health = _request(tmp_path, 1)
        start = _request(tmp_path, 2, "start_manager", profile="cpu")
        with caplog.at_level(logging.INFO, logger="httk.workflow"):
            _publish(broker, health)
            _publish(broker, start)
            broker.process_once(threading.Event())
        handle = _read(broker, start).handle
        log = tmp_path / "snapshots/jobs/httk-42.out"
        assert f"daemon_submitted handle={handle} job_id=42 cluster=cluster-1 log={log}" in caplog.text
        assert (
            f"daemon_request request_id={health.request_id} operation=health outcome=ready reason=None handle=None"
            in caplog.text
        )
        assert (
            f"daemon_request request_id={start.request_id} operation=start_manager outcome=submitted "
            f"reason=None handle={handle}" in caplog.text
        )
    finally:
        stack.close()


def test_main_reports_state_recovery_guidance_and_scheduler_reasons(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    for error in (
        sqlite3.DatabaseError("preserve this state, reconcile outstanding work, and initialize a new enrollment"),
        SchedulerError("squeue timed out: slurm_load_jobs error: Unable to contact slurm controller"),
    ):
        caplog.clear()
        monkeypatch.setattr(service_module, "_run", lambda _arguments, error=error: (_ for _ in ()).throw(error))
        with caplog.at_level(logging.ERROR):
            assert (
                service_module.main(
                    ["--policy", "/tmp/daemon-policy.json", "--policy-source", "/protected-policy.json", "--once"]
                )
                == 1
            )
        assert f"daemon_service_failed reason={error}" in caplog.text


def test_service_entry_point_logs_to_stdout() -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-m", "httk.workflow._daemon_service", "--policy", "/x", "--policy-source", "/y"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1 and result.stderr == ""
    assert " ERROR daemon_service_failed reason=service policy must be /tmp/daemon-policy.json" in result.stdout


def test_service_logs_check_and_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    policy = _policy(tmp_path)
    for path in (policy.state, policy.requests, policy.responses, policy.workspace):
        path.mkdir(parents=True)
    initialize_response_seed(policy.state)
    with Ledger(policy.state, policy.workspace_id, policy.enrollment_id, initialize=True):
        pass
    selected = tmp_path / "snapshots/selected.json"
    (policy.state / "active.json").write_text(json.dumps(activation_document(selected, policy)), encoding="utf-8")
    (policy.state / "active.json").chmod(0o600)
    monkeypatch.setattr(service_module, "_SNAPSHOT_POLICY", selected)
    monkeypatch.setattr(service_module, "_STATE_DIRECTORY", policy.state)
    monkeypatch.setattr(service_module, "_ROOT_DIRECTORY", policy.root)
    monkeypatch.setattr(service_module, "SlurmGateway", lambda configured: RecordingGateway(configured))
    monkeypatch.setattr(service_module, "load_policy", lambda _path: policy)
    monkeypatch.setattr(service_module.signal, "signal", lambda *_args: None)
    with caplog.at_level(logging.INFO, logger="httk.workflow"):
        service_module._run(argparse.Namespace(policy=selected, policy_source=selected, check=False, once=True))
    assert f"daemon_check_passed workspace={WORKSPACE_ID} cluster=cluster-1" in caplog.text
    assert f"daemon_started workspace={WORKSPACE_ID} enrollment={ENROLLMENT_ID} launchers=cpu" in caplog.text


def test_service_uses_the_host_view_tmp_destinations() -> None:
    assert (service_module._SNAPSHOT_POLICY, service_module._STATE_DIRECTORY, service_module._ROOT_DIRECTORY) == (
        Path("/tmp/daemon-policy.json"),
        Path("/tmp/control"),
        Path("/tmp/daemon-root"),
    )
    arguments = argparse.Namespace(policy=Path("/daemon-policy.json"), policy_source=Path("/protected-policy.json"))
    with pytest.raises(ValueError, match="service policy must be /tmp/daemon-policy.json"):
        service_module._run(arguments)


def test_stop_is_checked_between_sorted_requests(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    stop = threading.Event()
    gateway.stop = stop
    first = _request(tmp_path, 1, "start_manager", profile="cpu")
    second = _request(tmp_path, 2)
    try:
        _publish(broker, second)
        _publish(broker, first)
        assert broker.process_once(stop) == 1
        assert _read(broker, first).outcome == "submitted"
        assert broker.requests.read(f"{second.request_id}.json") == encode_request(second)
    finally:
        stack.close()


def test_each_iteration_polls_the_exchange_with_ledger_managers(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    polls: list[list[dict[str, str | None]]] = []

    class Mover(ExchangeMover):
        def poll(self, managers: list[dict[str, str | None]]) -> None:
            polls.append(managers)

    broker.exchange = Mover(policy.root, policy.exchange.name, policy.workspace.name, policy.enrollment_id)
    start = _request(tmp_path, 1, "start_manager", profile="cpu")
    try:
        broker.process_once(threading.Event())
        _publish(broker, start)
        broker.process_once(threading.Event())
        handle = _read(broker, start).handle
        assert handle is not None
        unobserved = dict.fromkeys(("scheduler_state", "exit_code", "started_at", "ended_at", "log"))
        row = {"handle": handle, "profile": "cpu", "request_id": start.request_id, "state": "submitted"}
        assert polls == [[], [{**row, "job_id": "42", **unobserved}]]
    finally:
        stack.close()


def test_exchange_failure_is_logged_and_does_not_stop_the_loop(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    calls: list[None] = []

    class Mover(ExchangeMover):
        def poll(self, managers: list[dict[str, str | None]]) -> None:
            calls.append(None)
            raise OSError("injected exchange failure")

    broker.exchange = Mover(policy.root, policy.exchange.name, policy.workspace.name, policy.enrollment_id)
    request = _request(tmp_path, 1)
    stop = threading.Event()
    try:
        _publish(broker, request)
        with caplog.at_level(logging.ERROR):
            broker.process_once(stop)
            broker.process_once(stop)
        assert _read(broker, request).outcome == "ready"
        assert len(calls) == 2
        assert "daemon_exchange_failed" in caplog.text
    finally:
        stack.close()


def test_service_resolves_mailboxes_under_the_root_bind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy(tmp_path)
    for path in (policy.state, policy.requests, policy.responses):
        path.mkdir(parents=True)
    initialize_response_seed(policy.state)
    with Ledger(policy.state, policy.workspace_id, policy.enrollment_id, initialize=True):
        pass
    selected = tmp_path / "snapshots/selected.json"
    (policy.state / "active.json").write_text(json.dumps(activation_document(selected, policy)), encoding="utf-8")
    (policy.state / "active.json").chmod(0o600)
    opened: list[Path] = []
    original = service_module.MailboxDirectory

    def record(path: Path) -> MailboxDirectory:
        opened.append(path)
        return original(path)

    monkeypatch.setattr(service_module, "_SNAPSHOT_POLICY", selected)
    monkeypatch.setattr(service_module, "_STATE_DIRECTORY", policy.state)
    monkeypatch.setattr(service_module, "_ROOT_DIRECTORY", policy.root)
    monkeypatch.setattr(service_module, "SlurmGateway", lambda configured: RecordingGateway(configured))
    monkeypatch.setattr(service_module, "MailboxDirectory", record)
    monkeypatch.setattr(service_module, "load_policy", lambda _path: policy)
    monkeypatch.setattr(service_module.signal, "signal", lambda *_args: None)
    service_module._run(argparse.Namespace(policy=selected, policy_source=selected, check=True, once=False))
    assert opened == [policy.requests, policy.responses]
    policy.requests.rmdir()
    with pytest.raises(FileNotFoundError):
        service_module._run(argparse.Namespace(policy=selected, policy_source=selected, check=True, once=False))


def test_ledger_listing_failure_is_logged_and_does_not_stop_the_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)

    def corrupt() -> list[dict[str, str]]:
        raise sqlite3.DatabaseError("invalid stored manager start")

    monkeypatch.setattr(ledger, "managers", corrupt)
    request = _request(tmp_path, 1)
    try:
        with caplog.at_level(logging.ERROR):
            broker.process_once(threading.Event())
            _publish(broker, request)
            broker.process_once(threading.Event())
        assert _read(broker, request).outcome == "ready"
        assert caplog.text.count("daemon_exchange_failed") == 2
    finally:
        stack.close()


class ScriptedGateway(RecordingGateway):
    """Report a settable scheduler state and accounting record."""

    def __init__(self, policy: Policy) -> None:
        super().__init__(policy)
        self.state = "PENDING"
        self.record: Observation | None = None
        self.accounting_error = False
        self.accountings: list[tuple[str, str, str]] = []

    def status(self, job_id: str, cluster: str, handle: str) -> str:
        super().status(job_id, cluster, handle)
        return self.state

    def accounting(self, job_id: str, cluster: str, handle: str) -> Observation | None:
        self.accountings.append((job_id, cluster, handle))
        if self.accounting_error:
            raise SchedulerError("sacct exited 1: sacct: error: slurmdbd: Connection refused")
        return self.record


def _observing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, initialize: bool = True, sacct: bool = False
) -> tuple[ExitStack, Broker, ScriptedGateway, list[float]]:
    """Open a broker that follows its managers, on a settable clock."""

    policy = _policy(tmp_path)
    if sacct:
        policy = replace(policy, sacct=tmp_path / "broker/sacct")
    stack, broker, ledger = _open(tmp_path, policy, initialize=initialize)
    staging = policy.workspace / ".httk-workspace/exchange"
    for path in (policy.exchange / "inbox", policy.exchange / "outbox/rejected", staging / "inbox", policy.jobs):
        path.mkdir(parents=True, exist_ok=True)
    (staging / "outbox/rejected").mkdir(parents=True, exist_ok=True)
    gateway = ScriptedGateway(policy)
    observing = Broker(
        policy,
        gateway,
        ledger,
        broker.requests,
        broker.responses,
        exchange=_mover(policy),
        response_seed=broker.response_seed,
        state=policy.state,
    )
    clock = [1_800_000_000.0]
    monkeypatch.setattr(service_module, "time", SimpleNamespace(time=lambda: clock[0]))
    return stack, observing, gateway, clock


def _managers(policy: Policy) -> Any:
    return json.loads((policy.exchange / "outbox/managers.json").read_bytes())


def test_managers_are_followed_to_their_final_state_and_their_log_published_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch)
    policy = broker.policy
    stop = threading.Event()
    start = _request(tmp_path, 1, "start_manager", profile="cpu")
    tail = b"x" * 10 + b"y" * (1024 * 1024)
    (policy.jobs / "httk-42.out").write_bytes(tail)
    try:
        _publish(broker, start)
        with caplog.at_level(logging.INFO):
            broker.process_once(stop)
            handle = _read(broker, start).handle
            assert handle is not None
            (row,) = _managers(policy)["managers"]
            assert row == {
                "handle": handle,
                "profile": "cpu",
                "request_id": start.request_id,
                "state": "submitted",
                "job_id": "42",
                "scheduler_state": "PENDING",
                "exit_code": None,
                "started_at": None,
                "ended_at": None,
                "log": None,
            }
            gateway.state = "RUNNING"
            clock[0] += 30
            broker.process_once(stop)
            assert len(gateway.statuses) == 1  # rate-limited to once a minute
            clock[0] += 30
            broker.process_once(stop)
            assert _managers(policy)["managers"][0]["scheduler_state"] == "RUNNING"
            assert gateway.accountings == []
            gateway.state = "UNKNOWN"
            gateway.record = Observation("FAILED", "1:0", "2026-10-05T10:00:00", "2026-10-05T10:01:00")
            clock[0] += 60
            broker.process_once(stop)
            clock[0] += 600
            broker.process_once(stop)
        assert len(gateway.statuses) == 3 and len(gateway.accountings) == 1
        (row,) = _managers(policy)["managers"]
        assert row["scheduler_state"] == "FAILED" and row["exit_code"] == "1:0"
        assert (row["started_at"], row["ended_at"]) == ("2026-10-05T10:00:00", "2026-10-05T10:01:00")
        assert row["log"] == f"managers/{handle}.log"
        assert (policy.exchange / "outbox" / row["log"]).read_bytes() == tail[10:]
        assert caplog.text.count("daemon_manager_log ") == 1
        assert f"path=outbox/managers/{handle}.log" in caplog.text
        states = [record.getMessage() for record in caplog.records if "daemon_manager_state" in record.getMessage()]
        assert [message.split("state=")[1] for message in states] == [
            "PENDING exit_code=None",
            "RUNNING exit_code=None",
            "FAILED exit_code=1:0",
        ]
        saved = json.loads((policy.state / "observations.json").read_bytes())
        assert saved[handle]["final"] is True and saved[handle]["log_published"] is True
    finally:
        stack.close()

    # Observations survive a restart: a final manager is neither queried nor published again.
    (policy.exchange / "outbox/managers.json").unlink()
    caplog.clear()
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch, initialize=False)
    try:
        with caplog.at_level(logging.INFO):
            broker.process_once(stop)
        assert gateway.statuses == [] and "daemon_manager_log " not in caplog.text
        assert _managers(policy)["managers"][0]["log"] == f"managers/{handle}.log"
    finally:
        stack.close()


def test_a_manager_unknown_everywhere_for_thirty_minutes_is_gone_with_a_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch)
    policy = broker.policy
    stop = threading.Event()
    gateway.state = "UNKNOWN"
    start = _request(tmp_path, 1, "start_manager", profile="cpu")
    try:
        _publish(broker, start)
        broker.process_once(stop)
        handle = _read(broker, start).handle
        clock[0] += 29 * 60
        broker.process_once(stop)
        row = _managers(policy)["managers"][0]
        assert (row["scheduler_state"], row["log"]) == ("UNKNOWN", None)
        clock[0] += 60
        broker.process_once(stop)
        row = _managers(policy)["managers"][0]
        assert (row["scheduler_state"], row["exit_code"]) == ("GONE", None)
        source = policy.jobs / "httk-42.out"
        note = f"no Slurm output was found at {source}\n".encode()
        assert (policy.exchange / "outbox/managers" / f"{handle}.log").read_bytes() == note
        assert len(gateway.accountings) == 3
        clock[0] += 3600
        broker.process_once(stop)
        assert len(gateway.statuses) == 3
    finally:
        stack.close()


def test_scheduler_and_state_failures_are_warnings_that_never_stop_the_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch)
    stop = threading.Event()
    gateway.status_error = True
    start = _request(tmp_path, 1, "start_manager", profile="cpu")
    health = _request(tmp_path, 2)
    try:
        _publish(broker, start)
        with caplog.at_level(logging.WARNING):
            broker.process_once(stop)
            assert "daemon_manager_check_failed" in caplog.text
            # The job ID comes from the ledger, before any check has succeeded.
            row = _managers(broker.policy)["managers"][0]
            assert (row["job_id"], row["scheduler_state"]) == ("42", None)
            (broker.policy.state / "observations.json").unlink()
            (broker.policy.state / "observations.json").mkdir()
            gateway.status_error = False
            clock[0] += 60
            _publish(broker, health)
            broker.process_once(stop)
        assert _read(broker, health).outcome == "ready"
        assert "daemon_observations_unsaved" in caplog.text
        assert _managers(broker.policy)["managers"][0]["scheduler_state"] == "PENDING"
        assert not [record for record in caplog.records if record.levelno > logging.WARNING]
    finally:
        stack.close()


_TRAVERSING = {
    "job_id": "../../x",
    "cluster": "c",
    "scheduler_state": "FAILED",
    "exit_code": None,
    "started_at": None,
    "ended_at": None,
    "final": True,
    "log_published": False,
    "checked_at": 1.0,
    "unknown_since": None,
}


@pytest.mark.parametrize(
    "content", [b"not json", b'{"x": {"final": true}}', b"[]", json.dumps({"x": _TRAVERSING}).encode()]
)
def test_invalid_observations_read_as_unobserved(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "observations.json"
    path.write_bytes(content)
    assert service_module._load_observations(path) == {}
    assert service_module._load_observations(tmp_path / "absent.json") == {}


def _withdrawing(tmp_path: Path, **limits: int) -> tuple[ExitStack, Broker, Path, Path]:
    """Open a broker whose exchange and staging directories exist; return its staging inbox and withdrawn."""

    policy = _policy(tmp_path, **limits)
    stack, broker, _ = _open(tmp_path, policy)
    staging = policy.workspace / ".httk-workspace/exchange"
    for path in (policy.exchange / "inbox", policy.exchange / "outbox/rejected", policy.exchange / "outbox/withdrawn"):
        path.mkdir(parents=True, exist_ok=True)
    for path in (staging / "inbox", staging / "outbox/rejected"):
        path.mkdir(parents=True, exist_ok=True)
    return stack, broker, staging / "inbox", policy.exchange / "outbox/withdrawn"


def _bundle(path: Path, content: str = "content") -> Path:
    path.mkdir()
    (path / "payload").write_text(content, encoding="utf-8")
    return path


def test_withdraw_moves_all_or_one_waiting_bundle_back_unchanged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    stack, broker, inbox, withdrawn = _withdrawing(tmp_path)
    stop = threading.Event()
    for name in ("job-a", "job-b", "job-c"):
        _bundle(inbox / name, name)
    for name in (".partial", "managers"):
        _bundle(inbox / name)
    one = _request(tmp_path, 1, "withdraw", bundle="job-b")
    every = _request(tmp_path, 2, "withdraw")
    try:
        _publish(broker, one)
        with caplog.at_level(logging.INFO):
            broker.process_once(stop)
        response = _read(broker, one)
        assert (response.outcome, response.detail, response.reason, response.handle) == (
            "withdrawn",
            "job-b",
            None,
            None,
        )
        assert os.listdir(withdrawn) == ["job-b"] and (withdrawn / "job-b/payload").read_text() == "job-b"
        assert "daemon_withdrawn names=job-b" in caplog.text
        _publish(broker, every)
        broker.process_once(stop)
        assert _read(broker, every).detail == "job-a,job-c"
        assert sorted(os.listdir(withdrawn)) == ["job-a", "job-b", "job-c"]
        assert sorted(os.listdir(inbox)) == [".partial", "managers"]

        # A retry with the same request ID replays the recorded response and moves nothing.
        _bundle(inbox / "job-d")
        _publish(broker, every)
        broker.process_once(stop)
        assert _read(broker, every).detail == "job-a,job-c"
        assert (inbox / "job-d").is_dir()
        _publish(broker, nothing := _request(tmp_path, 3, "withdraw", bundle="absent"))
        broker.process_once(stop)
        assert (_read(broker, nothing).outcome, _read(broker, nothing).detail) == ("withdrawn", None)
    finally:
        stack.close()


def test_withdraw_leaves_the_client_inbox_to_the_client(tmp_path: Path) -> None:
    stack, broker, inbox, withdrawn = _withdrawing(tmp_path)
    fresh = _bundle(broker.policy.exchange / "inbox/fresh")
    request = _request(tmp_path, 1, "withdraw")
    try:
        _publish(broker, request)
        broker.process_once(threading.Event())
        assert _read(broker, request).detail is None
        assert os.listdir(withdrawn) == []
        # The same pass then forwards the client's bundle: a later withdraw takes it back.
        assert not fresh.exists() and (inbox / "fresh/payload").is_file()
    finally:
        stack.close()


def test_withdraw_skips_raced_and_taken_names_and_quarantines_a_swapped_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stack, broker, inbox, withdrawn = _withdrawing(tmp_path)
    adopted, taken, swapped = _bundle(inbox / "adopted"), _bundle(inbox / "taken"), _bundle(inbox / "swapped")
    _bundle(withdrawn / "taken", "earlier")
    _bundle(inbox / "kept")
    outside = _bundle(tmp_path / "outside")
    original = exchange_module._rename
    swaps: list[str] = []

    def race(source: int, name: str, target: int, target_name: str) -> None:
        if name == "adopted":  # a manager adopts it first
            (adopted / "payload").unlink()
            adopted.rmdir()
        elif name == "swapped" and not swaps:
            swaps.append(name)
            (swapped / "payload").unlink()
            swapped.rmdir()
            swapped.symlink_to(outside, target_is_directory=True)
        original(source, name, target, target_name)

    monkeypatch.setattr(exchange_module, "_rename", race)
    request = _request(tmp_path, 1, "withdraw")
    try:
        _publish(broker, request)
        with caplog.at_level(logging.INFO):
            broker.process_once(threading.Event())
        assert _read(broker, request).detail == "kept"
        assert (withdrawn / "taken/payload").read_text() == "earlier" and (taken / "payload").is_file()
        assert "daemon_exchange_skipped direction=withdrawn name=taken reason=target_exists" in caplog.text
        assert "daemon_exchange_quarantined direction=withdrawn name=swapped" in caplog.text
        (quarantined,) = [name for name in os.listdir(withdrawn) if name.startswith(".quarantine-")]
        assert (withdrawn / quarantined).is_symlink() and (outside / "payload").is_file()
        assert "daemon_exchange_moved direction=withdrawn name=adopted" not in caplog.text
        assert "daemon_exchange_skipped direction=withdrawn name=adopted" not in caplog.text
    finally:
        stack.close()


def test_withdraw_does_not_count_towards_the_submission_quota(tmp_path: Path) -> None:
    stack, broker, _, _ = _withdrawing(tmp_path, max_submissions=1)
    start, withdraw = _request(tmp_path, 1, "start_manager", profile="cpu"), _request(tmp_path, 2, "withdraw")
    try:
        _publish(broker, start)
        _publish(broker, withdraw)
        broker.process_once(threading.Event())
        assert (_read(broker, start).outcome, _read(broker, withdraw).outcome) == ("submitted", "withdrawn")
    finally:
        stack.close()


def test_withdrawn_detail_is_cut_at_whole_names(tmp_path: Path) -> None:
    stack, broker, inbox, withdrawn = _withdrawing(tmp_path)
    names = [f"{index:03d}" + "x" * 96 for index in range(12)]
    for name in names:
        _bundle(inbox / name)
    request = _request(tmp_path, 1, "withdraw")
    try:
        _publish(broker, request)
        broker.process_once(threading.Event())
        detail = _read(broker, request).detail
        assert detail is not None and len(detail) <= 1000 and detail.endswith(",...")
        assert detail.removesuffix(",...").split(",") == names[:9]
        assert sorted(os.listdir(withdrawn)) == names
    finally:
        stack.close()


def _start(tmp_path: Path, broker: Broker) -> str:
    request = _request(tmp_path, 1, "start_manager", profile="cpu")
    _publish(broker, request)
    broker.process_once(threading.Event())
    handle = _read(broker, request).handle
    assert handle is not None
    return handle


def test_failing_accounting_still_reaches_gone_and_warns_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch, sacct=True)
    gateway.state, gateway.accounting_error = "UNKNOWN", True
    try:
        with caplog.at_level(logging.WARNING):
            handle = _start(tmp_path, broker)
            for _ in range(30):
                clock[0] += 60
                broker.process_once(threading.Event())
        row = _managers(broker.policy)["managers"][0]
        assert (row["scheduler_state"], row["log"]) == ("GONE", f"managers/{handle}.log")
        assert caplog.text.count("daemon_manager_accounting_failed") == 1
        assert "slurmdbd: Connection refused" in caplog.text
        assert "daemon_manager_check_failed" not in caplog.text
    finally:
        stack.close()


def test_a_final_squeue_state_waits_for_the_accounting_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch, sacct=True)
    gateway.state = "FAILED"
    try:
        _start(tmp_path, broker)
        row = _managers(broker.policy)["managers"][0]
        assert (row["scheduler_state"], row["exit_code"], row["log"]) == ("FAILED", None, None)
        gateway.state = "UNKNOWN"  # the job left squeue before accounting caught up
        clock[0] += 60
        broker.process_once(threading.Event())
        assert _managers(broker.policy)["managers"][0]["scheduler_state"] == "FAILED"
        gateway.record = Observation("FAILED", "2:0", None, "2026-10-05T10:01:00")
        clock[0] += 60
        broker.process_once(threading.Event())
        row = _managers(broker.policy)["managers"][0]
        assert (row["scheduler_state"], row["exit_code"], row["ended_at"]) == ("FAILED", "2:0", "2026-10-05T10:01:00")
        assert row["log"] is not None
        clock[0] += 60
        broker.process_once(threading.Event())
        assert len(gateway.accountings) == 3
    finally:
        stack.close()


def test_a_final_squeue_state_without_any_accounting_settles_after_thirty_minutes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch, sacct=True)
    gateway.state = "COMPLETED"
    try:
        _start(tmp_path, broker)
        clock[0] += 29 * 60
        broker.process_once(threading.Event())
        assert _managers(broker.policy)["managers"][0]["log"] is None
        clock[0] += 60
        broker.process_once(threading.Event())
        row = _managers(broker.policy)["managers"][0]
        assert (row["scheduler_state"], row["exit_code"]) == ("COMPLETED", None) and row["log"] is not None
    finally:
        stack.close()


def test_a_final_squeue_state_reads_accounting_at_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stack, broker, gateway, _ = _observing(tmp_path, monkeypatch, sacct=True)
    gateway.state = "COMPLETED"
    gateway.record = Observation("COMPLETED", "0:0", "2026-10-05T10:00:00", "2026-10-05T10:01:00")
    try:
        _start(tmp_path, broker)
        row = _managers(broker.policy)["managers"][0]
        assert (row["scheduler_state"], row["exit_code"]) == ("COMPLETED", "0:0") and row["log"] is not None
        assert len(gateway.accountings) == 1
    finally:
        stack.close()


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_a_non_regular_job_output_is_published_as_a_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    stack, broker, gateway, _ = _observing(tmp_path, monkeypatch)
    source = broker.policy.jobs / "httk-42.out"
    if kind == "symlink":
        secret = tmp_path / "secret"
        secret.write_text("private", encoding="utf-8")
        source.symlink_to(secret)
    else:
        os.mkfifo(source)
    gateway.state = "FAILED"
    try:
        handle = _start(tmp_path, broker)
        published = broker.policy.exchange / "outbox/managers" / f"{handle}.log"
        assert published.read_bytes() == f"the Slurm output at {source} is not a regular file\n".encode()
    finally:
        stack.close()


def test_a_failed_log_publication_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stack, broker, gateway, clock = _observing(tmp_path, monkeypatch)
    (broker.policy.jobs / "httk-42.out").write_bytes(b"output")
    outbox = broker.policy.exchange / "outbox"
    (outbox / "managers").write_text("not a directory", encoding="utf-8")
    gateway.state = "FAILED"
    try:
        handle = _start(tmp_path, broker)
        assert _managers(broker.policy)["managers"][0]["log"] is None
        assert broker._observations[handle]["log_published"] is False
        (outbox / "managers").unlink()
        clock[0] += 60
        broker.process_once(threading.Event())
        assert (outbox / "managers" / f"{handle}.log").read_bytes() == b"output"
        assert _managers(broker.policy)["managers"][0]["log"] == f"managers/{handle}.log"
        assert gateway.statuses == [("42", "cluster-1", handle)]
    finally:
        stack.close()


def test_withdraw_creates_a_missing_withdrawn_directory(tmp_path: Path) -> None:
    stack, broker, inbox, withdrawn = _withdrawing(tmp_path)
    withdrawn.rmdir()  # an enrollment made before the report directories existed
    _bundle(inbox / "job")
    request = _request(tmp_path, 1, "withdraw")
    try:
        _publish(broker, request)
        broker.process_once(threading.Event())
        assert _read(broker, request).detail == "job"
        assert withdrawn.stat().st_mode & 0o777 == 0o700 and (withdrawn / "job/payload").is_file()
    finally:
        stack.close()
