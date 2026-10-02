"""Foreground daemon broker behavior and ordering tests."""

import base64
import logging
import sqlite3
import threading
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path

import pytest
from httk.core.identity import identity_public_key

from httk.workflow import _daemon_service as service_module
from httk.workflow._daemon_auth import sign_request, verify_response
from httk.workflow._daemon_keys import initialize_response_seed, response_public_key, response_seed_path
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import Policy, Profile
from httk.workflow._daemon_protocol import Request, Response, decode_response, encode_request
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import SchedulerError, SlurmGateway, Submission
from httk.workflow._daemon_state import Ledger

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"


class RecordingGateway(SlurmGateway):
    """Record scheduler calls while retaining the concrete gateway API."""

    def __init__(self, policy: Policy) -> None:
        self.policy = policy
        self.policy_path = Path("/policy.json")
        self.submissions: list[tuple[Profile, str]] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.cancellations: list[tuple[str, str, str]] = []
        self.submit_error = False
        self.status_error = False
        self.cancel_error = False
        self.stop: threading.Event | None = None

    def check(self) -> None:
        pass

    def submit(self, profile: Profile, handle: str) -> Submission:
        self.submissions.append((profile, handle))
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
        workspace=tmp_path / "workspace",
        workspace_id=WORKSPACE_ID,
        enrollment_id=ENROLLMENT_ID,
        requests=tmp_path / "requests",
        responses=tmp_path / "responses",
        state=tmp_path / "state",
        bwrap=runtime / "bwrap",
        python=runtime / "python",
        sbatch=broker / "sbatch",
        squeue=broker / "squeue",
        scancel=broker / "scancel",
        cluster="cluster-1",
        readonly_paths=(runtime,),
        broker_paths=(broker,),
        profiles=(Profile("cpu", 2, 1024, 10),),
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
) -> Request:
    request = Request(
        f"{number:032x}",
        workspace_id,
        operation,
        profile=profile,
        handle=handle,
        enrollment_id=enrollment_id,
    )
    return sign_request(
        request,
        seed_path=_operator_seed(tmp_path) if seed is None else seed,
        now=now,
        lifetime=lifetime,
    )


def _open(tmp_path: Path, policy: Policy, *, initialize: bool = True) -> tuple[ExitStack, Broker, Ledger]:
    for path in (policy.requests, policy.responses, policy.state):
        path.mkdir(exist_ok=True)
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
        response_seed=response_seed_path(policy.state),
    )
    return stack, broker, ledger


def _publish(broker: Broker, request: Request) -> str:
    name = f"{request.request_id}.json"
    broker.requests.replace(name, encode_request(request))
    return name


def _read(broker: Broker, request: Request) -> Response:
    response = decode_response(broker.responses.read(f"{request.request_id}.json"))
    verify_response(response, response_public_key(broker.response_seed))
    return response


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
        assert gateway.submissions[0][0] == policy.profile("cpu")

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


def test_refusals_scheduler_failures_conflicts_and_capacity_are_signed(tmp_path: Path) -> None:
    policy = _policy(tmp_path, max_records=3, max_submissions=2)
    stack, broker, ledger = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        invalid = _request(tmp_path, 1, "start_manager", profile="missing")
        _publish(broker, invalid)
        broker.process_once(threading.Event())
        assert (_read(broker, invalid).outcome, _read(broker, invalid).reason) == ("refused", "invalid_profile")

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
        assert _read(broker, status).scheduler_state == "UNKNOWN"

        gateway.cancel_error = True
        cancel = _request(tmp_path, 4, "cancel_manager", handle=handle)
        _publish(broker, cancel)
        broker.process_once(threading.Event())
        assert (_read(broker, cancel).outcome, _read(broker, cancel).reason) == (
            "refused",
            "scheduler_unavailable",
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
            Broker(policy, RecordingGateway(policy), ledger, broker.requests, broker.responses, response_seed=seed)
        seed.write_text("bad\n", encoding="ascii")
        seed.chmod(0o600)
        with pytest.raises(ValueError, match="canonical base64"):
            Broker(policy, RecordingGateway(policy), ledger, broker.requests, broker.responses, response_seed=seed)
        assert ledger.lookup_request(f"{1:032x}") is None
    finally:
        stack.close()


def test_main_reports_state_recovery_guidance_but_hides_scheduler_details(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    message = "preserve this state, reconcile outstanding work, and initialize a new enrollment"
    monkeypatch.setattr(
        service_module, "_run", lambda _arguments: (_ for _ in ()).throw(sqlite3.DatabaseError(message))
    )
    with caplog.at_level(logging.ERROR):
        assert (
            service_module.main(
                ["--policy", "/daemon-policy.json", "--policy-source", "/protected-policy.json", "--once"]
            )
            == 1
        )
    assert message in caplog.text

    caplog.clear()
    secret = "scheduler raw output must stay hidden"
    monkeypatch.setattr(service_module, "_run", lambda _arguments: (_ for _ in ()).throw(SchedulerError(secret)))
    with caplog.at_level(logging.ERROR):
        assert (
            service_module.main(
                ["--policy", "/daemon-policy.json", "--policy-source", "/protected-policy.json", "--once"]
            )
            == 1
        )
    assert "daemon_service_failed" in caplog.text
    assert secret not in caplog.text


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
