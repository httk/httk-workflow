"""Foreground daemon broker behavior and ordering tests."""

import threading
from contextlib import ExitStack
from pathlib import Path

import pytest

from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import Policy, Profile
from httk.workflow._daemon_protocol import Request, Response, decode_response, encode_request
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import SchedulerError, SlurmGateway, Submission
from httk.workflow._daemon_state import Ledger

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"


class RecordingGateway(SlurmGateway):
    """Record fixed gateway calls while retaining the concrete gateway API."""

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


def _policy(tmp_path: Path, *, max_records: int = 64, max_submissions: int = 8) -> Policy:
    runtime = tmp_path / "runtime"
    broker = tmp_path / "broker"
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
        max_records=max_records,
        max_submissions=max_submissions,
        poll_seconds=0.05,
    )


def _request(number: int, operation: str = "health", **fields: str) -> Request:
    return Request(
        f"{number:032x}",
        WORKSPACE_ID,
        operation,
        enrollment_id=ENROLLMENT_ID,
        **fields,
    )


def _open(tmp_path: Path, policy: Policy, *, initialize: bool = True) -> tuple[ExitStack, Broker, Ledger]:
    for path in (policy.requests, policy.responses, policy.state):
        path.mkdir(exist_ok=True)
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
    return stack, Broker(policy, RecordingGateway(policy), ledger, requests, responses), ledger


def _publish(broker: Broker, request: Request) -> str:
    name = f"{request.request_id}.json"
    broker.requests.replace(name, encode_request(request))
    return name


def _read(broker: Broker, request: Request) -> Response:
    return decode_response(broker.responses.read(f"{request.request_id}.json"))


def test_health_start_status_and_cancel_use_only_protected_manager_identity(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        health = _request(1)
        _publish(broker, health)
        assert broker.process_once(threading.Event()) == 1
        assert _read(broker, health).outcome == "ready"

        start = _request(2, "start_manager", profile="cpu")
        _publish(broker, start)
        broker.process_once(threading.Event())
        submitted = _read(broker, start)
        assert submitted.outcome == "submitted" and submitted.handle is not None
        assert gateway.submissions[0][0] == policy.profile("cpu")

        status = _request(3, "manager_status", handle=submitted.handle)
        _publish(broker, status)
        broker.process_once(threading.Event())
        status_response = _read(broker, status)
        assert (status_response.outcome, status_response.scheduler_state) == ("status", "RUNNING")
        assert gateway.statuses == [("42", policy.cluster, submitted.handle)]

        cancel = _request(4, "cancel_manager", handle=submitted.handle)
        _publish(broker, cancel)
        broker.process_once(threading.Event())
        assert _read(broker, cancel).outcome == "cancel_requested"
        assert gateway.cancellations == [("42", policy.cluster, submitted.handle)]
    finally:
        stack.close()


def test_invalid_profile_and_scheduler_failures_are_durable(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        invalid = _request(1, "start_manager", profile="missing")
        _publish(broker, invalid)
        broker.process_once(threading.Event())
        invalid_response = _read(broker, invalid)
        assert (invalid_response.outcome, invalid_response.reason) == ("refused", "invalid_profile")
        assert not gateway.submissions

        gateway.submit_error = True
        uncertain = _request(2, "start_manager", profile="cpu")
        _publish(broker, uncertain)
        broker.process_once(threading.Event())
        uncertain_response = _read(broker, uncertain)
        assert (uncertain_response.outcome, uncertain_response.reason) == (
            "uncertain",
            "submission_unconfirmed",
        )
        _publish(broker, uncertain)
        broker.process_once(threading.Event())
        assert len(gateway.submissions) == 1
    finally:
        stack.close()


def test_status_and_cancel_failure_contracts_and_unknown_handle(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    try:
        unknown = _request(1, "manager_status", handle="f" * 32)
        _publish(broker, unknown)
        broker.process_once(threading.Event())
        assert (_read(broker, unknown).outcome, _read(broker, unknown).reason) == ("refused", "unknown_manager")

        start = _request(2, "start_manager", profile="cpu")
        _publish(broker, start)
        broker.process_once(threading.Event())
        handle = _read(broker, start).handle
        assert handle is not None

        gateway.status_error = True
        status = _request(3, "manager_status", handle=handle)
        _publish(broker, status)
        broker.process_once(threading.Event())
        assert _read(broker, status).scheduler_state == "UNKNOWN"

        gateway.cancel_error = True
        cancel = _request(4, "cancel_manager", handle=handle)
        _publish(broker, cancel)
        broker.process_once(threading.Event())
        refused = _read(broker, cancel)
        assert (refused.outcome, refused.reason) == ("refused", "scheduler_unavailable")
    finally:
        stack.close()


def test_wrong_identity_conflict_capacity_and_malformed_inputs(tmp_path: Path) -> None:
    policy = _policy(tmp_path, max_records=1, max_submissions=1)
    stack, broker, ledger = _open(tmp_path, policy)
    try:
        malformed_name = "0" * 32 + ".json"
        broker.requests.replace(malformed_name, b"{}")
        broker.process_once(threading.Event())
        with pytest.raises(FileNotFoundError):
            broker.requests.read(malformed_name)

        wrong_workspace = Request(
            f"{1:032x}",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "health",
            enrollment_id=ENROLLMENT_ID,
        )
        _publish(broker, wrong_workspace)
        broker.process_once(threading.Event())
        assert _read(broker, wrong_workspace).reason == "wrong_workspace"

        wrong_enrollment = Request(
            f"{2:032x}",
            WORKSPACE_ID,
            "health",
            enrollment_id="f" * 32,
        )
        _publish(broker, wrong_enrollment)
        broker.process_once(threading.Event())
        assert _read(broker, wrong_enrollment).reason == "wrong_enrollment"

        admitted = _request(3)
        _publish(broker, admitted)
        broker.process_once(threading.Event())
        assert _read(broker, admitted).outcome == "ready"

        conflict = Request(
            admitted.request_id,
            WORKSPACE_ID,
            "start_manager",
            profile="cpu",
            enrollment_id=ENROLLMENT_ID,
        )
        _publish(broker, conflict)
        broker.process_once(threading.Event())
        assert _read(broker, conflict).reason == "request_conflict"
        assert ledger.admit(admitted).response is not None

        capacity = _request(4)
        _publish(broker, capacity)
        broker.process_once(threading.Event())
        busy = _read(broker, capacity)
        assert (busy.outcome, busy.reason) == ("busy", "capacity")
    finally:
        stack.close()


def test_ledger_commit_precedes_response_and_publication_failure_replays_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _policy(tmp_path)
    stack, broker, ledger = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    request = _request(1, "start_manager", profile="cpu")
    publication = _publish(broker, request)
    original_replace = broker.responses.replace
    calls = 0

    def fail_once(name: str, data: bytes) -> None:
        nonlocal calls
        calls += 1
        assert ledger.admit(request).response is not None
        if calls == 1:
            raise OSError("injected response failure")
        original_replace(name, data)

    monkeypatch.setattr(broker.responses, "replace", fail_once)
    with pytest.raises(OSError, match="response failure"):
        broker.process_once(threading.Event())
    assert broker.requests.read(publication) == encode_request(request)
    assert len(gateway.submissions) == 1
    stack.close()

    restart_stack, restarted, _ = _open(tmp_path, policy, initialize=False)
    try:
        restarted.process_once(threading.Event())
        assert _read(restarted, request).outcome == "submitted"
        restarted_gateway = restarted.gateway
        assert isinstance(restarted_gateway, RecordingGateway)
        assert not restarted_gateway.submissions
        with pytest.raises(FileNotFoundError):
            restarted.requests.read(publication)
    finally:
        restart_stack.close()


def test_stop_is_checked_between_sorted_requests(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    stack, broker, _ = _open(tmp_path, policy)
    gateway = broker.gateway
    assert isinstance(gateway, RecordingGateway)
    stop = threading.Event()
    gateway.stop = stop
    first = _request(1, "start_manager", profile="cpu")
    second = _request(2)
    try:
        _publish(broker, second)
        _publish(broker, first)
        assert broker.process_once(stop) == 1
        assert _read(broker, first).outcome == "submitted"
        assert broker.requests.read(f"{second.request_id}.json") == encode_request(second)
    finally:
        stack.close()
