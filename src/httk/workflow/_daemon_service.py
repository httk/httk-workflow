"""Foreground broker for the confined workspace daemon."""

import argparse
import logging
import re
import signal
import sqlite3
import threading
from contextlib import ExitStack
from pathlib import Path
from types import FrameType

from ._daemon_activation import verify_active_snapshot
from ._daemon_auth import check_request_time, sign_response, verify_request
from ._daemon_exchange import ExchangeMover
from ._daemon_keys import read_response_seed, response_seed_path
from ._daemon_mailbox import MailboxDirectory
from ._daemon_policy import Policy, load_policy
from ._daemon_protocol import Request, Response, decode_request, encode_response, request_digest
from ._daemon_slurm import SchedulerError, SlurmGateway, Submission
from ._daemon_state import CapacityError, ConflictError, Entry, Ledger

_LOGGER = logging.getLogger(__name__)
_SNAPSHOT_POLICY = Path("/daemon-policy.json")
_STATE_DIRECTORY = Path("/control")
_ROOT_DIRECTORY = Path("/daemon-root")
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_CLUSTER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")


class Broker:
    """Process bounded mailbox requests against one protected ledger.

    :param policy: Validated protected daemon policy.
    :param gateway: Fixed Slurm gateway for scheduler operations.
    :param ledger: Locked durable daemon ledger.
    :param requests: Descriptor-anchored request mailbox.
    :param responses: Descriptor-anchored response mailbox.
    :param exchange: Mover between the client exchange and the workspace staging area; ``None`` skips
        the exchange pass.
    :param response_seed: Validated protected response-signing seed.
    """

    def __init__(
        self,
        policy: Policy,
        gateway: SlurmGateway,
        ledger: Ledger,
        requests: MailboxDirectory,
        responses: MailboxDirectory,
        *,
        exchange: ExchangeMover | None = None,
        response_seed: Path,
    ) -> None:
        if not isinstance(response_seed, Path):
            raise ValueError("response_seed must be a Path")
        read_response_seed(response_seed)
        self.policy = policy
        self.gateway = gateway
        self.ledger = ledger
        self.requests = requests
        self.responses = responses
        self.exchange = exchange
        self.response_seed = response_seed

    @staticmethod
    def _response(request: Request, outcome: str, **fields: str) -> Response:
        return Response(
            request.request_id,
            request.workspace_id,
            request.enrollment_id,
            request_digest(request),
            outcome,
            **fields,
        )

    def _publish(self, publication: str, request: Request, response: Response) -> None:
        """Publish a response after state commit, then consume the request."""

        signed = sign_response(response, seed_path=self.response_seed)
        self.responses.replace(f"{request.request_id}.json", encode_response(signed))
        try:
            self.requests.remove(publication)
        except FileNotFoundError:
            pass

    def _discard_invalid(self, publication: str, code: str) -> None:
        _LOGGER.warning("daemon_request_rejected code=%s publication=%s", code, publication)
        try:
            self.requests.remove(publication)
        except (FileNotFoundError, IsADirectoryError, PermissionError):
            pass

    def _finish_refused(self, entry: Entry, reason: str) -> Response:
        request = entry.request
        handle = entry.handle if request.operation == "start_manager" else request.handle
        response = self._response(request, "refused", reason=reason, **({"handle": handle} if handle else {}))
        return self._finished_response(self.ledger.finish(request.request_id, response))

    @staticmethod
    def _finished_response(entry: Entry) -> Response:
        if entry.response is None:
            raise sqlite3.DatabaseError("finished request has no response")
        return entry.response

    def _start(self, entry: Entry) -> Response:
        request = entry.request
        if entry.handle is None or request.profile is None:
            raise sqlite3.DatabaseError("invalid admitted manager start")
        try:
            profile = self.policy.profile(request.profile)
        except ValueError:
            return self._finish_refused(entry, "invalid_configuration")
        if request.configuration_digest != self.policy.configuration_digest(request.profile):
            return self._finish_refused(entry, "stale_configuration")

        try:
            check_request_time(request, max_age=self.policy.request_max_age)
        except ValueError:
            return self._finish_refused(entry, "request_expired")

        submitting = self.ledger.begin_submission(request.request_id)
        if submitting.handle is None:
            raise sqlite3.DatabaseError("submission intent has no manager handle")
        try:
            submission = self.gateway.submit(profile, submitting.handle)
            if (
                type(submission) is not Submission
                or _JOB_ID.fullmatch(submission.job_id) is None
                or _CLUSTER.fullmatch(submission.cluster) is None
                or submission.cluster != self.policy.cluster
            ):
                raise SchedulerError("invalid protected submission identity")
        except SchedulerError:
            response = self._response(
                request,
                "uncertain",
                handle=submitting.handle,
                reason="submission_unconfirmed",
            )
            completed = self.ledger.finish(request.request_id, response)
            return self._finished_response(completed)

        response = self._response(request, "submitted", handle=submitting.handle)
        completed = self.ledger.finish(
            request.request_id,
            response,
            job_id=submission.job_id,
            cluster=submission.cluster,
        )
        return self._finished_response(completed)

    def _manager(self, entry: Entry) -> Response:
        request = entry.request
        if request.handle is None:
            raise sqlite3.DatabaseError("manager request has no handle")
        manager = self.ledger.lookup_manager(request.handle)
        if manager is None or manager.state != "submitted" or manager.job_id is None or manager.cluster is None:
            return self._finish_refused(entry, "unknown_manager")
        if manager.cluster != self.policy.cluster:
            raise sqlite3.DatabaseError("stored scheduler cluster does not match policy")

        try:
            check_request_time(request, max_age=self.policy.request_max_age)
        except ValueError:
            return self._finish_refused(entry, "request_expired")

        if request.operation == "manager_status":
            try:
                scheduler_state = self.gateway.status(manager.job_id, manager.cluster, request.handle)
                response = self._response(
                    request,
                    "status",
                    handle=request.handle,
                    scheduler_state=scheduler_state,
                )
            except SchedulerError:
                response = self._response(
                    request,
                    "status",
                    handle=request.handle,
                    scheduler_state="UNKNOWN",
                )
        else:
            try:
                self.gateway.cancel(manager.job_id, manager.cluster, request.handle)
                response = self._response(request, "cancel_requested", handle=request.handle)
            except SchedulerError:
                response = self._response(
                    request,
                    "refused",
                    handle=request.handle,
                    reason="scheduler_unavailable",
                )
        completed = self.ledger.finish(request.request_id, response)
        return self._finished_response(completed)

    def _execute(self, entry: Entry) -> Response:
        request = entry.request
        if request.operation == "health":
            response = self._response(request, "ready")
            completed = self.ledger.finish(request.request_id, response)
            return self._finished_response(completed)
        if request.operation == "start_manager":
            return self._start(entry)
        return self._manager(entry)

    def _process(self, publication: str, request: Request) -> None:
        try:
            verify_request(request, self.policy.authorized_keys)
        except ValueError:
            self._discard_invalid(publication, "request_unauthorized")
            return

        existing = self.ledger.lookup_request(request.request_id)
        if existing is not None and request_digest(existing.request) != request_digest(request):
            response = self._response(request, "refused", reason="request_conflict")
            self._publish(publication, request, response)
            return
        if request.workspace_id != self.policy.workspace_id:
            response = self._response(request, "refused", reason="wrong_workspace")
            self._publish(publication, request, response)
            return
        if request.enrollment_id != self.policy.enrollment_id:
            response = self._response(request, "refused", reason="wrong_enrollment")
            self._publish(publication, request, response)
            return
        if existing is None:
            try:
                entry = self.ledger.admit(request)
            except ConflictError:
                response = self._response(request, "refused", reason="request_conflict")
                self._publish(publication, request, response)
                return
            except CapacityError:
                response = self._response(request, "busy", reason="capacity")
                self._publish(publication, request, response)
                return
        else:
            entry = existing

        if entry.response is not None:
            response = entry.response
        elif entry.state == "received":
            try:
                check_request_time(request, max_age=self.policy.request_max_age)
            except ValueError:
                response = self._finish_refused(entry, "request_expired")
            else:
                response = self._execute(entry)
        elif entry.state == "submitting":
            # Normal startup calls recover(), but this keeps direct broker use
            # from ever turning a committed intent into another submission.
            self.ledger.recover()
            recovered = self.ledger.admit(request)
            if recovered.response is None:
                raise sqlite3.DatabaseError("submission recovery did not produce a response")
            response = recovered.response
        else:
            raise sqlite3.DatabaseError("durable request has no response")
        self._publish(publication, request, response)

    def process_once(self, stop: threading.Event) -> int:
        """Process one bounded sorted mailbox snapshot, then run one exchange pass.

        :param stop: Stop admission before the next request when set.
        :return: Number of request publications considered.
        """

        processed = 0
        for publication in self.requests.names():
            if stop.is_set():
                break
            processed += 1
            try:
                data = self.requests.read(publication)
            except FileNotFoundError:
                continue
            except (ValueError, OSError):
                self._discard_invalid(publication, "invalid_publication")
                continue
            try:
                request = decode_request(data)
            except ValueError:
                self._discard_invalid(publication, "invalid_document")
                continue
            if publication != f"{request.request_id}.json":
                self._discard_invalid(publication, "identifier_mismatch")
                continue
            self._process(publication, request)
        if self.exchange is not None:
            try:
                self.exchange.poll(self.ledger.managers())
            except Exception:
                _LOGGER.exception("daemon_exchange_failed")
        return processed

    def run(self, stop: threading.Event, *, once: bool = False) -> None:
        """Run bounded scans until stopped, or one scan when requested."""

        while not stop.is_set():
            self.process_once(stop)
            if once or stop.wait(self.policy.poll_seconds):
                return


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-workspace-daemon-service")
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--policy-source", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--once", action="store_true")
    return parser


def _run(arguments: argparse.Namespace) -> None:
    if arguments.policy != _SNAPSHOT_POLICY:
        raise ValueError("service policy must be /daemon-policy.json")
    policy_source: Path = arguments.policy_source
    if not policy_source.is_absolute() or ".." in policy_source.parts:
        raise ValueError("policy source must be an absolute path without '..'")
    policy = load_policy(arguments.policy)
    gateway = SlurmGateway(policy, policy_source)

    seed = response_seed_path(_STATE_DIRECTORY)
    try:
        read_response_seed(seed)
    except (OSError, ValueError) as exc:
        raise ValueError(
            "daemon response seed is missing or invalid; preserve protected state and reconcile initialization"
        ) from exc

    stop = threading.Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop.set()

    previous_term = signal.signal(signal.SIGTERM, request_stop)
    previous_int = signal.signal(signal.SIGINT, request_stop)
    try:
        with ExitStack() as stack:
            ledger = stack.enter_context(
                Ledger(
                    _STATE_DIRECTORY,
                    policy.workspace_id,
                    policy.enrollment_id,
                    max_records=policy.max_records,
                    max_submissions=policy.max_submissions,
                )
            )
            verify_active_snapshot(_STATE_DIRECTORY, policy_source, policy)
            gateway.check()
            exchange = _ROOT_DIRECTORY / policy.exchange.name
            requests = stack.enter_context(MailboxDirectory(exchange / "requests"))
            responses = stack.enter_context(MailboxDirectory(exchange / "responses"))
            if arguments.check:
                return
            mover = ExchangeMover(_ROOT_DIRECTORY, policy.exchange.name, policy.workspace.name, policy.enrollment_id)
            ledger.recover()
            broker = Broker(policy, gateway, ledger, requests, responses, exchange=mover, response_seed=seed)
            broker.run(stop, once=arguments.once)
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


def main(argv: list[str] | None = None) -> int:
    """Run the fixed daemon service command and return a process status."""

    try:
        arguments = _parser().parse_args(argv)
        _run(arguments)
    except SchedulerError:
        _LOGGER.error("daemon_service_failed")
        return 1
    except (OSError, ValueError, sqlite3.DatabaseError) as exc:
        _LOGGER.error("daemon_service_failed reason=%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["Broker", "main"]
