"""Foreground broker for the confined workspace daemon."""

import argparse
import errno
import json
import logging
import os
import re
import signal
import sqlite3
import stat
import sys
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from types import FrameType
from typing import TypedDict, cast

from ._daemon_activation import verify_active_snapshot
from ._daemon_auth import check_request_time, sign_response, verify_request
from ._daemon_exchange import ExchangeMover, _install
from ._daemon_keys import read_response_seed, response_seed_path
from ._daemon_mailbox import MailboxDirectory
from ._daemon_policy import Policy, _open_directory, load_policy
from ._daemon_protocol import Request, Response, decode_request, encode_response, request_digest
from ._daemon_slurm import SchedulerError, SlurmGateway, Submission, excerpt
from ._daemon_state import CapacityError, ConflictError, Entry, Ledger

_LOGGER = logging.getLogger(__name__)
_SNAPSHOT_POLICY = Path("/tmp/daemon-policy.json")
_STATE_DIRECTORY = Path("/tmp/control")
_ROOT_DIRECTORY = Path("/tmp/daemon-root")
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_CLUSTER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_OBSERVATIONS = "observations.json"
#: The least time between two scheduler checks of the submitted managers.
_OBSERVE_SECONDS = 60.0
#: How long a manager may be unknown to both squeue and sacct before it is reported ``GONE``.
_GONE_SECONDS = 30 * 60.0
_MAX_LOG_BYTES = 1024 * 1024
_MAX_OBSERVATIONS_BYTES = 16 * 1024 * 1024
_FINAL_STATES = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
    }
)
_PUBLISHED = ("scheduler_state", "exit_code", "started_at", "ended_at", "log")
_MAX_WARNED = 4096


class _Observation(TypedDict):
    job_id: str
    cluster: str
    scheduler_state: str
    exit_code: str | None
    started_at: str | None
    ended_at: str | None
    final: bool
    log_published: bool
    checked_at: float
    unknown_since: float | None


_OBSERVATION_TYPES: dict[str, tuple[type, ...]] = {
    "job_id": (str,),
    "cluster": (str,),
    "scheduler_state": (str,),
    "exit_code": (str, type(None)),
    "started_at": (str, type(None)),
    "ended_at": (str, type(None)),
    "final": (bool,),
    "log_published": (bool,),
    "checked_at": (int, float),
    "unknown_since": (int, float, type(None)),
}


def _read_tail(path: Path, limit: int) -> bytes:
    """Return at most the last ``limit`` bytes of a regular file, without following or blocking on it.

    A symlink raises ``OSError`` with ``ELOOP``, and any other non-regular file ``ValueError``.
    """

    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("not a regular file")
        os.lseek(descriptor, max(0, information.st_size - limit), os.SEEK_SET)
        data = bytearray()
        while len(data) < limit and (chunk := os.read(descriptor, limit - len(data))):
            data.extend(chunk)
        return bytes(data)
    finally:
        os.close(descriptor)


def _load_observations(path: Path) -> dict[str, _Observation]:
    """Read the derived observations; anything absent or invalid reads as unobserved."""

    try:
        value = json.loads(_read_tail(path, _MAX_OBSERVATIONS_BYTES))
    except (OSError, ValueError, RecursionError):
        return {}
    if not isinstance(value, dict):
        return {}
    return {
        handle: cast(_Observation, entry)
        for handle, entry in value.items()
        if isinstance(entry, dict)
        and set(entry) == set(_OBSERVATION_TYPES)
        and all(type(entry[key]) in types for key, types in _OBSERVATION_TYPES.items())
        and _JOB_ID.fullmatch(entry["job_id"]) is not None  # it names the Slurm output file read for the log
    }


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
    :param state: Protected state directory for the derived ``observations.json``; ``None`` skips following
        manager jobs.
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
        state: Path | None = None,
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
        self.state = state
        self._observations = {} if state is None else _load_observations(state / _OBSERVATIONS)
        self._saved: bytes | None = None
        self._observed_at: float | None = None
        self._warned: set[tuple[str, str]] = set()

    @staticmethod
    def _response(request: Request, outcome: str, **fields: str | None) -> Response:
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
        _LOGGER.info(
            "daemon_request request_id=%s operation=%s outcome=%s reason=%s handle=%s",
            request.request_id,
            request.operation,
            response.outcome,
            response.reason,
            response.handle,
        )
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
    def _scheduler_failed(request: Request, handle: str, exc: SchedulerError) -> str | None:
        """Log one failed scheduler call and return its bounded response detail."""

        detail = excerpt(str(exc), 1000) or None
        _LOGGER.warning(
            "daemon_scheduler_failed request_id=%s operation=%s handle=%s detail=%s",
            request.request_id,
            request.operation,
            handle,
            detail,
        )
        return detail

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
        except SchedulerError as exc:
            response = self._response(
                request,
                "uncertain",
                handle=submitting.handle,
                reason="submission_unconfirmed",
                detail=self._scheduler_failed(request, submitting.handle, exc),
            )
            completed = self.ledger.finish(request.request_id, response)
            return self._finished_response(completed)

        _LOGGER.info(
            "daemon_submitted handle=%s job_id=%s cluster=%s log=%s",
            submitting.handle,
            submission.job_id,
            submission.cluster,
            self.policy.jobs / f"httk-{submission.job_id}.out",
        )
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
            except SchedulerError as exc:
                # A status outcome carries no reason, so the detail is only logged.
                self._scheduler_failed(request, request.handle, exc)
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
            except SchedulerError as exc:
                response = self._response(
                    request,
                    "refused",
                    handle=request.handle,
                    reason="scheduler_unavailable",
                    detail=self._scheduler_failed(request, request.handle, exc),
                )
        completed = self.ledger.finish(request.request_id, response)
        return self._finished_response(completed)

    def _withdraw(self, request: Request) -> Response:
        """Move waiting bundles out of the workspace staging inbox and list them in the response."""

        names = [] if self.exchange is None else self.exchange.withdraw(request.bundle)
        _LOGGER.info("daemon_withdrawn names=%s", ",".join(names))
        detail = ",".join(names)
        if len(detail) > 1000:
            detail = detail[:996].rsplit(",", 1)[0] + ",..."
        return self._response(request, "withdrawn", detail=detail or None)

    def _execute(self, entry: Entry) -> Response:
        request = entry.request
        if request.operation in {"health", "withdraw"}:
            response = self._response(request, "ready") if request.operation == "health" else self._withdraw(request)
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
                rows = self.ledger.managers()
                try:
                    self._observe(rows, time.time())
                except Exception as exc:
                    _LOGGER.warning("daemon_manager_observation_failed reason=%s", excerpt(str(exc)))
                self.exchange.poll(self._manager_rows(rows))
            except Exception:
                _LOGGER.exception("daemon_exchange_failed")
        return processed

    def _manager_rows(self, rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
        """Extend ledger manager rows, which carry the job ID, with what the scheduler last reported."""

        result: list[dict[str, str | None]] = []
        for row in rows:
            observation = self._observations.get(str(row["handle"]))
            if observation is None:
                result.append({**row, **dict.fromkeys(_PUBLISHED)})
                continue
            result.append(
                {
                    **row,
                    "scheduler_state": observation["scheduler_state"],
                    "exit_code": observation["exit_code"],
                    "started_at": observation["started_at"],
                    "ended_at": observation["ended_at"],
                    "log": f"managers/{row['handle']}.log" if observation["log_published"] else None,
                }
            )
        return result

    def _observe(self, rows: list[dict[str, str | None]], now: float) -> None:
        """Follow each submitted manager job until it ends, then publish its Slurm output once.

        Runs at most once per 60 seconds; a failed check is logged and retried on the next run.
        """

        if self.state is None or self.exchange is None:
            return
        if self._observed_at is not None and 0.0 <= now - self._observed_at < _OBSERVE_SECONDS:
            return
        self._observed_at = now
        for row in rows:
            handle = str(row["handle"])
            observation = self._observations.get(handle)
            if row["state"] == "submitted" and (observation is None or not observation["final"]):
                entry = self.ledger.lookup_manager(handle)
                if entry is None or entry.job_id is None or entry.cluster is None:
                    raise sqlite3.DatabaseError("submitted manager has no scheduler identity")
                try:
                    observation = self._check(handle, entry.job_id, entry.cluster, observation, now)
                except SchedulerError as exc:
                    _LOGGER.warning(
                        "daemon_manager_check_failed handle=%s job_id=%s detail=%s",
                        handle,
                        entry.job_id,
                        excerpt(str(exc), 1000),
                    )
                    continue
                self._observations[handle] = observation
            if observation is not None and observation["final"] and not observation["log_published"]:
                self._publish_log(handle, observation)
        data = json.dumps(self._observations, sort_keys=True, separators=(",", ":")).encode("ascii")
        if data == self._saved:
            return
        try:
            directory = _open_directory(self.state)
            try:
                _install(directory, _OBSERVATIONS, ".observations-", data)
            finally:
                os.close(directory)
        except OSError as exc:
            _LOGGER.warning("daemon_observations_unsaved reason=%s", exc.strerror)
            return
        self._saved = data

    def _check(self, handle: str, job_id: str, cluster: str, previous: _Observation | None, now: float) -> _Observation:
        """Ask squeue, then sacct once squeue no longer knows the job or reports it ended."""

        state = self.gateway.status(job_id, cluster, handle)
        record = None
        if state == "UNKNOWN" or state in _FINAL_STATES:
            try:
                record = self.gateway.accounting(job_id, cluster, handle)
            except SchedulerError as exc:
                # Treated as no record, so the GONE timer still runs while slurmdbd is down.
                detail = excerpt(str(exc), 1000)
                if (handle, detail) not in self._warned:
                    # ponytail: one line per handle and error until 4096 pairs, then the memory resets.
                    if len(self._warned) >= _MAX_WARNED:
                        self._warned.clear()
                    self._warned.add((handle, detail))
                    _LOGGER.warning(
                        "daemon_manager_accounting_failed handle=%s job_id=%s detail=%s", handle, job_id, detail
                    )
        exit_code, started_at, ended_at = (
            (None, None, None)
            if previous is None
            else (previous["exit_code"], previous["started_at"], previous["ended_at"])
        )
        since = None
        if record is not None:
            state, exit_code, started_at, ended_at = (
                record.scheduler_state,
                record.exit_code,
                record.started_at,
                record.ended_at,
            )
            final = state in _FINAL_STATES
        else:
            if state == "UNKNOWN" and previous is not None and previous["scheduler_state"] in _FINAL_STATES:
                state = previous["scheduler_state"]  # squeue saw it end; accounting has not caught up yet
            # Without sacct a final squeue state is all there will be; with it, wait for the exit code.
            final = state in _FINAL_STATES and self.policy.sacct is None
            if state == "UNKNOWN" or not final and state in _FINAL_STATES:
                since = now if previous is None or previous["unknown_since"] is None else previous["unknown_since"]
                if now - since >= _GONE_SECONDS:
                    state, final = "GONE" if state == "UNKNOWN" else state, True
        observation: _Observation = {
            "job_id": job_id,
            "cluster": cluster,
            "scheduler_state": state,
            "exit_code": exit_code,
            "started_at": started_at,
            "ended_at": ended_at,
            "final": final,
            "log_published": False,
            "checked_at": now,
            "unknown_since": since,
        }
        if previous is None or (previous["scheduler_state"], previous["exit_code"]) != (
            observation["scheduler_state"],
            observation["exit_code"],
        ):
            _LOGGER.info(
                "daemon_manager_state handle=%s job_id=%s state=%s exit_code=%s",
                handle,
                job_id,
                observation["scheduler_state"],
                observation["exit_code"],
            )
        return observation

    def _publish_log(self, handle: str, observation: _Observation) -> None:
        """Publish the tail of a finished manager's private Slurm output into the exchange outbox."""

        assert self.exchange is not None
        source = self.policy.jobs / f"httk-{observation['job_id']}.out"
        try:
            data = _read_tail(source, _MAX_LOG_BYTES)
        except FileNotFoundError:
            data = os.fsencode(f"no Slurm output was found at {source}\n")
        except ValueError:
            data = os.fsencode(f"the Slurm output at {source} is not a regular file\n")
        except OSError as exc:
            if exc.errno != errno.ELOOP:
                _LOGGER.warning("daemon_manager_log_unread handle=%s reason=%s", handle, exc.strerror)
                return
            data = os.fsencode(f"the Slurm output at {source} is not a regular file\n")
        if self.exchange.publish_log(handle, data):
            observation["log_published"] = True
            _LOGGER.info("daemon_manager_log handle=%s path=outbox/managers/%s.log", handle, handle)

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
        raise ValueError(f"service policy must be {_SNAPSHOT_POLICY}")
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
            try:
                gateway.check()
            except SchedulerError as exc:
                # Only the clients' own --version output: safe and needed to diagnose the operator's setup.
                raise ValueError(f"scheduler client check failed: {exc}") from exc
            _LOGGER.info("daemon_check_passed workspace=%s cluster=%s", policy.workspace_id, policy.cluster)
            exchange = _ROOT_DIRECTORY / policy.exchange.name
            requests = stack.enter_context(MailboxDirectory(exchange / "requests"))
            responses = stack.enter_context(MailboxDirectory(exchange / "responses"))
            if arguments.check:
                return
            mover = ExchangeMover(_ROOT_DIRECTORY, policy.exchange.name, policy.workspace.name, policy.enrollment_id)
            ledger.recover()
            broker = Broker(
                policy, gateway, ledger, requests, responses, exchange=mover, response_seed=seed, state=_STATE_DIRECTORY
            )
            _LOGGER.info(
                "daemon_started workspace=%s enrollment=%s launchers=%s",
                policy.workspace_id,
                policy.enrollment_id,
                ",".join(profile.name for profile in policy.profiles),
            )
            broker.run(stop, once=arguments.once)
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


def main(argv: list[str] | None = None) -> int:
    """Run the fixed daemon service command, logging to stdout, and return a process status."""

    # The service entry point owns the process; library modules stay handler-free.
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        arguments = _parser().parse_args(argv)
        _run(arguments)
    except (OSError, ValueError, sqlite3.DatabaseError, SchedulerError) as exc:
        _LOGGER.error("daemon_service_failed reason=%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["Broker", "main"]
