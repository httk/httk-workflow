"""Foreground broker for the confined workspace daemon."""

import argparse
import errno
import json
import logging
import os
import re
import signal
import stat
import sys
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import TypedDict, cast

from ._daemon_activation import verify_active_snapshot
from ._daemon_auth import CLOCK_SKEW_SECONDS, check_request_time, sign_response, verify_request
from ._daemon_keys import read_response_seed, response_seed_path
from ._daemon_mailbox import MAX_DIRECTORY_ENTRIES, MAX_SCANNED_ENTRIES, MailboxDirectory
from ._daemon_policy import Policy, _open_directory, load_policy
from ._daemon_protocol import Request, Response, decode_request, encode_response, request_digest
from ._daemon_slurm import KILL_GRACE_SECONDS, SchedulerError, SlurmGateway, Submission, excerpt
from ._daemon_state import CapacityError, ConflictError, Entry, Ledger, LedgerError, refusal_response
from ._exchange import ExchangeUnavailableError, publish_file

_LOGGER = logging.getLogger(__name__)
_SNAPSHOT_POLICY = Path("/tmp/daemon-policy.json")
_STATE_DIRECTORY = Path("/tmp/control")
_EXCHANGE_DIRECTORY = Path("/tmp/daemon-exchange")
_HANDLE = re.compile(r"[0-9a-f]{32}\Z")
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_CLUSTER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
#: The least time between two scheduler checks of the submitted managers.
_OBSERVE_SECONDS = 60.0
#: How long a manager may be unknown to both squeue and sacct before it is reported ``GONE``.
_GONE_SECONDS = 30 * 60.0
#: Added to the longest a scheduler call can take (``command_timeout`` plus the kill grace of
#: :func:`httk.workflow._daemon_slurm._run`) before an unanswered submit decision counts as unconfirmed.
_RECOVERY_MARGIN_SECONDS = 60.0
_MAX_LOG_BYTES = 1024 * 1024
_PUBLISHED = ("scheduler_state", "exit_code", "started_at", "ended_at", "log")
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
_MAX_WARNED = 4096
_MAX_REPORTED = 4096
_MANAGERS_FORMAT = "httk-workspace-daemon-managers"
_MANAGERS_VERSION = 3


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


def _parse_observation(data: bytes | None) -> _Observation | None:
    """Validate one stored scheduler observation; anything absent or invalid reads as unobserved."""

    if data is None:
        return None
    try:
        entry = json.loads(data)
    except (ValueError, RecursionError):
        return None
    if (
        not isinstance(entry, dict)
        or set(entry) != set(_OBSERVATION_TYPES)
        or not all(type(entry[key]) in types for key, types in _OBSERVATION_TYPES.items())
        or _JOB_ID.fullmatch(entry["job_id"]) is None  # it names the Slurm output file read for the log
    ):
        return None
    return cast(_Observation, entry)


class ExchangePublisher:
    """Publish ``managers.json`` and the manager logs into the workspace exchange.

    Every call reopens the exchange from its path without following a symlink and installs through the exchange
    module's descriptor-anchored primitive (an exclusive temporary and a rename), so a name the client planted is
    replaced, never followed. Nothing in the exchange is read back, and a failure is logged once per name and never
    raised: the exchange is hostile territory and the daemon's requests do not depend on it.

    :param exchange: Absolute path of the exchange directory, as the broker sandbox sees it.
    :param enrollment_id: Enrollment identity published in ``managers.json``.
    :raises ValueError: If ``exchange`` is not absolute or contains ``..``.
    """

    def __init__(self, exchange: Path, enrollment_id: str) -> None:
        if not isinstance(exchange, Path) or not exchange.is_absolute() or ".." in exchange.parts:
            raise ValueError("exchange must be an absolute path without '..'")
        self._exchange = exchange
        self._enrollment_id = enrollment_id
        self._reported: set[tuple[str, str]] = set()
        self._managers: list[dict[str, str | None]] | None = None

    def _report(self, name: str, reason: str) -> None:
        # ponytail: one log line per name and reason until 4096 distinct pairs, then the memory resets.
        if (name, reason) in self._reported:
            return
        if len(self._reported) >= _MAX_REPORTED:
            self._reported.clear()
        self._reported.add((name, reason))
        _LOGGER.warning("daemon_exchange_publish_failed name=%s reason=%s", name, reason)

    def _publish(self, name: str, data: bytes, *, directory: str | None = None) -> bool:
        try:
            exchange = _open_directory(self._exchange)
            try:
                publish_file(exchange, name, data, directory=directory)
            finally:
                os.close(exchange)
        except ExchangeUnavailableError as exc:
            self._report(name, str(exc))
            return False
        except OSError as exc:
            self._report(name, exc.strerror or type(exc).__name__)
            return False
        return True

    def poll(self, managers: list[dict[str, str | None]]) -> None:
        """Publish ``managers.json`` when its rows changed.

        :param managers: The manager rows to publish.
        """

        if managers == self._managers:
            return
        document = {
            "format": _MANAGERS_FORMAT,
            "format_version": _MANAGERS_VERSION,
            "enrollment_id": self._enrollment_id,
            "generated_at": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "managers": managers,
        }
        data = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        if self._publish("managers.json", data):
            self._managers = managers

    def publish_log(self, handle: str, data: bytes) -> bool:
        """Publish one manager's Slurm output tail as ``managers/<handle>.log``.

        :param handle: The manager's broker-issued handle: 32 lowercase hexadecimal digits.
        :param data: The bytes to publish.
        :return: Whether the file was installed; a failure is logged.
        :raises ValueError: If ``handle`` is not a broker-issued handle.
        """

        if _HANDLE.fullmatch(handle) is None:
            raise ValueError("handle must be 32 lowercase hexadecimal digits")
        return self._publish(f"{handle}.log", data, directory="managers")


class _ActivationChanged(Exception):
    """The active snapshot pointer no longer names the policy this instance serves."""


class Broker:
    """Process bounded mailbox requests against the shared lock-free ledger.

    Any number of brokers may serve one enrollment at once. Before every admission and every decision a
    broker calls *activation*; when it reports a change, the broker finishes its current step and retires
    (:attr:`retired`), never between a ``submit`` decision and the submission itself.

    :param policy: Validated protected daemon policy.
    :param gateway: Fixed Slurm gateway for scheduler operations.
    :param ledger: The enrollment's durable daemon ledger.
    :param requests: Descriptor-anchored request mailbox.
    :param responses: Descriptor-anchored response mailbox.
    :param exchange: Publisher of ``managers.json`` and manager logs into the workspace exchange; ``None``
        skips publication.
    :param response_seed: Validated protected response-signing seed.
    :param observe: Follow submitted manager jobs with the scheduler, keeping each observation in the ledger.
    :param activation: Check that the active snapshot is still the one this broker serves; raising
        ``ValueError`` or ``OSError`` retires the broker. ``None`` never retires it.
    :param mailboxes: Reopen the request and response mailboxes; called at the start of every poll, so a
        client that replaced either directory is followed (or, for a symlink, refused) at the next poll.
        The broker then owns and closes the mailboxes it holds. ``None`` keeps *requests* and *responses*.
    """

    def __init__(
        self,
        policy: Policy,
        gateway: SlurmGateway,
        ledger: Ledger,
        requests: MailboxDirectory,
        responses: MailboxDirectory,
        *,
        exchange: ExchangePublisher | None = None,
        response_seed: Path,
        observe: bool = False,
        activation: Callable[[], None] | None = None,
        mailboxes: Callable[[], tuple[MailboxDirectory, MailboxDirectory]] | None = None,
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
        self.observe = observe
        self.activation = activation
        self.mailboxes = mailboxes
        self._mailbox_problem: str | None = None
        #: Rejected publications that could be neither removed nor set aside, in the directory identified.
        self._stuck: set[str] = set()
        self._stuck_identity: tuple[int, int] | None = None
        #: Set once the active snapshot changed: this broker admits and decides nothing more.
        self.retired = False
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

    def _fence(self) -> None:
        """Re-read the active snapshot pointer before an admission or a decision."""

        if self.activation is None:
            return
        try:
            self.activation()
        except (OSError, ValueError) as exc:
            raise _ActivationChanged(str(exc)) from exc

    def _publish(self, publication: str, request: Request, response: Response) -> None:
        """Sign a response, install it while its request is still published, then consume the request.

        Signing is deterministic, so every instance publishes identical bytes. The request is looked up by
        name just before the installation: when the client already consumed the response and removed its
        request, a late instance installs nothing. A response name the client blocked (a directory there) is
        logged and its request discarded: the exchange is client territory and must not stop the daemon.
        """

        signed = sign_response(response, seed_path=self.response_seed)
        if self.requests.lstat(publication) is None:
            _LOGGER.debug("daemon_response_skipped request_id=%s: the request is gone", request.request_id)
            return
        try:
            self.responses.replace(f"{request.request_id}.json", encode_response(signed))
        except OSError as exc:
            _LOGGER.warning(
                "daemon_response_unpublishable request_id=%s reason=%s",
                request.request_id,
                exc.strerror or type(exc).__name__,
            )
            self._discard_invalid(publication, "response_unpublishable")
            return
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

    def _wrong_type(self, publication: str) -> bool:
        """Report whether a publication that failed to read is established as not a regular file (by lstat)."""

        try:
            information = self.requests.lstat(publication)
        except OSError:
            return False
        return information is not None and not stat.S_ISREG(information.st_mode)

    def _unreadable(self, publication: str, exc: OSError) -> None:
        """Log, once per publication and reason, a read failure that may be transient; the request stays."""

        reason = exc.strerror or type(exc).__name__
        if ("unreadable:" + publication, reason) in self._warned:
            return
        # ponytail: one line per publication and reason until 4096 pairs, then the memory resets.
        if len(self._warned) >= _MAX_WARNED:
            self._warned.clear()
        self._warned.add(("unreadable:" + publication, reason))
        _LOGGER.warning("daemon_request_unreadable publication=%s reason=%s: retried next poll", publication, reason)

    def _discard_invalid(self, publication: str, code: str) -> None:
        """Remove a rejected publication; set an unremovable one aside, or at least stop scanning it."""

        _LOGGER.warning("daemon_request_rejected code=%s publication=%s", code, publication)
        try:
            self.requests.remove(publication)
            return
        except FileNotFoundError:
            return
        except OSError:
            pass  # a directory, or an entry this process may not unlink
        try:
            aside = self.requests.set_aside(publication)
        except FileNotFoundError:
            return
        except OSError as exc:
            # ponytail: remembered per mailbox directory up to the scan bound; beyond it they may starve again.
            if len(self._stuck) < MAX_SCANNED_ENTRIES:
                self._stuck.add(publication)
            _LOGGER.warning(
                "daemon_request_unremovable publication=%s reason=%s: skipped from now on",
                publication,
                exc.strerror or type(exc).__name__,
            )
            return
        _LOGGER.info("daemon_request_set_aside publication=%s name=%s", publication, aside)

    def _finish(self, entry: Entry, response: Response) -> Response:
        """Link a response durably and return the stored one (another instance's, when it was first)."""

        stored = self.ledger.finish(entry.request.request_id, response).response
        if stored is None:
            raise LedgerError("finished request has no response")
        return stored

    def _refuse(self, entry: Entry, reason: str, detail: str | None = None) -> Response:
        request = entry.request
        handle = request.handle
        return self._finish(
            entry,
            self._response(request, "refused", reason=reason, detail=detail, **({"handle": handle} if handle else {})),
        )

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

    def _refusal_reason(self, request: Request) -> str | None:
        """Return why a start must be refused, or ``None`` when it may be submitted."""

        assert request.profile is not None
        try:
            check_request_time(request, max_age=self.policy.request_max_age)
        except ValueError:
            return "request_expired"
        try:
            self.policy.launcher(request.profile)
        except ValueError:
            return "invalid_configuration"
        if request.configuration_digest != self.policy.configuration_digest(request.profile):
            return "stale_configuration"
        return None

    def _start(self, entry: Entry) -> Response | None:
        """Decide a start once across all instances; the submit winner submits it.

        :param entry: The admitted start.
        :return: The stored response, or ``None`` while another instance's submission is unanswered.
        """

        request = entry.request
        handle = entry.handle
        if handle is None or request.profile is None:
            raise LedgerError("invalid admitted manager start")
        won = False
        if entry.decision is None:
            # _execute re-read the active snapshot just before: this is the decision's fence.
            entry, won = self.ledger.decide(request.request_id, reason=self._refusal_reason(request))
        decision = entry.decision
        assert decision is not None
        if decision.kind == "refuse":
            # Deterministic: any instance links it, also after the decider crashed.
            assert decision.reason is not None
            return self._finish(entry, refusal_response(request, handle, decision.reason))
        if won:
            return self._submit(entry)
        return entry.response

    def _submit(self, entry: Entry) -> Response:
        """Run the submission this instance won, record the scheduler identity, then the response."""

        request = entry.request
        handle = entry.handle
        assert handle is not None and request.profile is not None
        try:
            launcher = self.policy.launcher(request.profile)
            submission = self.gateway.submit(launcher, handle)
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
                handle=handle,
                reason="submission_unconfirmed",
                detail=self._scheduler_failed(request, handle, exc),
            )
            return self._finish(entry, response)

        self.ledger.record_scheduler(request.request_id, submission.job_id, submission.cluster)
        _LOGGER.info(
            "daemon_submitted handle=%s job_id=%s cluster=%s log=%s",
            handle,
            submission.job_id,
            submission.cluster,
            self.policy.jobs / f"httk-{submission.job_id}.out",
        )
        return self._finish(entry, self._response(request, "submitted", handle=handle))

    def _manager(self, entry: Entry) -> Response:
        request = entry.request
        if request.handle is None:
            raise LedgerError("manager request has no handle")
        # The scheduler identity decides, also when the start's response was a premature "uncertain".
        manager = self.ledger.lookup_manager(request.handle)
        if manager is None or manager.job_id is None or manager.cluster is None:
            return self._refuse(entry, "unknown_manager")
        if manager.cluster != self.policy.cluster:
            raise LedgerError("stored scheduler cluster does not match policy")

        try:
            check_request_time(request, max_age=self.policy.request_max_age)
        except ValueError:
            return self._refuse(entry, "request_expired")

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
            return self._finish(entry, response)
        try:
            self.gateway.cancel(manager.job_id, manager.cluster, request.handle)
        except SchedulerError as exc:
            return self._refuse(entry, "scheduler_unavailable", self._scheduler_failed(request, request.handle, exc))
        return self._finish(entry, self._response(request, "cancel_requested", handle=request.handle))

    def _execute(self, entry: Entry) -> Response | None:
        if entry.response is not None:
            return entry.response
        # Nothing executes, and no start is decided, for a configuration that is no longer active.
        self._fence()
        request = entry.request
        if request.operation == "start_manager":
            return self._start(entry)
        try:
            check_request_time(request, max_age=self.policy.request_max_age)
        except ValueError:
            return self._refuse(entry, "request_expired")
        if request.operation == "health":
            return self._finish(entry, self._response(request, "ready"))
        return self._manager(entry)

    def _process(self, publication: str, request: Request) -> None:
        try:
            verify_request(request, self.policy.authorized_keys)
        except ValueError:
            self._discard_invalid(publication, "request_unauthorized")
            return

        existing = self.ledger.lookup_request(request.request_id)
        if existing is not None and request_digest(existing.request) != request_digest(request):
            self._publish(publication, request, self._response(request, "refused", reason="request_conflict"))
            return
        if request.workspace_id != self.policy.workspace_id:
            self._publish(publication, request, self._response(request, "refused", reason="wrong_workspace"))
            return
        if request.enrollment_id != self.policy.enrollment_id:
            self._publish(publication, request, self._response(request, "refused", reason="wrong_enrollment"))
            return
        if existing is None:
            self._fence()
        try:
            entry = self.ledger.admit(request)
        except ConflictError:
            self._publish(publication, request, self._response(request, "refused", reason="request_conflict"))
            return
        except CapacityError:
            self._publish(publication, request, self._response(request, "busy", reason="capacity"))
            return
        response = self._execute(entry)
        if response is None:
            _LOGGER.debug("daemon_request_pending request_id=%s: another instance submits it", request.request_id)
            return
        self._publish(publication, request, response)

    def _recover(self, now: float) -> None:
        """Settle starts whose decider crashed: refusals at once, submissions after the longest submission."""

        self.ledger.recover(
            older_than=self.policy.command_timeout + KILL_GRACE_SECONDS + _RECOVERY_MARGIN_SECONDS,
            now_ns=int(now * 1_000_000_000),
        )

    def close(self) -> None:
        """Close the mailboxes this broker currently holds; repeated calls are harmless."""

        self.requests.close()
        self.responses.close()

    def _mailbox_trouble(self, problem: str | None) -> None:
        """Log a mailbox problem once until it changes or clears."""

        if problem is not None and problem != self._mailbox_problem:
            _LOGGER.warning("daemon_mailbox_problem reason=%s", problem)
        self._mailbox_problem = problem

    def _reopen(self) -> bool:
        """Reopen the mailboxes for this poll; ``False`` (logged once) when they cannot be opened."""

        if self.mailboxes is None:
            return True
        try:
            requests, responses = self.mailboxes()
        except (OSError, ValueError) as exc:
            self._mailbox_trouble(f"the mailboxes cannot be opened without following a symlink: {excerpt(str(exc))}")
            return False
        self.close()
        self.requests, self.responses = requests, responses
        return True

    def _scan(self) -> tuple[str, ...]:
        """Return this poll's bounded batch of request publications; a failed scan is logged and skipped."""

        try:
            identity = self.requests.identity()
            if identity != self._stuck_identity:
                self._stuck_identity, self._stuck = identity, set()
            names, truncated = self.requests.scan(self._stuck)
        except (OSError, ValueError) as exc:
            self._mailbox_trouble(f"the request mailbox cannot be scanned: {excerpt(str(exc))}")
            return ()
        self._mailbox_trouble(
            f"the request mailbox holds more than {MAX_DIRECTORY_ENTRIES} publications or {MAX_SCANNED_ENTRIES} "
            "entries; serving it in batches"
            if truncated
            else None
        )
        return names

    def _sweep_responses(self, now: float) -> None:
        """Remove mailbox responses no client can still be waiting for.

        A response older than the request lifetime plus the clock skew answers an expired request: it is
        either one a late instance reinstalled after the client consumed it, or one the client abandoned.
        """

        limit = self.policy.request_max_age + CLOCK_SKEW_SECONDS
        try:
            for name in self.responses.names():
                information = self.responses.lstat(name)
                if information is None or now - information.st_mtime <= limit:
                    continue
                try:
                    self.responses.remove(name)
                except (FileNotFoundError, IsADirectoryError, PermissionError):
                    continue
                _LOGGER.info("daemon_response_swept request_id=%s", name.removesuffix(".json"))
        except (OSError, ValueError) as exc:
            _LOGGER.warning("daemon_response_sweep_failed reason=%s", excerpt(str(exc)))

    def process_once(self, stop: threading.Event) -> int:
        """Recover, process one bounded sorted mailbox snapshot, then publish and sweep the exchange.

        :param stop: Stop admission before the next request when set.
        :return: Number of request publications considered.
        """

        if self.retired:
            return 0
        now = time.time()
        self._recover(now)
        processed = 0
        available = self._reopen()
        for publication in self._scan() if available else ():
            if stop.is_set():
                break
            processed += 1
            try:
                data = self.requests.read(publication)
            except FileNotFoundError:
                continue
            except ValueError:  # not a regular file, or too large: established as invalid
                self._discard_invalid(publication, "invalid_publication")
                continue
            except OSError as exc:
                if self._wrong_type(publication):
                    self._discard_invalid(publication, "invalid_publication")
                else:
                    self._unreadable(publication, exc)
                continue
            try:
                request = decode_request(data)
            except ValueError:
                self._discard_invalid(publication, "invalid_document")
                continue
            if publication != f"{request.request_id}.json":
                self._discard_invalid(publication, "identifier_mismatch")
                continue
            try:
                self._process(publication, request)
            except _ActivationChanged as exc:
                self.retired = True
                _LOGGER.warning(
                    "daemon_retired reason=%s: the active daemon configuration changed; this instance admits and "
                    "decides nothing more and stops; start the daemon again to serve the new configuration",
                    excerpt(str(exc)),
                )
                break
        if self.exchange is not None:
            try:
                rows = self.ledger.managers()
                try:
                    self._observe(rows, now)
                except Exception as exc:
                    _LOGGER.warning("daemon_manager_observation_failed reason=%s", excerpt(str(exc)))
                self.exchange.poll(self._manager_rows(rows))
            except Exception:
                _LOGGER.exception("daemon_exchange_failed")
        if available:
            self._sweep_responses(now)
        return processed

    def _observation(self, handle: str) -> _Observation | None:
        return _parse_observation(self.ledger.observation(handle))

    def _manager_rows(self, rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
        """Extend ledger manager rows, which carry the job ID, with what the scheduler last reported."""

        result: list[dict[str, str | None]] = []
        for row in rows:
            observation = self._observation(str(row["handle"]))
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

        Runs at most once per 60 seconds; a failed check is logged and retried on the next run. Each
        observation is read from and written to the ledger, so instances share them (last writer wins).
        """

        if not self.observe or self.exchange is None:
            return
        if self._observed_at is not None and 0.0 <= now - self._observed_at < _OBSERVE_SECONDS:
            return
        self._observed_at = now
        for row in rows:
            handle = str(row["handle"])
            previous = self._observation(handle)
            observation = previous
            if row["job_id"] is not None and (observation is None or not observation["final"]):
                entry = self.ledger.lookup_manager(handle)
                if entry is None or entry.job_id is None or entry.cluster is None:
                    raise LedgerError("submitted manager has no scheduler identity")
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
            if observation is None:
                continue
            if observation["final"] and not observation["log_published"] and self._publish_log(handle, observation):
                observation = cast(_Observation, {**observation, "log_published": True})
            if observation != previous:
                self._save_observation(handle, observation)

    def _save_observation(self, handle: str, observation: _Observation) -> None:
        data = json.dumps(observation, sort_keys=True, separators=(",", ":")).encode("ascii")
        try:
            self.ledger.record_observation(handle, data)
        except OSError as exc:
            _LOGGER.warning("daemon_observations_unsaved handle=%s reason=%s", handle, exc.strerror)

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

    def _publish_log(self, handle: str, observation: _Observation) -> bool:
        """Publish the tail of a finished manager's private Slurm output into the exchange.

        :param handle: The manager handle.
        :param observation: The manager's final observation, naming its job.
        :return: Whether the log was published.
        """

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
                return False
            data = os.fsencode(f"the Slurm output at {source} is not a regular file\n")
        if not self.exchange.publish_log(handle, data):
            return False
        _LOGGER.info("daemon_manager_log handle=%s path=managers/%s.log", handle, handle)
        return True

    def run(self, stop: threading.Event, *, once: bool = False) -> None:
        """Run bounded scans until stopped or retired, or one scan when requested."""

        while not stop.is_set() and not self.retired:
            self.process_once(stop)
            if once or self.retired or stop.wait(self.policy.poll_seconds):
                return


def _activation_check(state: Path, snapshot: Path, policy: Policy) -> Callable[[], None]:
    """Return the check a broker runs before each admission and decision.

    :param state: The protected state directory holding ``active.json``.
    :param snapshot: The host path of the snapshot this broker serves.
    :param policy: The policy loaded from that snapshot.
    :return: A callable that raises when the active snapshot pointer names anything else.
    """

    def check() -> None:
        verify_active_snapshot(state, snapshot, policy)

    return check


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
    gateway = SlurmGateway(policy)

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
            if arguments.check:
                # Every record is validated whenever it is read; 'daemon check' also validates them all.
                ledger.verify()
            try:
                gateway.check()
            except SchedulerError as exc:
                # Only the clients' own --version output: safe and needed to diagnose the operator's setup.
                raise ValueError(f"scheduler client check failed: {exc}") from exc
            _LOGGER.info("daemon_check_passed workspace=%s cluster=%s", policy.workspace_id, policy.cluster)
            exchange = _EXCHANGE_DIRECTORY
            exchange_fd = _open_directory(exchange)
            stack.callback(os.close, exchange_fd)

            def mailboxes() -> tuple[MailboxDirectory, MailboxDirectory]:
                # Through the exchange descriptor, never following a symlink the client put in their place.
                requests = MailboxDirectory.child(exchange_fd, "requests")
                try:
                    return requests, MailboxDirectory.child(exchange_fd, "responses")
                except BaseException:
                    requests.close()
                    raise

            requests, responses = mailboxes()
            stack.callback(responses.close)
            stack.callback(requests.close)
            if arguments.check:
                return
            publisher = ExchangePublisher(exchange, policy.enrollment_id)
            broker = Broker(
                policy,
                gateway,
                ledger,
                requests,
                responses,
                exchange=publisher,
                response_seed=seed,
                observe=True,
                activation=_activation_check(_STATE_DIRECTORY, policy_source, policy),
                mailboxes=mailboxes,
            )
            stack.callback(broker.close)
            _LOGGER.info(
                "daemon_started workspace=%s enrollment=%s launchers=%s",
                policy.workspace_id,
                policy.enrollment_id,
                ",".join(launcher.name for launcher in policy.launchers),
            )
            broker.run(stop, once=arguments.once)
            if broker.retired:
                _LOGGER.info("daemon_stopped reason=activation_changed workspace=%s", policy.workspace_id)
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
    except (OSError, ValueError, LedgerError, SchedulerError) as exc:
        _LOGGER.error("daemon_service_failed reason=%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["Broker", "ExchangePublisher", "main"]
