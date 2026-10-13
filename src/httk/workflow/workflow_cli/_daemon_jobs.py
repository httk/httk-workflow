"""Signed client commands for job control through a mounted daemon."""

import json
import math
import time
import uuid
from argparse import Namespace
from pathlib import Path
from typing import cast

from httk.core.cli import CLIContext
from httk.core.identity import OperatorIdentity

from .._daemon_client import Endpoint, decode_matching_response, prepare_job_request, prepare_request
from .._daemon_protocol import Request, Response, encode_request
from ..adapters import read_metadata, remote_settings, resolve_remote, run_adapter
from ..registry import WorkspaceBinding

#: The bounded mailbox wait for one daemon response.
_WAIT_SECONDS = 10.0
#: The pause between two signed status observations while waiting for a pause.
_POLL_SECONDS = 1.0
#: The least response wait a status observation is given near the end of ``--timeout``.
_MIN_WAIT_SECONDS = 0.05
_TERMINAL_STATES = frozenset({"failed", "succeeded", "cancelled"})


def _exchange(
    bundle: Path, endpoint: Endpoint, request: Request, *, wait_seconds: float, adapter_timeout: float | None
) -> Response:
    """Run one already signed command through the mount-daemon adapter and validate its signed response."""

    result = run_adapter(
        bundle,
        "daemon",
        {"daemon_request": json.loads(encode_request(request)), "wait_seconds": wait_seconds},
        timeout=adapter_timeout,
    )
    data = result.get("stdout")
    if type(data) is not str:
        raise ValueError("daemon adapter did not return a response document")
    response = decode_matching_response(data.encode("utf-8"), request, public_key=endpoint.require_daemon()[1])
    expected = 1 if response.outcome in {"refused", "uncertain"} else 0
    if result.get("returncode") != expected or result.get("stderr", "") != "":
        raise ValueError("daemon adapter returned an inconsistent job-command result")
    return response


def job_request_status(
    bundle: Path,
    endpoint: Endpoint,
    identity: OperatorIdentity,
    command_id: str,
    *,
    request_id: str | None = None,
    wait_seconds: float = _WAIT_SECONDS,
    adapter_timeout: float | None = None,
) -> tuple[Request, Response]:
    """Send one fresh signed observation of an original job command.

    :param bundle: The mount-daemon adapter bundle.
    :param endpoint: Its pinned mounted exchange.
    :param identity: The selected local signing identity.
    :param command_id: The original ``job_request`` command ID.
    :param request_id: A status request ID, generated fresh when omitted.
    :param wait_seconds: The bounded mailbox response wait.
    :param adapter_timeout: Optional outer adapter process timeout.
    :return: The signed observation request and its matching signed response.
    """

    enrollment_id, _ = endpoint.require_daemon()
    intent = Request(
        uuid.uuid4().hex if request_id is None else request_id,
        endpoint.workspace_id,
        "job_request_status",
        enrollment_id=enrollment_id,
        command_id=command_id,
    )
    request = prepare_request(endpoint, intent, seed_path=identity.seed_path)
    return request, _exchange(bundle, endpoint, request, wait_seconds=wait_seconds, adapter_timeout=adapter_timeout)


def _await_pause(
    bundle: Path,
    endpoint: Endpoint,
    identity: OperatorIdentity,
    command_id: str,
    response: Response,
    *,
    timeout: float | None,
    adapter_timeout: float | None,
) -> tuple[str, str, str | None]:
    """Return a pause command's outcome, detail and job state once it lands, is refused, or ``timeout`` passes.

    Each poll is one fresh signed status request of the original command.
    """

    deadline = None if timeout is None else time.monotonic() + timeout
    outcome, detail, state = response.outcome, response.detail or response.reason or "", response.current_state
    while outcome in {"queued", "applied"}:
        if state == "paused":
            return "applied", "job is paused", state
        if state in _TERMINAL_STATES:
            return "refused", f"pause superseded by terminal state {state}", state
        remaining = math.inf if deadline is None else deadline - time.monotonic()
        if remaining <= 0:
            return "refused", "pause request timed out and remains pending", state
        time.sleep(min(_POLL_SECONDS, remaining))
        wait = min(_WAIT_SECONDS, max(_MIN_WAIT_SECONDS, math.inf if deadline is None else deadline - time.monotonic()))
        _, response = job_request_status(
            bundle, endpoint, identity, command_id, wait_seconds=wait, adapter_timeout=adapter_timeout
        )
        outcome, detail, state = response.outcome, response.detail or response.reason or "", response.current_state
    return outcome, detail, state


def request_remote_jobs(
    binding: WorkspaceBinding,
    context: CLIContext,
    arguments: Namespace,
    identity: OperatorIdentity,
) -> tuple[int, str]:
    """Send one typed daemon request per remote job selector and optionally wait for pauses.

    The caller validates ``--wait`` and ``--timeout``.

    :param binding: The resolved remote workspace binding.
    :param context: The active CLI context.
    :param arguments: The job request arguments.
    :param identity: The already selected operator identity.
    :return: Exit status and per-job result rows.
    :raises ValueError: If the daemon request or batch options are invalid.
    """

    target = resolve_remote(binding.remote, project=context.cwd)
    if read_metadata(target.bundle).get("kind") != "mount-daemon":
        raise ValueError(f"remote {binding.remote!r} is not a mount-daemon remote")
    endpoint = Endpoint.from_settings(remote_settings(target.bundle))
    enrollment_id, _ = endpoint.require_daemon()
    selectors = list(arguments.job_id)
    if not selectors:
        raise ValueError("daemon job requests require at least one UUID or UUID-prefix selector")
    request_id = getattr(arguments, "request_id", None)
    if request_id is not None and len(selectors) != 1:
        raise ValueError("--request-id is valid only for one daemon-mediated job command")
    if getattr(arguments, "force", False):
        raise ValueError("--force is not supported by the mounted daemon; no request was sent")
    adapter_timeout = arguments.adapter_timeout
    if adapter_timeout is not None and not 0 < adapter_timeout < math.inf:
        raise ValueError("--adapter-timeout must be finite and positive")
    digest = cast(str, endpoint.live()["configuration_digest"])
    intents = [
        Request(
            uuid.uuid4().hex if request_id is None else request_id,
            endpoint.workspace_id,
            "job_request",
            enrollment_id=enrollment_id,
            configuration_digest=digest,
            selector=selector,
            action=str(arguments.action),
            reason=str(arguments.reason),
            priority=arguments.priority,
            step=arguments.step,
            destination=arguments.destination,
        )
        for selector in selectors
    ]
    # Sign and verify every request before sending any command in this batch.
    prepared = [prepare_job_request(endpoint, intent, seed_path=identity.seed_path) for intent in intents]

    rows: list[str] = []
    failed = False
    for request in prepared:
        command_id = request.request_id
        try:
            response = _exchange(
                target.bundle, endpoint, request, wait_seconds=_WAIT_SECONDS, adapter_timeout=adapter_timeout
            )
            outcome, detail, state = response.outcome, response.detail or response.reason or "", response.current_state
            if arguments.wait:
                outcome, detail, state = _await_pause(
                    target.bundle,
                    endpoint,
                    identity,
                    command_id,
                    response,
                    timeout=arguments.timeout,
                    adapter_timeout=adapter_timeout,
                )
            columns = (response.job_id or str(request.selector), outcome, response.internal_request_id or "-")
            row = "\t".join((*columns, command_id, detail or "no further detail", state or "-"))
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            outcome = "uncertain"
            row = "\t".join((str(request.selector), outcome, "-", command_id, str(exc), "-"))
        rows.append(row)
        failed |= outcome in {"refused", "uncertain"}
    return (1 if failed else 0), "".join(f"{row}\n" for row in rows)


__all__ = ["job_request_status", "request_remote_jobs"]
