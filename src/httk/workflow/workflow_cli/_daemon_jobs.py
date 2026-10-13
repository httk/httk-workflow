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


def exchange_job_command(
    bundle: Path,
    endpoint: Endpoint,
    intent: Request,
    identity: OperatorIdentity,
    *,
    wait_seconds: float,
    adapter_timeout: float | None = None,
) -> tuple[Request, Response]:
    """Sign, cache, send and validate one daemon job command.

    :param bundle: The mount-daemon adapter bundle.
    :param endpoint: Its pinned mounted exchange.
    :param intent: One unsigned typed job request or observation.
    :param identity: The selected local signing identity.
    :param wait_seconds: The bounded mailbox response wait.
    :param adapter_timeout: Optional outer adapter process timeout.
    :return: The exact signed request and its matching signed response.
    :raises OSError: If local cache or mounted exchange operations fail.
    :raises ValueError: If request, endpoint, signature or response validation fails.
    :raises RuntimeError: If the adapter cannot complete the exchange.
    :raises TimeoutError: If the response is not acknowledged in time.
    """

    prepare = prepare_job_request if intent.operation == "job_request" else prepare_request
    request = prepare(endpoint, intent, seed_path=identity.seed_path)
    response = _exchange_prepared_job_command(
        bundle, endpoint, request, wait_seconds=wait_seconds, adapter_timeout=adapter_timeout
    )
    return request, response


def _exchange_prepared_job_command(
    bundle: Path,
    endpoint: Endpoint,
    request: Request,
    *,
    wait_seconds: float,
    adapter_timeout: float | None,
) -> Response:
    """Run one already signed command through the mount-daemon adapter."""

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


def job_request(
    bundle: Path,
    endpoint: Endpoint,
    identity: OperatorIdentity,
    *,
    selector: str,
    action: str,
    reason: str,
    priority: int | None = None,
    step: str | None = None,
    destination: str | None = None,
    request_id: str | None = None,
    wait_seconds: float = 10.0,
    adapter_timeout: float | None = None,
) -> tuple[Request, Response]:
    """Send one signed daemon-mediated job request.

    :param bundle: The mount-daemon adapter bundle.
    :param endpoint: Its pinned mounted exchange.
    :param identity: The selected local signing identity.
    :param selector: A canonical job UUID or UUID prefix.
    :param action: One supported internal job-request action.
    :param reason: The operator's explanation.
    :param priority: Priority for ``set_priority``.
    :param step: Step for ``override_step``.
    :param destination: Outbox-relative destination for ``eject``.
    :param request_id: Stable command ID for an exact retry, or a fresh generated ID.
    :param wait_seconds: The bounded mailbox response wait.
    :param adapter_timeout: Optional outer adapter process timeout.
    :return: The exact signed request and its matching signed response.
    """

    enrollment_id, _ = endpoint.require_daemon()
    intent = Request(
        uuid.uuid4().hex if request_id is None else request_id,
        endpoint.workspace_id,
        "job_request",
        enrollment_id=enrollment_id,
        configuration_digest=cast(str, endpoint.live()["configuration_digest"]),
        selector=selector,
        action=action,
        reason=reason,
        priority=priority,
        step=step,
        destination=destination,
    )
    return exchange_job_command(
        bundle, endpoint, intent, identity, wait_seconds=wait_seconds, adapter_timeout=adapter_timeout
    )


def job_request_status(
    bundle: Path,
    endpoint: Endpoint,
    identity: OperatorIdentity,
    command_id: str,
    *,
    request_id: str | None = None,
    wait_seconds: float = 10.0,
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
    return exchange_job_command(
        bundle, endpoint, intent, identity, wait_seconds=wait_seconds, adapter_timeout=adapter_timeout
    )


def request_remote_jobs(
    binding: WorkspaceBinding,
    context: CLIContext,
    arguments: Namespace,
    identity: OperatorIdentity,
) -> tuple[int, str, str]:
    """Send one typed daemon request per remote job selector and optionally wait for pauses.

    :param binding: The resolved remote workspace binding.
    :param context: The active CLI context.
    :param arguments: The job request arguments.
    :param identity: The already selected operator identity.
    :return: Exit status, per-job result rows and diagnostics.
    :raises ValueError: If the daemon request or batch options are invalid.
    """

    remote_name = binding.remote
    target = resolve_remote(remote_name, project=context.cwd)
    if read_metadata(target.bundle).get("kind") != "mount-daemon":
        raise ValueError(f"remote {remote_name!r} is not a mount-daemon remote")
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
    wait = bool(arguments.wait)
    timeout = arguments.timeout
    if timeout is not None and (type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout < 0):
        raise ValueError("--timeout must be finite and nonnegative")
    adapter_timeout = arguments.adapter_timeout
    if adapter_timeout is not None and (
        type(adapter_timeout) not in {int, float} or not math.isfinite(adapter_timeout) or adapter_timeout <= 0
    ):
        raise ValueError("--adapter-timeout must be finite and positive")
    wait_seconds = 10.0
    action = str(arguments.action)
    if wait and action != "pause":
        raise ValueError("--wait is only valid with the pause action")
    reason = str(arguments.reason)
    priority = arguments.priority
    step = arguments.step
    destination = arguments.destination
    digest = cast(str, endpoint.live()["configuration_digest"])
    intents = [
        Request(
            request_id if request_id is not None else uuid.uuid4().hex,
            endpoint.workspace_id,
            "job_request",
            enrollment_id=enrollment_id,
            configuration_digest=digest,
            selector=selector,
            action=action,
            reason=reason,
            priority=priority,
            step=step,
            destination=destination,
        )
        for selector in selectors
    ]
    # Sign and verify every request before sending any command in this batch.
    prepared = [prepare_job_request(endpoint, intent, seed_path=identity.seed_path) for intent in intents]

    rows: list[str] = []
    failed = False
    for intent, request in zip(intents, prepared, strict=True):
        command_id = intent.request_id
        try:
            response = _exchange_prepared_job_command(
                target.bundle,
                endpoint,
                request,
                wait_seconds=wait_seconds,
                adapter_timeout=adapter_timeout,
            )
            outcome = response.outcome
            detail = response.detail or response.reason or ""
            state = response.current_state
            if wait and outcome in {"queued", "applied"}:
                deadline = None if timeout is None else time.monotonic() + timeout
                while True:
                    if outcome in {"refused", "uncertain"}:
                        break
                    if state in {"failed", "succeeded", "cancelled"}:
                        outcome, detail = "refused", f"pause superseded by terminal state {state}"
                        break
                    if state == "paused":
                        outcome, detail = "applied", "job is paused"
                        break
                    if deadline is not None and time.monotonic() >= deadline:
                        outcome, detail = "refused", "pause request timed out and remains pending"
                        break
                    remaining = None if deadline is None else deadline - time.monotonic()
                    poll_wait = wait_seconds if remaining is None else min(wait_seconds, remaining)
                    if poll_wait < 0.05:
                        outcome, detail = "refused", "pause request timed out and remains pending"
                        break
                    time.sleep(1.0 if remaining is None else min(1.0, remaining))
                    _status_request, response = job_request_status(
                        target.bundle,
                        endpoint,
                        identity,
                        command_id,
                        wait_seconds=poll_wait,
                        adapter_timeout=adapter_timeout,
                    )
                    outcome = response.outcome
                    detail = response.detail or response.reason or ""
                    state = response.current_state
            row = "\t".join(
                (
                    response.job_id or str(intent.selector),
                    outcome,
                    response.internal_request_id or "-",
                    command_id,
                    detail or "no further detail",
                    state or "-",
                )
            )
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            outcome = "uncertain"
            row = "\t".join((str(intent.selector), outcome, "-", command_id, str(exc), "-"))
        rows.append(row)
        failed |= outcome in {"refused", "uncertain"}
    return (1 if failed else 0), "".join(f"{row}\n" for row in rows), ""


__all__ = ["exchange_job_command", "job_request", "job_request_status", "request_remote_jobs"]
