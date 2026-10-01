"""Typed CLI controls for a mounted workspace daemon."""

import argparse
import json
import math
import sys
import uuid
from collections.abc import Mapping

from httk.core.cli import CLIContext

from .._daemon_client import Endpoint, decode_matching_response
from .._daemon_protocol import Request, encode_request, encode_response
from ..adapters import read_metadata, remote_settings, resolve_remote, run_adapter
from ._common import _group, _leaf

_OPERATION_NAMES = {
    "health": "health",
    "start": "start_manager",
    "status": "manager_status",
    "cancel": "cancel_manager",
}
_POSITIVE_OUTCOMES = frozenset({"ready", "submitted", "status", "cancel_requested"})


def _request_id(value: str | None, *, required: bool) -> str:
    """Return a supplied request ID or generate one for a read-only request."""

    if value is None:
        if required:
            raise ValueError("--request-id is required for this daemon operation")
        return uuid.uuid4().hex
    if len(value) != 32 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("request ID must be 32 lowercase hexadecimal digits")
    return value


def _response_returncode(outcome: str) -> int:
    """Map a confirmed daemon outcome to the CLI status convention."""

    if outcome in _POSITIVE_OUTCOMES:
        return 0
    if outcome in {"refused", "busy", "uncertain"}:
        return 2
    raise ValueError(f"unsupported daemon outcome: {outcome}")


def handle_remote_daemon(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Send one typed request to the configured mounted workspace daemon.

    :param arguments: The parsed daemon command arguments.
    :param context: The active CLI context.
    :return: The status derived from the confirmed daemon outcome, or 2 when no result is acknowledged.
    """

    wait_seconds = arguments.wait_seconds
    if (
        not isinstance(wait_seconds, (int, float))
        or isinstance(wait_seconds, bool)
        or not math.isfinite(wait_seconds)
        or not 0.05 <= wait_seconds <= 120
    ):
        raise ValueError("--wait-seconds must be finite and between 0.05 and 120")

    target = resolve_remote(arguments.remote, project=context.cwd)
    metadata = read_metadata(target.bundle)
    if metadata.get("kind") != "mount-daemon":
        raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
    settings = remote_settings(target.bundle)
    if not isinstance(settings, Mapping):
        raise ValueError("mount-daemon remote settings must be an object")
    endpoint = Endpoint.from_settings(settings)

    verb = arguments.daemon_verb
    request = Request(
        _request_id(arguments.request_id, required=verb in {"start", "cancel"}),
        endpoint.workspace_id,
        _OPERATION_NAMES[verb],
        profile=getattr(arguments, "profile", None),
        handle=getattr(arguments, "handle", None),
        enrollment_id=endpoint.enrollment_id,
    )
    request_document = json.loads(encode_request(request))
    print(f"daemon request ID: {request.request_id}", file=sys.stderr)
    try:
        result = run_adapter(
            target.bundle,
            "daemon",
            {"daemon_request": request_document, "wait_seconds": wait_seconds},
            timeout=wait_seconds + 5,
        )
        response_data = result.get("stdout")
        if not isinstance(response_data, str):
            raise ValueError("daemon adapter did not return a response document")
        response = decode_matching_response(response_data.encode("utf-8"), request)
        returncode = _response_returncode(response.outcome)
        if result.get("returncode") != returncode or result.get("stderr", "") != "":
            raise ValueError("daemon adapter returned an inconsistent result")
        print(encode_response(response).decode("ascii"))
        return returncode
    except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
        print(
            f"daemon request {request.request_id} has no acknowledged result: {exc}. "
            "If retrying, reuse this request ID with identical fields; do not create a new ID.",
            file=sys.stderr,
        )
        return 2


def build_daemon_remote_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Add ``workflow remote daemon`` typed controls to the remote group.

    :param subparsers: The command collection for ``workflow remote``.
    """

    _, daemon_subparsers = _group(
        subparsers,
        "daemon",
        summary="control a mounted workspace daemon",
        description="Send bounded typed health, manager, and cancellation requests to a mount-daemon remote",
    )
    for verb, summary in (
        ("health", "check daemon readiness"),
        ("start", "start an approved manager profile"),
        ("status", "inspect a broker-issued manager handle"),
        ("cancel", "request cancellation of a manager handle"),
    ):
        parser = _leaf(
            daemon_subparsers,
            verb,
            summary=summary,
            description=f"Run the typed daemon {verb} operation",
            handler=handle_remote_daemon,
        )
        parser.add_argument("remote", metavar="REMOTE", help="the configured mount-daemon remote")
        parser.add_argument(
            "--wait-seconds",
            type=float,
            default=10.0,
            metavar="SECONDS",
            help="bound the mailbox wait (0.05 to 120 seconds; default: 10)",
        )
        if verb == "start":
            parser.add_argument("--profile", required=True, metavar="PROFILE", help="approved manager profile")
            parser.add_argument(
                "--request-id", required=True, metavar="ID", help="32 lowercase hexadecimal request ID; reuse on retry"
            )
        elif verb == "cancel":
            parser.add_argument("--handle", required=True, metavar="HANDLE", help="broker-issued manager handle")
            parser.add_argument(
                "--request-id", required=True, metavar="ID", help="32 lowercase hexadecimal request ID; reuse on retry"
            )
        elif verb == "status":
            parser.add_argument("--handle", required=True, metavar="HANDLE", help="broker-issued manager handle")
            parser.add_argument("--request-id", metavar="ID", help="optional 32 lowercase hexadecimal request ID")
        else:
            parser.add_argument("--request-id", metavar="ID", help="optional 32 lowercase hexadecimal request ID")
        parser.set_defaults(daemon_verb=verb)
