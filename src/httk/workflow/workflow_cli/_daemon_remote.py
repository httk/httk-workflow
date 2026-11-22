"""Typed CLI controls for a mounted workspace daemon: ``manager``, ``remote take-back`` and their helpers."""

import argparse
import json
import math
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from httk.core.cli import CLIContext
from httk.core.identity import configured_operator_identity

from .._daemon_cli import _anchored
from .._daemon_client import (
    Endpoint,
    decode_matching_response,
    prepare_request,
    read_daemon,
    read_exchange,
    read_launcher_log,
    read_passive_status,
    take_back,
)
from .._daemon_protocol import Request, encode_request, encode_response
from .._durations import format_duration, parse_slurm_duration
from ..adapters import read_metadata, remote_settings, resolve_remote, run_adapter
from ..registry import WorkspaceBinding
from ._common import _group, _leaf
from ._daemon_jobs import job_request_status

_OPERATION_NAMES = {
    "start": "start_manager",
    "status": "manager_status",
    "cancel": "cancel_manager",
}
_POSITIVE_OUTCOMES = frozenset({"ready", "submitted", "status", "cancel_requested"})
#: The daemon manager's default ``--deadline-margin``: a shorter job would start draining at once.
_MIN_TIME_LIMIT_SECONDS = 120


def exchange_pins(value: str) -> tuple[dict[str, str], list[str]]:
    """Read the identities a mount-daemon remote pins from a mounted exchange directory (``remote set R exchange=P``).

    :param value: The mounted ``WORKSPACE/exchange`` directory; a relative path anchors to the working directory.
    :return: The settings to store (the anchored ``exchange``, the workspace published in ``exchange.json`` and,
        when ``daemon.json`` exists, the daemon identities), and the daemon pins to remove when it does not.
    :raises ValueError: If a document is invalid or the path is ambiguous.
    :raises OSError: If the exchange cannot be opened safely.
    """

    exchange = _anchored(Path(value), "exchange")
    settings = {"exchange": str(exchange), "daemon_workspace_id": cast(str, read_exchange(exchange)["workspace_id"])}
    try:
        exported = read_daemon(exchange)
    except FileNotFoundError:
        print(
            "no daemon.json in the exchange: pinned the workspace only; signed requests stay unavailable",
            file=sys.stderr,
        )
        Endpoint.from_settings(settings).check()
        return settings, ["daemon_enrollment_id", "daemon_public_key"]  # unpins stale daemon pins
    settings["daemon_enrollment_id"] = cast(str, exported["enrollment_id"])
    settings["daemon_public_key"] = cast(str, exported["daemon_public_key"])
    Endpoint.from_settings(settings).check()
    return settings, []


def _request_id(value: str | None) -> str:
    """Return a supplied request ID, or generate a fresh one."""

    if value is None:
        return uuid.uuid4().hex
    if len(value) != 32 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("request ID must be 32 lowercase hexadecimal digits")
    return value


def _mount_endpoint(arguments: argparse.Namespace, context: CLIContext) -> tuple[Path, Endpoint]:
    """Return the remote bundle and pinned endpoint of a mount-daemon remote."""

    target = resolve_remote(arguments.remote, project=context.cwd)
    if read_metadata(target.bundle).get("kind") != "mount-daemon":
        raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
    settings = remote_settings(target.bundle)
    if not isinstance(settings, Mapping):
        raise ValueError("mount-daemon remote settings must be an object")
    return target.bundle, Endpoint.from_settings(settings)


def handle_manager_log(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Write the published log of one manager handle to standard output, byte for byte.

    :param arguments: The parsed ``log`` command arguments.
    :param context: The active CLI context.
    :return: 0 after the log is written.
    """

    _, endpoint = _mount_endpoint(arguments, context)
    sys.stdout.flush()
    sys.stdout.buffer.write(read_launcher_log(endpoint, arguments.handle))
    sys.stdout.buffer.flush()
    return 0


def daemon_description(bundle: Path) -> dict[str, object]:
    """Return the validated ``daemon.json`` a mount-daemon remote pins, for ``remote check``.

    :param bundle: The remote bundle.
    :return: The daemon's identities and approved configurations, as plain JSON values.
    :raises ValueError: If the settings or the document are invalid, the daemon is not pinned (the message says how
        to pin it), or the enrollment changed.
    :raises OSError: If the document cannot be opened safely.
    """

    settings = remote_settings(bundle)
    if not isinstance(settings, Mapping):
        raise ValueError("mount-daemon remote settings must be an object")
    return json.loads(json.dumps(Endpoint.from_settings(settings).live(), default=dict))


def handle_remote_take_back(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Take an ejected bundle back out of the exchange inbox before a manager adopts it.

    :param arguments: The parsed ``take-back`` command arguments.
    :param context: The active CLI context.
    :return: 0 after the bundle is copied out and removed from the inbox.
    """

    _, endpoint = _mount_endpoint(arguments, context)
    destination = _anchored(Path(arguments.destination or arguments.name), "destination")
    print(take_back(endpoint, arguments.name, destination))
    return 0


def _time_limit(text: str) -> str:
    """Return a ``--time-limit`` longer than the manager's deadline margin as canonical Slurm duration text."""

    try:
        seconds = parse_slurm_duration(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if seconds <= _MIN_TIME_LIMIT_SECONDS:
        raise argparse.ArgumentTypeError(
            f"{text!r} does not exceed the manager's {_MIN_TIME_LIMIT_SECONDS} s deadline margin"
        )
    return format_duration(seconds)


def _response_returncode(outcome: str) -> int:
    """Map a confirmed daemon outcome to the CLI status convention."""

    if outcome in _POSITIVE_OUTCOMES:
        return 0
    if outcome in {"refused", "busy", "uncertain"}:
        return 2
    raise ValueError(f"unsupported daemon outcome: {outcome}")


def handle_manager(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Send one typed ``manager status`` or ``manager cancel`` request to a mounted workspace daemon.

    :param arguments: The parsed manager command arguments.
    :param context: The active CLI context.
    :return: The status derived from the confirmed daemon outcome, or 2 when no result is acknowledged.
    """

    return daemon_request(arguments, context, arguments.daemon_verb)


def start_manager(binding: WorkspaceBinding, arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run ``httk run`` against a mount-daemon remote: a typed start of an approved manager configuration.

    :param binding: The remote workspace binding.
    :param arguments: The parsed ``run`` arguments.
    :param context: The active CLI context.
    :return: The status derived from the confirmed daemon outcome, or 2 when no result is acknowledged.
    :raises ValueError: If a manager option the daemon does not carry is given, or none names a configuration.
    """

    from ._manager import manager_option_defaults

    defaults = {**manager_option_defaults(), "no_durable": False}
    defaults.pop("time_limit")
    if any(getattr(arguments, name, value) != value for name, value in defaults.items()):
        raise ValueError(
            "a mount-daemon remote starts an approved manager configuration; run takes only --configuration, "
            "--time-limit, --request-id and --wait-seconds there"
        )
    if arguments.configuration is None:
        raise ValueError(
            f"{binding.remote} is a mount-daemon remote: name an approved manager configuration with "
            f"--configuration NAME (httk remote check {binding.remote} lists them)"
        )
    try:
        time_limit = None if arguments.time_limit is None else _time_limit(arguments.time_limit)
    except argparse.ArgumentTypeError as exc:
        raise ValueError(f"--time-limit: {exc}") from exc
    start = argparse.Namespace(
        remote=binding.remote,
        configuration=arguments.configuration,
        time_limit=time_limit,
        request_id=arguments.request_id,
        wait_seconds=10.0 if arguments.wait_seconds is None else arguments.wait_seconds,
    )
    return daemon_request(start, context, "start")


def daemon_request(arguments: argparse.Namespace, context: CLIContext, verb: str) -> int:
    """Send one typed request to the configured mounted workspace daemon.

    :param arguments: The request fields: ``remote``, ``wait_seconds``, ``request_id`` and, by verb, ``handle``,
        ``configuration`` and ``time_limit``.
    :param context: The active CLI context.
    :param verb: ``start``, ``status`` or ``cancel``.
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

    if verb == "status" and arguments.handle is None and arguments.request_id is None:
        target = resolve_remote(arguments.remote, project=context.cwd)
        if read_metadata(target.bundle).get("kind") != "mount-daemon":
            raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
        settings = remote_settings(target.bundle)
        if not isinstance(settings, Mapping):
            raise ValueError("mount-daemon remote settings must be an object")
        print(json.dumps(read_passive_status(Endpoint.from_settings(settings)), indent=2, sort_keys=True))
        return 0

    if verb == "status" and arguments.request_id is not None:
        target = resolve_remote(arguments.remote, project=context.cwd)
        settings = remote_settings(target.bundle)
        if read_metadata(target.bundle).get("kind") != "mount-daemon" or not isinstance(settings, Mapping):
            raise ValueError(f"remote {arguments.remote!r} is not a configured mount-daemon remote")
        identity = configured_operator_identity()
        if identity is None:
            raise ValueError("no operator identity is configured; run httk setup to establish one")
        _, response = job_request_status(
            target.bundle,
            Endpoint.from_settings(settings),
            identity,
            arguments.request_id,
            wait_seconds=arguments.wait_seconds,
        )
        print(encode_response(response).decode("ascii"))
        return 0 if response.outcome in {"applied", "queued"} else 1

    target = resolve_remote(arguments.remote, project=context.cwd)
    metadata = read_metadata(target.bundle)
    if metadata.get("kind") != "mount-daemon":
        raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
    settings = remote_settings(target.bundle)
    if not isinstance(settings, Mapping):
        raise ValueError("mount-daemon remote settings must be an object")
    endpoint = Endpoint.from_settings(settings)
    enrollment_id, public_key = endpoint.require_daemon()

    configuration = getattr(arguments, "configuration", None)
    configuration_digest = None
    if verb == "start":
        if type(configuration) is not str:
            raise ValueError("start requires a configuration name")
        configurations = cast(Mapping[str, Mapping[str, object]], endpoint.live()["configurations"])
        if configuration not in configurations:
            available = ", ".join(sorted(configurations)) or "(none)"
            raise ValueError(f"unknown approved daemon configuration {configuration!r}; available: {available}")
        configuration_digest = cast(str, configurations[configuration]["digest"])
    request_id = _request_id(arguments.request_id)
    intent = Request(
        request_id,
        endpoint.workspace_id,
        _OPERATION_NAMES[verb],
        profile=configuration,
        handle=getattr(arguments, "handle", None),
        enrollment_id=enrollment_id,
        configuration_digest=configuration_digest,
        time_limit=getattr(arguments, "time_limit", None),
    )
    request = prepare_request(endpoint, intent)
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
        response = decode_matching_response(
            response_data.encode("utf-8"),
            request,
            public_key=public_key,
        )
        returncode = _response_returncode(response.outcome)
        if result.get("returncode") != returncode or result.get("stderr", "") != "":
            raise ValueError("daemon adapter returned an inconsistent result")
        print(encode_response(response).decode("ascii"))
        return returncode
    except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
        print(
            f"daemon request {request.request_id} has no acknowledged result: {exc}. "
            "It may still be queued in the exchange and be applied when the daemon picks it up, "
            "until the request expires. Check with "
            f"`{context.program} manager status {arguments.remote}`. "
            f"To keep waiting for this request, rerun with identical fields and --request-id {request.request_id}; "
            "rerunning without it sends a new request"
            + (" and may start another manager." if verb == "start" else "."),
            file=sys.stderr,
        )
        return 2


def add_take_back_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Add ``remote take-back`` to the remote group.

    :param subparsers: The command collection for ``remote``.
    """

    take = _leaf(
        subparsers,
        "take-back",
        summary="take an ejected bundle back from a mount-daemon inbox",
        description=(
            "Rename EXCHANGE/inbox/NAME of a mount-daemon remote to a dot name (managers ignore it), copy it to "
            "DESTINATION (default: ./NAME) and remove it; fails when a manager already took it, then cancel the job "
            "instead"
        ),
        handler=handle_remote_take_back,
    )
    take.add_argument("remote", metavar="REMOTE", help="the configured mount-daemon remote")
    take.add_argument("name", metavar="NAME", help="bundle name in the exchange inbox")
    take.add_argument("destination", nargs="?", metavar="DESTINATION", help="local path to copy to (default: ./NAME)")


def build_manager_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``manager`` group: the managers a mount-daemon remote's daemon started.

    :param subparsers: The top-level command collection.
    """

    _, group = _group(
        subparsers,
        "manager",
        summary="inspect and cancel the managers a workspace daemon started",
        description=(
            "Inspect, read the log of, and cancel the managers the daemon behind a mount-daemon remote started "
            "(httk run -w REMOTE:WS --configuration NAME starts one)"
        ),
    )
    log = _leaf(
        group,
        "log",
        summary="print a manager's published launcher log",
        description=(
            "Print EXCHANGE/launchers/<handle>.log, the tail of the manager's Slurm (launcher) output, "
            "which appears when the manager's Slurm job ends and is removed by the workspace's retention "
            "(retention.trash_days)"
        ),
        handler=handle_manager_log,
    )
    log.add_argument("remote", metavar="REMOTE", help="the configured mount-daemon remote")
    log.add_argument("--handle", required=True, metavar="HANDLE", help="broker-issued manager handle")
    for verb, summary, description in (
        (
            "status",
            "inspect the daemon's managers",
            (
                "Print the published exchange status and managers, or, with --handle or --request-id, send a typed "
                "status request"
            ),
        ),
        ("cancel", "request cancellation of a manager", "Send a typed cancellation request for one manager handle"),
    ):
        parser = _leaf(group, verb, summary=summary, description=description, handler=handle_manager)
        parser.add_argument("remote", metavar="REMOTE", help="the configured mount-daemon remote")
        parser.add_argument(
            "--wait-seconds",
            type=float,
            default=10.0,
            metavar="SECONDS",
            help="bound the mailbox wait (0.05 to 120 seconds; default: 10)",
        )
        if verb == "cancel":
            parser.add_argument("--handle", required=True, metavar="HANDLE", help="broker-issued manager handle")
            parser.add_argument(
                "--request-id",
                metavar="ID",
                help="32 lowercase hexadecimal request ID (default: generated); reuse one to keep waiting for a request",
            )
        else:
            group_options = parser.add_mutually_exclusive_group()
            group_options.add_argument(
                "--handle",
                metavar="HANDLE",
                help="broker-issued manager handle (signed manager-status request)",
            )
            group_options.add_argument(
                "--request-id",
                metavar="ID",
                help="original 32-digit job command ID (send a fresh signed job-status request)",
            )
        parser.set_defaults(daemon_verb=verb)
