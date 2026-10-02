"""Typed CLI controls for a mounted workspace daemon."""

import argparse
import json
import math
import os
import stat
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path

from httk.core.cli import CLIContext

from .._daemon_client import Endpoint, decode_matching_response, prepare_request
from .._daemon_protocol import Request, encode_request, encode_response
from .._util import write_json_atomic
from ..adapters import metadata_path, read_metadata, remote_settings, resolve_remote, run_adapter
from ._common import _group, _leaf

_OPERATION_NAMES = {
    "health": "health",
    "start": "start_manager",
    "status": "manager_status",
    "cancel": "cancel_manager",
}
_POSITIVE_OUTCOMES = frozenset({"ready", "submitted", "status", "cancel_requested"})
_ENDPOINT_FORMAT = "httk-workspace-daemon-endpoint"
_ENDPOINT_FORMAT_VERSION = 1
_MAX_ENDPOINT_BYTES = 64 * 1024
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while refusing duplicate decoded keys."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    """Refuse nonfinite JSON extensions."""

    raise ValueError("nonfinite JSON value")


def _read_endpoint_export(path: Path) -> dict[str, object]:
    """Read one bounded no-follow public endpoint export."""

    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("daemon endpoint export is not a regular file")
        if information.st_size > _MAX_ENDPOINT_BYTES:
            raise ValueError("daemon endpoint export is too large")
        data = bytearray()
        while len(data) <= _MAX_ENDPOINT_BYTES:
            chunk = os.read(descriptor, _MAX_ENDPOINT_BYTES + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _MAX_ENDPOINT_BYTES:
                raise ValueError("daemon endpoint export is too large")
    finally:
        os.close(descriptor)
    raw = bytes(data)
    if any(raw.startswith(bom) for bom in _BOMS):
        raise ValueError("daemon endpoint export must be UTF-8 without a BOM")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid daemon endpoint export") from exc
    fields = {
        "format",
        "format_version",
        "workspace_id",
        "enrollment_id",
        "daemon_public_key",
        "configurations",
        "request_max_age",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("invalid daemon endpoint export fields")
    if value["format"] != _ENDPOINT_FORMAT:
        raise ValueError("invalid daemon endpoint export format")
    if type(value["format_version"]) is not int or value["format_version"] != _ENDPOINT_FORMAT_VERSION:
        raise ValueError("unsupported daemon endpoint export version")
    return value


def handle_remote_daemon_configure(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import one trusted public endpoint export into a mount-daemon remote."""

    target = resolve_remote(arguments.remote, project=context.cwd)
    metadata = read_metadata(target.bundle)
    if metadata.get("kind") != "mount-daemon":
        raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
    exported = _read_endpoint_export(Path(arguments.endpoint))
    settings: dict[str, object] = {
        "mount_root": arguments.mount_root,
        "daemon_requests": arguments.requests,
        "daemon_responses": arguments.responses,
        "daemon_workspace_id": exported["workspace_id"],
        "daemon_enrollment_id": exported["enrollment_id"],
        "daemon_public_key": exported["daemon_public_key"],
        "daemon_configurations": exported["configurations"],
        "daemon_request_max_age": exported["request_max_age"],
    }
    Endpoint.from_settings(settings)
    result = run_adapter(target.bundle, "configure", {"settings": settings})
    if result.get("configured") is not True:
        raise ValueError("mount-daemon adapter did not confirm endpoint configuration")
    configured = metadata.setdefault("settings", {})
    if not isinstance(configured, dict):
        raise ValueError("adapter settings are not mutable JSON")
    configured.update(settings)
    write_json_atomic(metadata_path(target.bundle), metadata)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


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
    configuration = getattr(arguments, "configuration", None)
    configuration_digest = None
    if verb == "start":
        if type(configuration) is not str:
            raise ValueError("start requires a configuration name")
        configuration_digest = endpoint.configurations.get(configuration)
        if configuration_digest is None:
            available = ", ".join(sorted(endpoint.configurations)) or "(none)"
            raise ValueError(f"unknown approved daemon configuration {configuration!r}; available: {available}")
    intent = Request(
        _request_id(arguments.request_id, required=verb in {"start", "cancel"}),
        endpoint.workspace_id,
        _OPERATION_NAMES[verb],
        profile=configuration,
        handle=getattr(arguments, "handle", None),
        enrollment_id=endpoint.enrollment_id,
        configuration_digest=configuration_digest,
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
            public_key=endpoint.daemon_public_key,
        )
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
    configure = _leaf(
        daemon_subparsers,
        "configure",
        summary="import a trusted daemon endpoint",
        description="Configure local mounted paths from a trusted public daemon endpoint export",
        handler=handle_remote_daemon_configure,
    )
    configure.add_argument("remote", metavar="REMOTE", help="the existing mount-daemon remote")
    configure.add_argument("--endpoint", required=True, metavar="FILE", help="trusted public endpoint export")
    configure.add_argument("--mount-root", required=True, metavar="PATH", help="locally mounted workspace root")
    configure.add_argument("--requests", required=True, metavar="PATH", help="locally mounted request mailbox")
    configure.add_argument("--responses", required=True, metavar="PATH", help="locally mounted response mailbox")
    for verb, summary in (
        ("health", "check daemon readiness"),
        ("start", "start an approved manager configuration"),
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
            parser.add_argument(
                "--configuration",
                required=True,
                metavar="NAME",
                help="approved manager configuration",
            )
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
