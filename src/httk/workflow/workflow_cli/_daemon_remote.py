"""Typed CLI controls for a mounted workspace daemon."""

import argparse
import errno
import json
import math
import os
import stat
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from httk.core.cli import CLIContext

from .._daemon_cli import _anchored
from .._daemon_client import (
    Endpoint,
    decode_matching_response,
    prepare_request,
    read_endpoint,
    read_manager_log,
    read_passive_status,
)
from .._daemon_mailbox import MAX_DIRECTORY_ENTRIES, MailboxDirectory
from .._daemon_protocol import _BUNDLE_NAME, _RESERVED_NAMES, Request, encode_request, encode_response
from .._util import write_json_atomic
from ..adapters import metadata_path, read_metadata, remote_settings, resolve_remote, run_adapter
from ._common import _group, _leaf

_OPERATION_NAMES = {
    "health": "health",
    "start": "start_manager",
    "status": "manager_status",
    "cancel": "cancel_manager",
    "withdraw": "withdraw",
}
_RACED = frozenset({errno.ENOENT, errno.EEXIST, errno.ENOTEMPTY, errno.ENOTDIR, errno.EISDIR})
_POSITIVE_OUTCOMES = frozenset({"ready", "submitted", "status", "cancel_requested", "withdrawn"})


def handle_remote_daemon_configure(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Pin the identities of a mounted daemon exchange directory into a mount-daemon remote."""

    target = resolve_remote(arguments.remote, project=context.cwd)
    metadata = read_metadata(target.bundle)
    if metadata.get("kind") != "mount-daemon":
        raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
    exchange = _anchored(Path(arguments.exchange), "exchange")
    exported = read_endpoint(exchange)
    settings: dict[str, object] = {
        "exchange": str(exchange),
        "daemon_workspace_id": exported["workspace_id"],
        "daemon_enrollment_id": exported["enrollment_id"],
        "daemon_public_key": exported["daemon_public_key"],
    }
    Endpoint.from_settings(settings).check()
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


def _mount_endpoint(arguments: argparse.Namespace, context: CLIContext) -> tuple[Path, Endpoint]:
    """Return the remote bundle and pinned endpoint of a mount-daemon remote."""

    target = resolve_remote(arguments.remote, project=context.cwd)
    if read_metadata(target.bundle).get("kind") != "mount-daemon":
        raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
    settings = remote_settings(target.bundle)
    if not isinstance(settings, Mapping):
        raise ValueError("mount-daemon remote settings must be an object")
    return target.bundle, Endpoint.from_settings(settings)


def handle_remote_daemon_log(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Write the published log of one manager handle to standard output, byte for byte.

    :param arguments: The parsed ``log`` command arguments.
    :param context: The active CLI context.
    :return: 0 after the log is written.
    """

    _, endpoint = _mount_endpoint(arguments, context)
    sys.stdout.flush()
    sys.stdout.buffer.write(read_manager_log(endpoint, arguments.handle))
    sys.stdout.buffer.flush()
    return 0


def _take_back_waiting(endpoint: Endpoint, only: str | None, moved: list[str]) -> None:
    """Rename this client's not-yet-moved bundles from ``inbox`` to ``outbox/withdrawn``, appending names to ``moved``."""

    try:
        target_directory = MailboxDirectory(endpoint.exchange / "outbox/withdrawn")
    except FileNotFoundError:
        raise ValueError(
            "the exchange has no outbox/withdrawn directory; update the broker and restart it with "
            "`httk workspace daemon run …`"
        ) from None
    with MailboxDirectory(endpoint.exchange / "inbox") as source, target_directory as target:
        source_fd, target_fd = source._require_open(), target._require_open()
        if only is not None:
            names = [only]
        else:
            with os.scandir(source_fd) as entries:
                names = [entry.name for _, entry in zip(range(MAX_DIRECTORY_ENTRIES), entries, strict=False)]
        for name in sorted(names):
            if name.startswith(".") or name in _RESERVED_NAMES or _BUNDLE_NAME.fullmatch(name) is None:
                continue
            try:
                if not stat.S_ISDIR(os.stat(name, dir_fd=source_fd, follow_symlinks=False).st_mode):
                    continue
            except FileNotFoundError:
                continue
            try:
                os.stat(name, dir_fd=target_fd, follow_symlinks=False)
                continue  # the target name is taken: leave the bundle where it is
            except FileNotFoundError:
                pass
            # ponytail: no-replace rename is unavailable on network filesystems; a racing empty target directory could
            # be replaced, losing nothing. Use renameat2(RENAME_NOREPLACE) where available if that ever matters.
            try:
                os.rename(name, name, src_dir_fd=source_fd, dst_dir_fd=target_fd)
            except OSError as exc:
                if exc.errno in _RACED:
                    continue  # the broker moved it first, or the target appeared
                raise
            moved.append(name)


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

    if arguments.daemon_verb == "status" and arguments.handle is None:
        if arguments.request_id is not None:
            raise ValueError("--request-id is accepted only with --handle")
        target = resolve_remote(arguments.remote, project=context.cwd)
        if read_metadata(target.bundle).get("kind") != "mount-daemon":
            raise ValueError(f"remote {arguments.remote!r} is not a mount-daemon remote")
        settings = remote_settings(target.bundle)
        if not isinstance(settings, Mapping):
            raise ValueError("mount-daemon remote settings must be an object")
        print(json.dumps(read_passive_status(Endpoint.from_settings(settings)), indent=2, sort_keys=True))
        return 0

    verb = arguments.daemon_verb
    bundle = getattr(arguments, "bundle", None)
    if bundle is not None and (_BUNDLE_NAME.fullmatch(bundle) is None or bundle in _RESERVED_NAMES):
        raise ValueError(f"invalid bundle name: {bundle!r}")
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

    configuration = getattr(arguments, "configuration", None)
    configuration_digest = None
    if verb == "start":
        if type(configuration) is not str:
            raise ValueError("start requires a configuration name")
        configurations = cast(Mapping[str, str], endpoint.live()["configurations"])
        configuration_digest = configurations.get(configuration)
        if configuration_digest is None:
            available = ", ".join(sorted(configurations)) or "(none)"
            raise ValueError(f"unknown approved daemon configuration {configuration!r}; available: {available}")
    request_id = _request_id(arguments.request_id, required=verb in {"start", "cancel", "withdraw"})
    intent = Request(
        request_id,
        endpoint.workspace_id,
        _OPERATION_NAMES[verb],
        profile=configuration,
        handle=getattr(arguments, "handle", None),
        enrollment_id=endpoint.enrollment_id,
        configuration_digest=configuration_digest,
        bundle=bundle,
    )
    request = prepare_request(endpoint, intent)
    request_document = json.loads(encode_request(request))
    print(f"daemon request ID: {request.request_id}", file=sys.stderr)
    taken: list[str] = []
    try:
        if verb == "withdraw":
            _take_back_waiting(endpoint, bundle, taken)
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
            public_key=endpoint.public_key,
        )
        returncode = _response_returncode(response.outcome)
        if result.get("returncode") != returncode or result.get("stderr", "") != "":
            raise ValueError("daemon adapter returned an inconsistent result")
        print(encode_response(response).decode("ascii"))
        if taken:
            print(f"taken back locally: {','.join(taken)}")
        return returncode
    except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
        print(
            f"daemon request {request.request_id} has no acknowledged result: {exc}. "
            "If retrying, reuse this request ID with identical fields; do not create a new ID.",
            file=sys.stderr,
        )
        if taken:
            print(f"taken back locally: {','.join(taken)}", file=sys.stderr)
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
        summary="pin a mounted daemon exchange",
        description="Pin the daemon identities published in EXCHANGE/endpoint.json of a mounted exchange directory",
        handler=handle_remote_daemon_configure,
    )
    configure.add_argument("remote", metavar="REMOTE", help="the existing mount-daemon remote")
    configure.add_argument("--exchange", required=True, metavar="PATH", help="locally mounted exchange directory")
    log = _leaf(
        daemon_subparsers,
        "log",
        summary="print a manager's published log",
        description="Print EXCHANGE/outbox/managers/<handle>.log, which appears after the manager's Slurm job ends",
        handler=handle_remote_daemon_log,
    )
    log.add_argument("remote", metavar="REMOTE", help="the configured mount-daemon remote")
    log.add_argument("--handle", required=True, metavar="HANDLE", help="broker-issued manager handle")
    for verb, summary in (
        ("health", "check daemon readiness"),
        ("start", "start an approved manager configuration"),
        ("status", "inspect a broker-issued manager handle"),
        ("cancel", "request cancellation of a manager handle"),
        ("withdraw", "take back waiting job bundles"),
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
        elif verb == "withdraw":
            parser.add_argument("--bundle", metavar="NAME", help="take back only this bundle (default: all waiting)")
            parser.add_argument(
                "--request-id", required=True, metavar="ID", help="32 lowercase hexadecimal request ID; reuse on retry"
            )
        elif verb == "status":
            parser.add_argument(
                "--handle",
                metavar="HANDLE",
                help="broker-issued manager handle (signed request); without it, print the passive exchange status files",
            )
            parser.add_argument(
                "--request-id", metavar="ID", help="optional (only with --handle) 32 lowercase hexadecimal request ID"
            )
        else:
            parser.add_argument("--request-id", metavar="ID", help="optional 32 lowercase hexadecimal request ID")
        parser.set_defaults(daemon_verb=verb)
