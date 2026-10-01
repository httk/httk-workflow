"""Bounded mounted-mailbox client for the confined workspace daemon."""

import json
import logging
import math
import os
import re
import stat
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Self, cast

from ._daemon_mailbox import MailboxDirectory
from ._daemon_protocol import Request, Response, decode_request, decode_response, encode_request, request_digest

_LOGGER = logging.getLogger(__name__)
_SETTING_NAMES = frozenset(
    {
        "mount_root",
        "daemon_requests",
        "daemon_responses",
        "daemon_workspace_id",
        "daemon_enrollment_id",
    }
)
_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_WORKSPACE_CONTROL = ".httk-workspace"
_WORKSPACE_FORMAT = "format.json"
_MAX_WORKSPACE_FORMAT_BYTES = 64 * 1024
_POLL_INTERVAL = 0.05
_OUTCOMES = {
    "health": frozenset({"ready", "refused", "busy"}),
    "start_manager": frozenset({"submitted", "uncertain", "refused", "busy"}),
    "manager_status": frozenset({"status", "refused", "busy"}),
    "cancel_manager": frozenset({"cancel_requested", "refused", "busy"}),
}
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")


def _canonical_uuid(value: object, name: str) -> str:
    """Return one canonical UUID string."""

    if type(value) is not str:
        raise ValueError(f"{name} must be a canonical UUID")
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{name} must be a canonical UUID") from exc
    return value


def _absolute_path(value: object, name: str) -> Path:
    """Return one absolute lexical path without parent traversal."""

    if type(value) is not str or not value or "\0" in value:
        raise ValueError(f"{name} must be an absolute path")
    candidate = Path(value)
    if not candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"{name} must be an absolute path without .. components")
    return candidate


def _paths_overlap(first: Path, second: Path) -> bool:
    """Report whether either lexical path contains the other."""

    return first == second or first.is_relative_to(second) or second.is_relative_to(first)


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


def _validate_unicode(value: object) -> None:
    """Refuse decoded strings that cannot be encoded as UTF-8."""

    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, list):
        for item in value:
            _validate_unicode(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _validate_unicode(key)
            _validate_unicode(item)


def _read_workspace_format(descriptor: int) -> dict[str, object]:
    """Read the workspace format through a pinned workspace descriptor."""

    directory = -1
    document = -1
    try:
        directory = os.open(
            _WORKSPACE_CONTROL,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=descriptor,
        )
        document = os.open(
            _WORKSPACE_FORMAT,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory,
        )
        information = os.fstat(document)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("workspace format is not a regular file")
        if information.st_size > _MAX_WORKSPACE_FORMAT_BYTES:
            raise ValueError("workspace format exceeds 65536 bytes")
        data = bytearray()
        while len(data) <= _MAX_WORKSPACE_FORMAT_BYTES:
            chunk = os.read(document, _MAX_WORKSPACE_FORMAT_BYTES + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _MAX_WORKSPACE_FORMAT_BYTES:
                raise ValueError("workspace format exceeds 65536 bytes")
    finally:
        try:
            if document >= 0:
                os.close(document)
        finally:
            if directory >= 0:
                os.close(directory)

    raw = bytes(data)
    if any(raw.startswith(bom) for bom in _BOMS):
        raise ValueError("workspace format must be UTF-8 without a BOM")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid workspace format document") from exc
    if not isinstance(value, dict):
        raise ValueError("workspace format must be a JSON object")
    return value


@dataclass(frozen=True, slots=True)
class Endpoint:
    """Validated mounted paths and identities for one daemon endpoint.

    :param workspace: Mounted workspace data root.
    :param requests: Mounted daemon request mailbox root.
    :param responses: Mounted daemon response mailbox root.
    :param workspace_id: Canonical UUID of the mounted workspace.
    :param enrollment_id: Protected daemon enrollment identifier.
    """

    workspace: Path
    requests: Path
    responses: Path
    workspace_id: str
    enrollment_id: str

    def __post_init__(self) -> None:
        """Refuse direct construction that bypasses endpoint invariants."""

        roots = (self.workspace, self.requests, self.responses)
        if any(not isinstance(root, Path) or not root.is_absolute() or ".." in root.parts for root in roots):
            raise ValueError("endpoint roots must be absolute paths without .. components")
        if any(_paths_overlap(first, second) for index, first in enumerate(roots) for second in roots[index + 1 :]):
            raise ValueError("mount-daemon roots must be pairwise disjoint")
        _canonical_uuid(self.workspace_id, "workspace_id")
        if type(self.enrollment_id) is not str or _ID_PATTERN.fullmatch(self.enrollment_id) is None:
            raise ValueError("enrollment_id must be 32 lower-case hexadecimal characters")

    @classmethod
    def from_settings(cls, settings: Mapping[str, object]) -> Self:
        """Build an endpoint from exactly the five daemon remote settings.

        :param settings: Adapter settings containing only daemon endpoint fields.
        :return: The validated endpoint value.
        :raises ValueError: If names, paths, or identities are invalid.
        """

        if not isinstance(settings, Mapping) or set(settings) != _SETTING_NAMES:
            raise ValueError("mount-daemon settings must contain exactly the five endpoint settings")
        workspace = _absolute_path(settings["mount_root"], "mount_root")
        requests = _absolute_path(settings["daemon_requests"], "daemon_requests")
        responses = _absolute_path(settings["daemon_responses"], "daemon_responses")
        roots = (workspace, requests, responses)
        if any(_paths_overlap(first, second) for index, first in enumerate(roots) for second in roots[index + 1 :]):
            raise ValueError("mount-daemon roots must be pairwise disjoint")
        workspace_id = _canonical_uuid(settings["daemon_workspace_id"], "daemon_workspace_id")
        enrollment_id = settings["daemon_enrollment_id"]
        if type(enrollment_id) is not str or _ID_PATTERN.fullmatch(enrollment_id) is None:
            raise ValueError("daemon_enrollment_id must be 32 lower-case hexadecimal characters")
        return cls(workspace, requests, responses, workspace_id, enrollment_id)

    def check(self) -> None:
        """Open all roots without following symlinks and verify workspace identity.

        :raises OSError: If a root or workspace control file cannot be opened safely.
        :raises ValueError: If a root or workspace format document is invalid.
        """

        with (
            MailboxDirectory(self.workspace) as workspace,
            MailboxDirectory(self.requests),
            MailboxDirectory(self.responses),
        ):
            value = _read_workspace_format(workspace._require_open())
        version = value.get("format_version")
        if value.get("format") != "httk-workflow-filesystem" or type(version) is not int or version != 2:
            raise ValueError("mounted workspace uses an unsupported filesystem format")
        if value.get("workspace_id") != self.workspace_id:
            raise ValueError("mounted workspace identity does not match daemon_workspace_id")


def decode_matching_response(data: bytes, request: Request) -> Response:
    """Decode a response and bind it to the exact request.

    :param data: Raw response bytes.
    :param request: Request whose identity and digest must match.
    :return: The validated matching response.
    :raises ValueError: If the response is malformed or does not match.
    """

    if type(request) is not Request:
        raise ValueError("request must be a Request")
    response = decode_response(data)
    if response.request_id != request.request_id:
        raise ValueError("response request_id does not match request")
    if response.workspace_id != request.workspace_id:
        raise ValueError("response workspace_id does not match request")
    if response.enrollment_id != request.enrollment_id:
        raise ValueError("response enrollment_id does not match request")
    if response.request_digest != request_digest(request):
        raise ValueError("response request_digest does not match request")
    if response.outcome not in _OUTCOMES[request.operation]:
        raise ValueError("response outcome does not match request operation")
    if request.operation in {"manager_status", "cancel_manager"}:
        if response.handle is not None and response.handle != request.handle:
            raise ValueError("response handle does not match request")
    elif request.operation == "health" and response.handle is not None:
        raise ValueError("health response must not contain a handle")
    return response


def _timeout(wait_seconds: object) -> float:
    """Return a finite supported mailbox wait duration."""

    if type(wait_seconds) not in {int, float}:
        raise ValueError("wait_seconds must be a finite number from 0.05 through 120")
    try:
        value = float(cast(int | float, wait_seconds))
    except OverflowError as exc:
        raise ValueError("wait_seconds must be a finite number from 0.05 through 120") from exc
    if not math.isfinite(value) or not 0.05 <= value <= 120.0:
        raise ValueError("wait_seconds must be a finite number from 0.05 through 120")
    return value


def _read_request(mailbox: MailboxDirectory, name: str, expected: bytes) -> bool:
    """Report whether an existing request matches, or whether it is absent."""

    try:
        observed = mailbox.read(name)
    except FileNotFoundError:
        return False
    try:
        canonical = encode_request(decode_request(observed))
    except ValueError as exc:
        raise ValueError("existing request is malformed") from exc
    if canonical != expected:
        raise ValueError("existing request conflicts with this request_id")
    return True


def _read_response(mailbox: MailboxDirectory, name: str, request: Request) -> Response | None:
    """Return one matching response, or ``None`` when it is absent."""

    try:
        data = mailbox.read(name)
    except FileNotFoundError:
        return None
    return decode_matching_response(data, request)


def _remove_returned_response(mailbox: MailboxDirectory, name: str) -> None:
    """Best-effort remove a response whose validated result is already known."""

    try:
        mailbox.remove(name)
    except OSError as exc:
        _LOGGER.warning(
            "daemon response cleanup failed (%s)",
            type(exc).__name__,
            extra={"context": "daemon_response_cleanup"},
        )


def _return_response(mailbox: MailboxDirectory, name: str, response: Response) -> Response:
    """Clean up and return one already validated response."""

    _remove_returned_response(mailbox, name)
    return response


def _clear_stale_busy(mailbox: MailboxDirectory, name: str) -> None:
    """Remove a retryable response before permitting a new publication."""

    try:
        mailbox.remove(name)
    except OSError as exc:
        raise RuntimeError("cannot clear stale busy daemon response") from exc


def _sleep_until_poll(deadline: float) -> None:
    """Sleep for one bounded poll interval within the original deadline."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("daemon response deadline expired")
    time.sleep(min(_POLL_INTERVAL, remaining))


def exchange(endpoint: Endpoint, request: Request, *, wait_seconds: float = 10) -> Response:
    """Publish at most one request and wait for its matching daemon response.

    :param endpoint: Mounted workspace and mailbox endpoint.
    :param request: Exact typed daemon request to exchange.
    :param wait_seconds: Total monotonic deadline, from 0.05 through 120 seconds.
    :return: A validated response for the request.
    :raises TimeoutError: If no response arrives before the one deadline.
    :raises ValueError: If endpoint, request, existing files, or response conflict.
    :raises OSError: If mounted filesystem operations fail.
    :raises RuntimeError: If a stale busy response cannot be cleared.
    """

    started = time.monotonic()
    duration = _timeout(wait_seconds)
    deadline = started + duration
    if type(endpoint) is not Endpoint:
        raise ValueError("endpoint must be an Endpoint")
    if type(request) is not Request:
        raise ValueError("request must be a Request")
    if request.workspace_id != endpoint.workspace_id or request.enrollment_id != endpoint.enrollment_id:
        raise ValueError("request identity does not match daemon endpoint")
    encoded = encode_request(request)
    endpoint.check()
    name = f"{request.request_id}.json"
    published = False

    with MailboxDirectory(endpoint.requests) as requests, MailboxDirectory(endpoint.responses) as responses:
        while not published:
            existing = _read_request(requests, name, encoded)
            response = _read_response(responses, name, request)
            if response is not None:
                if response.outcome != "busy":
                    return _return_response(responses, name, response)
                _clear_stale_busy(responses, name)

            if existing:
                _sleep_until_poll(deadline)
                continue

            # A broker can publish a response immediately before withdrawing the
            # matching request. Check that response-after-request window once more
            # before this caller installs a new request.
            response = _read_response(responses, name, request)
            if response is not None:
                if response.outcome != "busy":
                    return _return_response(responses, name, response)
                _clear_stale_busy(responses, name)
            if time.monotonic() >= deadline:
                raise TimeoutError("daemon response deadline expired")
            requests.replace(name, encoded)
            published = True

        while True:
            response = _read_response(responses, name, request)
            if response is not None:
                return _return_response(responses, name, response)
            _sleep_until_poll(deadline)


__all__ = ["Endpoint", "decode_matching_response", "exchange"]
