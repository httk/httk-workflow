"""Bounded mounted-mailbox client for the confined workspace daemon."""

import base64
import fcntl
import json
import logging
import math
import os
import re
import stat
import tempfile
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Self, cast

from httk.core.identity import identity_public_key
from httk.core.userdirs import data_home

from ._daemon_auth import sign_request, verify_request, verify_response
from ._daemon_mailbox import MailboxDirectory
from ._daemon_protocol import Request, Response, decode_request, decode_response, encode_request, request_digest

_LOGGER = logging.getLogger(__name__)
_SETTING_NAMES = frozenset({"exchange", "daemon_workspace_id", "daemon_enrollment_id", "daemon_public_key"})
_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_CONFIGURATION_PATTERN = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ENDPOINT_FORMAT = "httk-workspace-daemon-endpoint"
_ENDPOINT_FORMAT_VERSION = 2
_ENDPOINT_FILE = "endpoint.json"
_MAX_ENDPOINT_BYTES = 64 * 1024
_SUBDIRECTORIES = ("requests", "responses", "inbox", "outbox")
_POLL_INTERVAL = 0.05
_OUTCOMES = {
    "health": frozenset({"ready", "refused", "busy"}),
    "start_manager": frozenset({"submitted", "uncertain", "refused", "busy"}),
    "manager_status": frozenset({"status", "refused", "busy"}),
    "cancel_manager": frozenset({"cancel_requested", "refused", "busy"}),
}
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")
_CACHE_FORMAT = "httk-workspace-daemon-request-cache"
_CACHE_FORMAT_VERSION = 1
_MAX_CACHE_DOCUMENT_BYTES = 16 * 1024


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


def _canonical_public_key(value: object, name: str) -> str:
    """Return one canonical Ed25519 public key."""

    if not isinstance(value, str) or not value.startswith("ed25519:"):
        raise ValueError(f"{name} must be a canonical Ed25519 public key")
    encoded = value.removeprefix("ed25519:")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError(f"{name} must be a canonical Ed25519 public key") from exc
    if len(raw) != 32 or base64.b64encode(raw).decode("ascii") != encoded:
        raise ValueError(f"{name} must be a canonical Ed25519 public key")
    return value


def _configurations(value: object) -> Mapping[str, str]:
    """Return an immutable validated approved-configuration catalog."""

    if not isinstance(value, Mapping):
        raise ValueError("configurations must be an object")
    result: dict[str, str] = {}
    for name, digest in value.items():
        if type(name) is not str or _CONFIGURATION_PATTERN.fullmatch(name) is None:
            raise ValueError("invalid daemon configuration name")
        if type(digest) is not str or _DIGEST_PATTERN.fullmatch(digest) is None:
            raise ValueError(f"invalid daemon configuration digest for {name!r}")
        result[name] = digest
    return MappingProxyType(result)


def _request_max_age(value: object) -> int:
    """Return a validated request maximum age."""

    if type(value) is not int or not 1 <= value <= 86_400:
        raise ValueError("request_max_age must be an integer from 1 through 86400")
    return value


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


def read_endpoint(exchange: Path) -> dict[str, object]:
    """Read and strictly validate ``endpoint.json`` of one mounted exchange directory.

    :param exchange: Absolute path of the mounted exchange directory.
    :return: The validated endpoint document, with ``configurations`` as an immutable mapping.
    :raises OSError: If the directory or file cannot be opened without following symlinks.
    :raises ValueError: If the file is not a bounded, strict, valid version 2 endpoint document.
    """

    with MailboxDirectory(exchange) as directory:
        document = os.open(
            _ENDPOINT_FILE,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory._require_open(),
        )
        try:
            if not stat.S_ISREG(os.fstat(document).st_mode):
                raise ValueError("daemon endpoint is not a regular file")
            data = bytearray()
            while len(data) <= _MAX_ENDPOINT_BYTES:
                chunk = os.read(document, _MAX_ENDPOINT_BYTES + 1 - len(data))
                if not chunk:
                    break
                data.extend(chunk)
        finally:
            os.close(document)
    if len(data) > _MAX_ENDPOINT_BYTES:
        raise ValueError("daemon endpoint exceeds 65536 bytes")
    raw = bytes(data)
    if any(raw.startswith(bom) for bom in _BOMS):
        raise ValueError("daemon endpoint must be UTF-8 without a BOM")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid daemon endpoint document") from exc
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
        raise ValueError("invalid daemon endpoint fields")
    if value["format"] != _ENDPOINT_FORMAT:
        raise ValueError("invalid daemon endpoint format")
    if type(value["format_version"]) is not int or value["format_version"] != _ENDPOINT_FORMAT_VERSION:
        raise ValueError("unsupported daemon endpoint version")
    _canonical_uuid(value["workspace_id"], "workspace_id")
    if type(value["enrollment_id"]) is not str or _ID_PATTERN.fullmatch(value["enrollment_id"]) is None:
        raise ValueError("enrollment_id must be 32 lower-case hexadecimal characters")
    _canonical_public_key(value["daemon_public_key"], "daemon_public_key")
    value["configurations"] = _configurations(value["configurations"])
    value["request_max_age"] = _request_max_age(value["request_max_age"])
    return value


_MAX_PASSIVE_BYTES = 1024 * 1024
_STRING_LISTS: dict[str, dict[str, tuple[str, ...]]] = {
    "status": {
        "jobs": ("job_id", "job_key", "state"),
        "rejected": ("name", "reason"),
        "eject_errors": ("job_id", "reason"),
    },
    "managers": {"managers": ("handle", "profile", "request_id", "state")},
}


def _read_passive_file(directory: MailboxDirectory, name: str) -> object | None:
    """Read one bounded strict JSON file of the outbox, or ``None`` when it is absent."""

    try:
        document = os.open(
            name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory._require_open()
        )
    except FileNotFoundError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(document).st_mode):
            raise ValueError(f"{name} is not a regular file")
        data = bytearray()
        while len(data) <= _MAX_PASSIVE_BYTES:
            chunk = os.read(document, _MAX_PASSIVE_BYTES + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
    finally:
        os.close(document)
    if len(data) > _MAX_PASSIVE_BYTES:
        raise ValueError(f"{name} exceeds {_MAX_PASSIVE_BYTES} bytes")
    raw = bytes(data)
    if any(raw.startswith(bom) for bom in _BOMS):
        raise ValueError(f"{name} must be UTF-8 without a BOM")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"{name} is not valid strict JSON") from exc
    return value


def _check_passive(name: str, value: object, kind: str, identity: tuple[str, str]) -> dict[str, object]:
    """Validate the shape and pinned identity of one passive status document."""

    lists = _STRING_LISTS[kind]
    fields = {"format", "format_version", "generated_at", identity[0], *lists}
    if kind == "status":
        fields.add("truncated")
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"{name} has invalid fields")
    if value["format"] != f"httk-workspace-daemon-{kind}":
        raise ValueError(f"{name} has an invalid format")
    if type(value["format_version"]) is not int or value["format_version"] != 1:
        raise ValueError(f"{name} has an unsupported version")
    if value[identity[0]] != identity[1]:
        raise ValueError(f"{name} does not belong to the pinned daemon")
    if type(value["generated_at"]) is not str or ("truncated" in value and type(value["truncated"]) is not bool):
        raise ValueError(f"{name} has invalid fields")
    for key, keys in lists.items():
        items = value[key]
        if not isinstance(items, list) or not all(
            isinstance(item, dict) and set(item) == set(keys) and all(type(field) is str for field in item.values())
            for item in items
        ):
            raise ValueError(f"{name} has an invalid {key} list")
    return value


def read_passive_status(endpoint: "Endpoint") -> dict[str, object]:
    """Read the passive ``status.json`` and ``managers.json`` of a mounted exchange.

    Both files are untrusted informational content: they are validated for shape and pinned
    identity only, and must never be acted upon.

    :param endpoint: The pinned daemon endpoint.
    :return: ``{"status": ..., "managers": ...}``, each part ``None`` when its file is absent.
    :raises OSError: If the exchange cannot be opened safely.
    :raises ValueError: If the enrollment changed or a present file is not a valid bounded document.
    """

    endpoint.live()
    parts: dict[str, object] = {}
    with MailboxDirectory(endpoint.exchange / "outbox") as directory:
        for kind, identity in (
            ("status", ("workspace_id", endpoint.workspace_id)),
            ("managers", ("enrollment_id", endpoint.enrollment_id)),
        ):
            name = f"{kind}.json"
            value = _read_passive_file(directory, name)
            parts[kind] = None if value is None else _check_passive(name, value, kind, identity)
    return parts


def _cache_directory(endpoint: "Endpoint") -> Path:
    """Return the private cache directory for one enrollment."""

    return data_home() / "daemon-requests" / endpoint.enrollment_id


@contextmanager
def _request_lock(endpoint: "Endpoint", request_id: str) -> Iterator[None]:
    """Hold the per-request local cache lock across one critical section."""

    directory = _cache_directory(endpoint)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("daemon request cache directory is unsafe")
    descriptor = os.open(
        directory / f"{request_id}.lock",
        os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("daemon request is already active in another local caller") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _write_exclusive(path: Path, data: bytes) -> None:
    """Install exact bytes durably without replacing an existing cache entry."""

    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _read_cache_file(path: Path) -> bytes:
    """Read one bounded regular cache file without following a symlink."""

    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("daemon request cache entry is not a regular file")
        if information.st_size > _MAX_CACHE_DOCUMENT_BYTES:
            raise ValueError("daemon request cache entry is too large")
        data = bytearray()
        while len(data) <= _MAX_CACHE_DOCUMENT_BYTES:
            chunk = os.read(descriptor, _MAX_CACHE_DOCUMENT_BYTES + 1 - len(data))
            if not chunk:
                return bytes(data)
            data.extend(chunk)
            if len(data) > _MAX_CACHE_DOCUMENT_BYTES:
                raise ValueError("daemon request cache entry is too large")
        raise ValueError("daemon request cache entry is too large")
    finally:
        os.close(descriptor)


def _binding(endpoint: "Endpoint", request: Request) -> dict[str, object]:
    """Return the exact local endpoint and signer binding for a signed request."""

    return {
        "request_id": request.request_id,
        "workspace_id": endpoint.workspace_id,
        "enrollment_id": endpoint.enrollment_id,
        "daemon_public_key": endpoint.public_key,
        "operator_key": request.operator_key,
    }


def _cache_document(endpoint: "Endpoint", request: Request) -> dict[str, object]:
    """Return one signed request and endpoint binding cache envelope."""

    request_document = json.loads(encode_request(request))
    if not isinstance(request_document, dict):  # pragma: no cover - the request encoder emits an object
        raise ValueError("encoded daemon request is not an object")
    return {
        "format": _CACHE_FORMAT,
        "format_version": _CACHE_FORMAT_VERSION,
        "binding": _binding(endpoint, request),
        "request": request_document,
    }


def _cache_bytes(endpoint: "Endpoint", request: Request) -> bytes:
    """Encode one canonical signed-request cache envelope."""

    return json.dumps(_cache_document(endpoint, request), sort_keys=True, separators=(",", ":")).encode("ascii")


def _decode_cache(data: bytes, endpoint: "Endpoint") -> Request:
    """Decode and validate one exact signed-request cache envelope."""

    if any(data.startswith(bom) for bom in _BOMS):
        raise ValueError("cached daemon request must be UTF-8 without a BOM")
    try:
        value = json.loads(
            data.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid cached daemon request envelope") from exc
    if not isinstance(value, dict) or set(value) != {"format", "format_version", "binding", "request"}:
        raise ValueError("invalid cached daemon request envelope fields")
    if value["format"] != _CACHE_FORMAT or type(value["format_version"]) is not int:
        raise ValueError("unsupported cached daemon request envelope")
    if value["format_version"] != _CACHE_FORMAT_VERSION:
        raise ValueError("unsupported cached daemon request envelope")
    binding = value["binding"]
    request_document = value["request"]
    if not isinstance(binding, dict) or set(binding) != {
        "request_id",
        "workspace_id",
        "enrollment_id",
        "daemon_public_key",
        "operator_key",
    }:
        raise ValueError("invalid cached daemon request binding")
    if not isinstance(request_document, dict):
        raise ValueError("cached daemon request document must be an object")
    try:
        encoded_request = json.dumps(
            request_document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        request = decode_request(encoded_request)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("invalid cached daemon request document") from exc
    if binding != _binding(endpoint, request):
        raise ValueError("cached daemon request endpoint binding conflicts")
    if data != _cache_bytes(endpoint, request):
        raise ValueError("cached daemon request envelope is not its exact canonical encoding")
    return request


def _same_intent(saved: Request, intent: Request) -> bool:
    """Report whether two requests differ only in signature material and time."""

    return (
        saved.request_id,
        saved.workspace_id,
        saved.enrollment_id,
        saved.operation,
        saved.profile,
        saved.handle,
        saved.configuration_digest,
    ) == (
        intent.request_id,
        intent.workspace_id,
        intent.enrollment_id,
        intent.operation,
        intent.profile,
        intent.handle,
        intent.configuration_digest,
    )


def prepare_request(endpoint: "Endpoint", intent: Request) -> Request:
    """Sign and durably cache one request, or return its exact cached retry.

    :param endpoint: Endpoint whose identities and response pin bind the request.
    :param intent: Unsigned request identity and operation fields.
    :return: The exact initially signed request, including its original timestamps.
    :raises ValueError: If the intent, endpoint, cache, or local signer conflicts.
    :raises OSError: If the private cache cannot be locked or written durably.
    """

    if type(endpoint) is not Endpoint:
        raise ValueError("endpoint must be an Endpoint")
    if type(intent) is not Request:
        raise ValueError("intent must be a Request")
    if intent.workspace_id != endpoint.workspace_id or intent.enrollment_id != endpoint.enrollment_id:
        raise ValueError("request identity does not match daemon endpoint")
    if any(
        (
            intent.created_at != 0,
            intent.expires_at != 0,
            intent.operator_key is not None,
            intent.signature is not None,
        )
    ):
        raise ValueError("request intent must be unsigned and untimestamped")

    directory = _cache_directory(endpoint)
    request_path = directory / f"{intent.request_id}.json"
    with _request_lock(endpoint, intent.request_id):
        try:
            cached = _read_cache_file(request_path)
        except FileNotFoundError:
            cached = None
        except (OSError, ValueError) as exc:
            raise ValueError("cached daemon request is corrupt") from exc
        if cached is not None:
            try:
                saved = _decode_cache(cached, endpoint)
            except ValueError as exc:
                raise ValueError(f"cached daemon request is corrupt: {exc}") from exc
            if not _same_intent(saved, intent):
                raise ValueError("cached daemon request intent conflicts")
            current_key = identity_public_key()
            if current_key is None or current_key != saved.operator_key:
                raise ValueError("cached daemon request signer conflicts with the local identity")
            verify_request(saved, [current_key])
            return saved

        live = endpoint.live()
        if intent.operation == "start_manager":
            approved_digest = cast(Mapping[str, str], live["configurations"]).get(intent.profile or "")
            if approved_digest is None:
                raise ValueError(f"daemon configuration is not approved: {intent.profile!r}")
            if intent.configuration_digest != approved_digest:
                raise ValueError("daemon configuration digest does not match the approved endpoint catalog")
        signed = sign_request(intent, lifetime=min(3600, cast(int, live["request_max_age"])))
        current_key = identity_public_key()
        if current_key is None or signed.operator_key != current_key:
            raise ValueError("signed daemon request does not match the local identity")
        _write_exclusive(request_path, _cache_bytes(endpoint, signed))
        return signed


@dataclass(frozen=True, slots=True)
class Endpoint:
    """Validated mounted exchange path and pinned identities for one daemon endpoint.

    :param exchange: Absolute mounted exchange directory.
    :param workspace_id: Canonical UUID of the daemon workspace.
    :param enrollment_id: Protected daemon enrollment identifier.
    :param public_key: Pinned broker response-signing public key.
    """

    exchange: Path
    workspace_id: str
    enrollment_id: str
    public_key: str

    def __post_init__(self) -> None:
        """Refuse direct construction that bypasses endpoint invariants."""

        if not isinstance(self.exchange, Path) or not self.exchange.is_absolute() or ".." in self.exchange.parts:
            raise ValueError("exchange must be an absolute path without .. components")
        _canonical_uuid(self.workspace_id, "workspace_id")
        if type(self.enrollment_id) is not str or _ID_PATTERN.fullmatch(self.enrollment_id) is None:
            raise ValueError("enrollment_id must be 32 lower-case hexadecimal characters")
        _canonical_public_key(self.public_key, "public_key")

    @property
    def requests(self) -> Path:
        """The signed request mailbox directory."""

        return self.exchange / "requests"

    @property
    def responses(self) -> Path:
        """The signed response mailbox directory."""

        return self.exchange / "responses"

    @classmethod
    def from_settings(cls, settings: Mapping[str, object]) -> Self:
        """Build an endpoint from exactly the four daemon remote settings.

        :param settings: Adapter settings containing only daemon endpoint fields.
        :return: The validated endpoint value.
        :raises ValueError: If names, the path, or identities are invalid.
        """

        if not isinstance(settings, Mapping) or set(settings) != _SETTING_NAMES:
            raise ValueError("mount-daemon settings must contain exactly the four endpoint settings")
        enrollment_id = settings["daemon_enrollment_id"]
        if type(enrollment_id) is not str or _ID_PATTERN.fullmatch(enrollment_id) is None:
            raise ValueError("daemon_enrollment_id must be 32 lower-case hexadecimal characters")
        return cls(
            _absolute_path(settings["exchange"], "exchange"),
            _canonical_uuid(settings["daemon_workspace_id"], "daemon_workspace_id"),
            enrollment_id,
            _canonical_public_key(settings["daemon_public_key"], "daemon_public_key"),
        )

    def live(self) -> dict[str, object]:
        """Read the current ``endpoint.json`` and require the pinned identities.

        :return: The validated document; its ``configurations`` and ``request_max_age`` are current.
        :raises ValueError: If the document is invalid or the daemon enrollment changed.
        :raises OSError: If the document cannot be opened safely.
        """

        value = read_endpoint(self.exchange)
        for name, pinned in (
            ("workspace_id", self.workspace_id),
            ("enrollment_id", self.enrollment_id),
            ("daemon_public_key", self.public_key),
        ):
            if value[name] != pinned:
                raise ValueError(
                    f"daemon endpoint {name} does not match the pinned value: "
                    "the daemon enrollment changed, so reconfigure the remote"
                )
        return value

    def check(self) -> None:
        """Verify the exchange subdirectories and the live endpoint identity, reading nothing else.

        :raises OSError: If the exchange or a subdirectory cannot be opened safely.
        :raises ValueError: If the endpoint document is invalid or the daemon enrollment changed.
        """

        with MailboxDirectory(self.exchange) as exchange:
            for name in _SUBDIRECTORIES:
                os.close(
                    os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                        dir_fd=exchange._require_open(),
                    )
                )
        self.live()


def decode_matching_response(data: bytes, request: Request, *, public_key: str) -> Response:
    """Decode a response and bind it to the exact request.

    :param data: Raw response bytes.
    :param request: Request whose identity and digest must match.
    :param public_key: Pinned broker response-signing public key.
    :return: The validated matching response.
    :raises ValueError: If the response is malformed or does not match.
    """

    if type(request) is not Request:
        raise ValueError("request must be a Request")
    response = decode_response(data)
    verify_response(response, public_key)
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


def _read_response(mailbox: MailboxDirectory, name: str, request: Request, public_key: str) -> Response | None:
    """Return one matching response, or ``None`` when it is absent."""

    try:
        data = mailbox.read(name)
    except FileNotFoundError:
        return None
    return decode_matching_response(data, request, public_key=public_key)


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


def _exchange_locked(endpoint: Endpoint, request: Request, *, wait_seconds: float = 10) -> Response:
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
    verify_request(request, [request.operator_key] if request.operator_key is not None else [])
    encoded = encode_request(request)
    endpoint.check()
    name = f"{request.request_id}.json"
    published = False

    with MailboxDirectory(endpoint.requests) as requests, MailboxDirectory(endpoint.responses) as responses:
        while not published:
            existing = _read_request(requests, name, encoded)
            response = _read_response(responses, name, request, endpoint.public_key)
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
            response = _read_response(responses, name, request, endpoint.public_key)
            if response is not None:
                if response.outcome != "busy":
                    return _return_response(responses, name, response)
                _clear_stale_busy(responses, name)
            if time.monotonic() >= deadline:
                raise TimeoutError("daemon response deadline expired")
            requests.replace(name, encoded)
            published = True

        while True:
            response = _read_response(responses, name, request, endpoint.public_key)
            if response is not None:
                return _return_response(responses, name, response)
            _sleep_until_poll(deadline)


def exchange(endpoint: Endpoint, request: Request, *, wait_seconds: float = 10) -> Response:
    """Exchange one signed request while excluding another local caller with its ID.

    :param endpoint: Mounted workspace and mailbox endpoint.
    :param request: Exact signed request, normally returned by :func:`prepare_request`.
    :param wait_seconds: Total monotonic exchange deadline.
    :return: The authenticated response bound to the request.
    """

    if type(endpoint) is not Endpoint:
        raise ValueError("endpoint must be an Endpoint")
    if type(request) is not Request:
        raise ValueError("request must be a Request")
    with _request_lock(endpoint, request.request_id):
        return _exchange_locked(endpoint, request, wait_seconds=wait_seconds)


__all__ = ["Endpoint", "decode_matching_response", "exchange", "prepare_request", "read_endpoint"]
