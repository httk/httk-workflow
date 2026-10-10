"""Bounded mounted-mailbox client for the confined workspace daemon."""

import base64
import errno
import json
import logging
import math
import os
import re
import shutil
import stat
import tempfile
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Self, cast

from httk.core.identity import identity_public_key
from httk.core.userdirs import data_home

from . import _fs
from ._daemon_auth import sign_request, verify_request, verify_response
from ._daemon_mailbox import MailboxDirectory
from ._daemon_protocol import Request, Response, decode_request, decode_response, encode_request, request_digest

_LOGGER = logging.getLogger(__name__)
_SETTING_NAMES = frozenset({"exchange", "daemon_workspace_id", "daemon_enrollment_id", "daemon_public_key"})
_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_CONFIGURATION_PATTERN = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_DAEMON_FORMAT = "httk-workspace-daemon"
_DAEMON_FORMAT_VERSION = 1
_DAEMON_FILE = "daemon.json"
_EXCHANGE_FORMAT = "httk-workspace-exchange"
_EXCHANGE_FILE = "exchange.json"
_MAX_DOCUMENT_BYTES = 64 * 1024
_SUBDIRECTORIES = ("requests", "responses", "inbox", "outbox", "managers")
#: The content file of a request cache entry, the record directory ``daemon-requests/<enrollment>/<request id>/``.
_CACHE_RECORD = "record"
_CONFIGURE = "httk remote daemon configure REMOTE --exchange PATH"
_PIN_HINT = f"pin the daemon with '{_CONFIGURE}' (it reads EXCHANGE/daemon.json)"
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


def _read_json_file(directory: MailboxDirectory, name: str, limit: int) -> object:
    """Read one bounded strict JSON file below an open directory without following symlinks.

    :param directory: The open directory.
    :param name: The file name.
    :param limit: The maximum file size in bytes.
    :return: The decoded JSON value.
    :raises FileNotFoundError: If the file is absent.
    :raises ValueError: If it is not a bounded regular strict UTF-8 JSON file.
    """

    document = os.open(
        name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory._require_open()
    )
    try:
        if not stat.S_ISREG(os.fstat(document).st_mode):
            raise ValueError(f"{name} is not a regular file")
        data = bytearray()
        while len(data) <= limit:
            chunk = os.read(document, limit + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
    finally:
        os.close(document)
    if len(data) > limit:
        raise ValueError(f"{name} exceeds {limit} bytes")
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


def read_exchange(exchange: Path) -> dict[str, object]:
    """Read and strictly validate ``exchange.json`` of one mounted exchange directory.

    :param exchange: Absolute path of the mounted exchange directory.
    :return: The validated document (``format``, ``format_version``, ``workspace_id``).
    :raises OSError: If the directory or file cannot be opened without following symlinks.
    :raises ValueError: If the file is not a bounded, strict, valid version 1 exchange document.
    """

    with MailboxDirectory(exchange) as directory:
        try:
            value = _read_json_file(directory, _EXCHANGE_FILE, _MAX_DOCUMENT_BYTES)
        except FileNotFoundError:
            raise ValueError(
                f"{_EXCHANGE_FILE} not found in {exchange}: point the remote at the mounted WORKSPACE/exchange "
                f"of a workspace with the exchange extension and re-run '{_CONFIGURE}'"
            ) from None
    if not isinstance(value, dict) or set(value) != {"format", "format_version", "workspace_id"}:
        raise ValueError("invalid exchange.json fields")
    if value["format"] != _EXCHANGE_FORMAT:
        raise ValueError("invalid exchange.json format")
    if type(value["format_version"]) is not int or value["format_version"] != 1:
        raise ValueError("unsupported exchange.json version")
    _canonical_uuid(value["workspace_id"], "workspace_id")
    return value


def read_daemon(exchange: Path) -> dict[str, object]:
    """Read and strictly validate ``daemon.json`` of one mounted exchange directory.

    :param exchange: Absolute path of the mounted exchange directory.
    :return: The validated daemon document, with ``configurations`` as an immutable mapping.
    :raises OSError: If the directory or file cannot be opened without following symlinks.
    :raises ValueError: If the file is not a bounded, strict, valid version 1 daemon document.
    """

    with MailboxDirectory(exchange) as directory:
        value = _read_json_file(directory, _DAEMON_FILE, _MAX_DOCUMENT_BYTES)
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
        raise ValueError("invalid daemon.json fields")
    if value["format"] != _DAEMON_FORMAT:
        raise ValueError("invalid daemon.json format")
    if type(value["format_version"]) is not int or value["format_version"] != _DAEMON_FORMAT_VERSION:
        raise ValueError("unsupported daemon.json version")
    _canonical_uuid(value["workspace_id"], "workspace_id")
    if type(value["enrollment_id"]) is not str or _ID_PATTERN.fullmatch(value["enrollment_id"]) is None:
        raise ValueError("enrollment_id must be 32 lower-case hexadecimal characters")
    _canonical_public_key(value["daemon_public_key"], "daemon_public_key")
    value["configurations"] = _configurations(value["configurations"])
    value["request_max_age"] = _request_max_age(value["request_max_age"])
    return value


_MAX_PASSIVE_BYTES = 1024 * 1024
# kind -> (format, identity field, timestamp field, list name, required string fields, nullable string fields, extra)
_PASSIVE = {
    "status": (
        "httk-workspace-exchange-status",
        "workspace_id",
        "updated_at",
        "jobs",
        ("job_id", "job_key", "state"),
        (),
        ("truncated",),
    ),
    "managers": (
        "httk-workspace-daemon-managers",
        "enrollment_id",
        "generated_at",
        "managers",
        ("handle", "profile", "request_id", "state"),
        ("job_id", "scheduler_state", "exit_code", "started_at", "ended_at", "log"),
        (),
    ),
}
_PASSIVE_VERSIONS = {"status": 1, "managers": 3}
_MAX_LOG_BYTES = 1024 * 1024


def _read_passive_file(directory: MailboxDirectory, name: str) -> object | None:
    """Read one bounded strict JSON file of the exchange, or ``None`` when it is absent."""

    try:
        return _read_json_file(directory, name, _MAX_PASSIVE_BYTES)
    except FileNotFoundError:
        return None


def _check_passive(name: str, value: object, kind: str, identity: str | None) -> dict[str, object]:
    """Validate the shape and (when pinned) the identity of one passive status document."""

    fmt, identity_field, stamp, list_name, strings, nullable, extra = _PASSIVE[kind]
    if not isinstance(value, dict) or set(value) != {
        "format",
        "format_version",
        identity_field,
        stamp,
        list_name,
        *extra,
    }:
        raise ValueError(f"{name} has invalid fields")
    if value["format"] != fmt:
        raise ValueError(f"{name} has an invalid format")
    if type(value["format_version"]) is not int or value["format_version"] != _PASSIVE_VERSIONS[kind]:
        raise ValueError(f"{name} has an unsupported version")
    if identity is not None and value[identity_field] != identity:
        raise ValueError(f"{name} does not belong to the pinned daemon")
    if type(value[stamp]) is not str or type(value[identity_field]) is not str:
        raise ValueError(f"{name} has invalid fields")
    if "truncated" in extra and type(value["truncated"]) is not bool:
        raise ValueError(f"{name} has invalid fields")
    items = value[list_name]
    if not isinstance(items, list) or not all(
        isinstance(item, dict)
        and set(item) == {*strings, *nullable}
        and all(type(item[field]) is str for field in strings)
        and all(item[field] is None or type(item[field]) is str for field in nullable)
        for item in items
    ):
        raise ValueError(f"{name} has an invalid {list_name} list")
    return value


def read_passive_status(endpoint: "Endpoint") -> dict[str, object]:
    """Read the passive root ``status.json`` and ``managers.json`` of a mounted exchange.

    Both files are untrusted informational content: they are validated for shape and pinned
    identity only (``managers.json`` is identity-checked only when the enrollment is pinned), and
    must never be acted upon.

    :param endpoint: The pinned exchange.
    :return: ``{"status": ..., "managers": ...}``, each part ``None`` when its file is absent.
    :raises OSError: If the exchange cannot be opened safely.
    :raises ValueError: If the exchange identity changed or a present file is not a valid bounded document.
    """

    endpoint.check_exchange()
    parts: dict[str, object] = {}
    with MailboxDirectory(endpoint.exchange) as directory:
        for kind, identity in (("status", endpoint.workspace_id), ("managers", endpoint.enrollment_id)):
            name = f"{kind}.json"
            value = _read_passive_file(directory, name)
            parts[kind] = None if value is None else _check_passive(name, value, kind, identity)
    return parts


def read_manager_log(endpoint: "Endpoint", handle: str) -> bytes:
    """Read the published log of one manager from ``EXCHANGE/managers/<handle>.log``.

    The content is untrusted: it is returned as bytes and must never be acted upon.

    :param endpoint: The pinned daemon endpoint.
    :param handle: The 32-digit lowercase hexadecimal manager handle.
    :return: The log bytes, at most 1 MiB.
    :raises OSError: If the exchange cannot be opened safely.
    :raises ValueError: If the handle is invalid, no log is published, or the file is not a bounded regular file.
    """

    if type(handle) is not str or _ID_PATTERN.fullmatch(handle) is None:
        raise ValueError("handle must be 32 lowercase hexadecimal digits")
    endpoint.check_exchange()
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    with MailboxDirectory(endpoint.exchange / "managers") as directory:
        try:
            document = os.open(f"{handle}.log", flags | os.O_NONBLOCK, dir_fd=directory._require_open())
        except FileNotFoundError:
            raise ValueError(
                "no log has been published for this manager yet; it appears when the manager's Slurm job ends"
            ) from None
        try:
            if not stat.S_ISREG(os.fstat(document).st_mode):
                raise ValueError("manager log is not a regular file")
            data = bytearray()
            while len(data) <= _MAX_LOG_BYTES:
                chunk = os.read(document, _MAX_LOG_BYTES + 1 - len(data))
                if not chunk:
                    break
                data.extend(chunk)
        finally:
            os.close(document)
    if len(data) > _MAX_LOG_BYTES:
        raise ValueError(f"manager log exceeds {_MAX_LOG_BYTES} bytes")
    return bytes(data)


def _fsync_directory_path(path: Path) -> None:
    """Flush one directory's entries to stable storage."""

    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    """Flush every regular file and directory below ``root`` (symlinks are not followed) to stable storage."""

    for directory, _names, files in os.walk(root):
        for file in files:
            path = Path(directory, file)
            if path.is_symlink():
                continue
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        _fsync_directory_path(Path(directory))


def _present(path: Path) -> bool:
    """Report whether a path exists; an unobservable one counts as absent (the caller then stays cautious)."""

    try:
        os.lstat(path)
    except OSError:
        return False
    return True


def _ambiguous(name: str, held: str, destination: Path) -> str:
    """Describe a take-back whose publication outcome is unknown, so the original is kept."""

    return (
        f"take-back of {name!r} may or may not have been published to {destination}; the original is kept as "
        f"{held}: if the destination is complete, delete the held entry, otherwise rename it back to {name!r}"
    )


def take_back(endpoint: "Endpoint", name: str, destination: Path) -> Path:
    """Take one ejected bundle back out of ``EXCHANGE/inbox`` before a manager adopts it.

    The entry is atomically renamed to a dot name in the inbox (managers ignore dot names), copied to a
    temporary sibling of ``destination`` that is then renamed into place, and only then removed. If the
    copy fails the entry is renamed back.

    :param endpoint: The pinned exchange; the daemon pins are not needed.
    :param name: The bundle name in the inbox.
    :param destination: A not yet existing local path outside the exchange to copy the bundle to.
    :return: ``destination``.
    :raises ValueError: If the name or destination is invalid, or a manager already took the entry
        (cancel the job instead).
    :raises OSError: If a filesystem operation fails, or ``destination`` exists. A failed restore names the
        held ``.takeback-*`` entry, which is renamed back by hand.
    """

    if type(name) is not str or not name or name.startswith(".") or "/" in name or "\0" in name or len(name) > 255:
        raise ValueError("take-back name must be a plain inbox entry name that does not start with a dot")
    endpoint.check_exchange()
    destination = Path(os.path.abspath(destination))
    if os.path.lexists(destination):
        raise FileExistsError(errno.EEXIST, "take-back destination exists", str(destination))
    existing = destination
    while not os.path.lexists(existing):
        existing = existing.parent
    if Path(os.path.realpath(existing)).is_relative_to(os.path.realpath(endpoint.exchange)):
        raise ValueError("take-back destination must be outside the exchange, or a manager could adopt the copy")
    hidden = f".takeback-{uuid.uuid4().hex}"
    partial = destination.with_name(f"{destination.name}.tmp-{uuid.uuid4().hex}")
    with MailboxDirectory(endpoint.exchange / "inbox") as inbox:
        descriptor = inbox._require_open()
        held = str(endpoint.exchange / "inbox" / hidden)
        try:
            if not stat.S_ISDIR(os.stat(name, dir_fd=descriptor, follow_symlinks=False).st_mode):
                raise ValueError(f"inbox entry {name!r} is not a bundle directory")
            # Contested with the managers' take of the same entry: exactly one of the moves wins.
            moved = _fs.move_once(
                _fs.anchored(descriptor, name), _fs.anchored(descriptor, hidden), durable=True, create_parents=False
            )
            hidden_now = moved is _fs.Moved.WON
        except FileNotFoundError:
            hidden_now = False
        except _fs.MoveFailed as exc:
            raise OSError(exc.errno or errno.EIO, str(exc)) from exc
        if not hidden_now:
            raise ValueError(f"inbox entry {name!r} is gone: a manager has taken it, so cancel the job instead")
        publishing = False
        try:
            # ponytail: /proc is Linux-only; elsewhere the copy source is the path, which a swapped inbox could redirect
            source = f"/proc/self/fd/{descriptor}/{hidden}" if os.path.isdir("/proc/self/fd") else held
            shutil.copytree(source, partial, symlinks=True)
            _fsync_tree(partial)
            publishing = True
            _fs.move_owned(_fs.loc(partial), _fs.loc(destination), durable=True, create_parents=False)
        except BaseException as exc:
            # Restore only when the failure is positively established: before the publish rename, or with the
            # temporary copy still in place. Otherwise the copy may already be published (and consumed).
            if publishing and not _present(partial):
                raise OSError(_ambiguous(name, held, destination)) from exc
            try:
                _fs.discard(_fs.loc(partial), trash_dir=partial.parent, durable=False)
            except (OSError, _fs.MoveFailed):
                _LOGGER.debug("could not remove the partial take-back copy %s", partial, exc_info=True)
            try:
                _fs.move_owned(_fs.anchored(descriptor, hidden), _fs.anchored(descriptor, name), durable=True)
            except (OSError, _fs.MoveFailed) as exc:
                raise OSError(
                    f"take-back failed and {name!r} could not be restored: it is held as "
                    f"{held}; rename it back to {name!r} to re-offer it"
                ) from exc
            raise
        try:
            _fsync_directory_path(destination.parent)  # the copy must be durable before the original goes
            # The trash name goes to the exchange root, which no manager scans for bundles.
            _fs.discard(_fs.anchored(descriptor, hidden), trash_dir=endpoint.exchange, durable=True)
        except (OSError, _fs.MoveFailed):
            _LOGGER.warning(
                "take-back copied %r but could not make it durable or remove the held inbox entry %s; "
                "check the copy, then delete the held entry by hand",
                name,
                held,
                extra={"context": "daemon_take_back"},
            )
    return destination


def _cache_directory(enrollment_id: str) -> Path:
    """Return the private cache directory for one enrollment, created when absent."""

    directory = data_home() / "daemon-requests" / enrollment_id
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("daemon request cache directory is unsafe")
    return directory


def _write_exclusive(path: Path, data: bytes) -> None:
    """Install exact bytes durably as the record directory *path* (``record``), never replacing an entry.

    :param path: The cache entry.
    :param data: The exact content.
    :raises FileExistsError: If another preparer installed the entry first.
    :raises OSError: If the entry could not be published.
    """

    staging = Path(tempfile.mkdtemp(prefix=f".{path.name}.", dir=path.parent))
    nonce = uuid.uuid4().hex.encode("ascii")
    try:
        _fs.write_file(_fs.loc(staging / ".nonce"), nonce, durable=True, mode=0o600)
        _fs.write_file(_fs.loc(staging / _CACHE_RECORD), data, durable=True, mode=0o600)
        won = _fs.publish_dir(_fs.loc(staging), _fs.loc(path), nonce=nonce, durable=True)
    finally:
        if _fs.exists(_fs.loc(staging)):
            _fs.discard(_fs.loc(staging), trash_dir=path.parent, durable=False)
    if not won:
        if not _fs.exists(_fs.loc(path)):
            raise OSError(errno.EIO, "the daemon request cache entry could not be published", str(path))
        raise FileExistsError(errno.EEXIST, "daemon request cache entry exists", str(path))


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


def _adopt_cached(endpoint: "Endpoint", cached: bytes, intent: Request) -> Request:
    """Validate a cached signed request against the intent and the local signer, and return it."""

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


def prepare_request(endpoint: "Endpoint", intent: Request) -> Request:
    """Sign and durably cache one request, or return its exact cached retry.

    :param endpoint: Endpoint whose identities and response pin bind the request.
    :param intent: Unsigned request identity and operation fields.
    :return: The exact initially signed request, including its original timestamps.
    :raises ValueError: If the intent, endpoint, cache, or local signer conflicts.
    :raises OSError: If the private cache cannot be written durably.
    """

    if type(endpoint) is not Endpoint:
        raise ValueError("endpoint must be an Endpoint")
    if type(intent) is not Request:
        raise ValueError("intent must be a Request")
    enrollment_id, _ = endpoint.require_daemon()
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

    request_path = _cache_directory(enrollment_id) / intent.request_id
    # ponytail: no local lock; concurrent preparers race on publish_dir and the loser adopts the winner's entry
    # (at most one signature is ever installed; a loser may have signed once more and discards it).
    try:
        cached = _read_cache_file(request_path / _CACHE_RECORD)
    except FileNotFoundError:
        cached = None
    except (OSError, ValueError) as exc:
        raise ValueError("cached daemon request is corrupt") from exc
    if cached is not None:
        return _adopt_cached(endpoint, cached, intent)

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
    try:
        _write_exclusive(request_path, _cache_bytes(endpoint, signed))
    except FileExistsError:
        try:
            cached = _read_cache_file(request_path / _CACHE_RECORD)
        except (OSError, ValueError) as exc:
            raise ValueError("cached daemon request is corrupt") from exc
        return _adopt_cached(endpoint, cached, intent)
    return signed


@dataclass(frozen=True, slots=True)
class Endpoint:
    """Validated mounted exchange path and pinned identities for one daemon endpoint.

    :param exchange: Absolute mounted ``WORKSPACE/exchange`` directory.
    :param workspace_id: Canonical UUID of the daemon workspace.
    :param enrollment_id: Protected daemon enrollment identifier, or ``None`` when the daemon is not pinned.
    :param public_key: Pinned broker response-signing public key, or ``None`` when the daemon is not pinned.
    """

    exchange: Path
    workspace_id: str
    enrollment_id: str | None = None
    public_key: str | None = None

    def __post_init__(self) -> None:
        """Refuse direct construction that bypasses endpoint invariants."""

        if not isinstance(self.exchange, Path) or not self.exchange.is_absolute() or ".." in self.exchange.parts:
            raise ValueError("exchange must be an absolute path without .. components")
        _canonical_uuid(self.workspace_id, "workspace_id")
        if self.enrollment_id is not None and (
            type(self.enrollment_id) is not str or _ID_PATTERN.fullmatch(self.enrollment_id) is None
        ):
            raise ValueError("enrollment_id must be 32 lower-case hexadecimal characters")
        if self.public_key is not None:
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
        """Build an endpoint from the daemon remote settings.

        :param settings: Adapter settings: ``exchange`` and ``daemon_workspace_id`` are required,
            ``daemon_enrollment_id`` and ``daemon_public_key`` optional (without them signed requests are refused).
        :return: The validated endpoint value.
        :raises ValueError: If names, the path, or identities are invalid.
        """

        required = {"exchange", "daemon_workspace_id"}
        if not isinstance(settings, Mapping) or not required <= set(settings) <= _SETTING_NAMES:
            raise ValueError(
                "mount-daemon settings must name exchange and daemon_workspace_id, and optionally the daemon pins; "
                f"a remote set up for the old layout needs '{_CONFIGURE}'"
            )
        enrollment_id = settings.get("daemon_enrollment_id")
        if enrollment_id is not None and (
            type(enrollment_id) is not str or _ID_PATTERN.fullmatch(enrollment_id) is None
        ):
            raise ValueError("daemon_enrollment_id must be 32 lower-case hexadecimal characters")
        public_key = settings.get("daemon_public_key")
        return cls(
            _absolute_path(settings["exchange"], "exchange"),
            _canonical_uuid(settings["daemon_workspace_id"], "daemon_workspace_id"),
            enrollment_id,
            None if public_key is None else _canonical_public_key(public_key, "daemon_public_key"),
        )

    def require_daemon(self) -> tuple[str, str]:
        """Return the pinned daemon identity.

        :return: ``(enrollment_id, public_key)``.
        :raises ValueError: If the daemon is not pinned; the message says how to pin it.
        """

        if self.enrollment_id is None or self.public_key is None:
            raise ValueError(f"signed daemon requests need the daemon pins: {_PIN_HINT}")
        return self.enrollment_id, self.public_key

    def live(self) -> dict[str, object]:
        """Read the current ``daemon.json`` and require the pinned identities.

        :return: The validated document; its ``configurations`` and ``request_max_age`` are current.
        :raises ValueError: If the daemon is not pinned, the document is invalid, or the enrollment changed.
        :raises OSError: If the document cannot be opened safely.
        """

        enrollment_id, public_key = self.require_daemon()
        value = read_daemon(self.exchange)
        for name, pinned in (
            ("workspace_id", self.workspace_id),
            ("enrollment_id", enrollment_id),
            ("daemon_public_key", public_key),
        ):
            if value[name] != pinned:
                raise ValueError(
                    f"daemon endpoint {name} does not match the pinned value: "
                    "the daemon enrollment changed, so reconfigure the remote"
                )
        return value

    def check_exchange(self) -> None:
        """Verify the exchange subdirectories and that ``exchange.json`` names the pinned workspace.

        :raises OSError: If the exchange or a subdirectory cannot be opened safely.
        :raises ValueError: If ``exchange.json`` is invalid or belongs to another workspace.
        """

        if read_exchange(self.exchange)["workspace_id"] != self.workspace_id:
            raise ValueError("exchange.json workspace_id does not match the pinned daemon_workspace_id")
        with MailboxDirectory(self.exchange) as exchange:
            for name in _SUBDIRECTORIES:
                try:
                    os.close(
                        os.open(
                            name,
                            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                            dir_fd=exchange._require_open(),
                        )
                    )
                except FileNotFoundError:
                    raise ValueError(
                        f"exchange directory {name!r} is missing in {self.exchange}: this is not a current "
                        f"WORKSPACE/exchange; re-run '{_CONFIGURE}'"
                    ) from None

    def check(self) -> None:
        """Verify the exchange and, when the daemon is pinned, the live ``daemon.json`` identity.

        :raises OSError: If the exchange or a subdirectory cannot be opened safely.
        :raises ValueError: If a document is invalid or an identity changed.
        """

        self.check_exchange()
        if self.enrollment_id is not None or self.public_key is not None:
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
    except FileNotFoundError:
        pass  # a concurrent caller already cleared it
    except OSError as exc:
        raise RuntimeError("cannot clear stale busy daemon response") from exc


def _sleep_until_poll(deadline: float) -> None:
    """Sleep for one bounded poll interval within the original deadline."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("daemon response deadline expired")
    time.sleep(min(_POLL_INTERVAL, remaining))


def exchange(endpoint: Endpoint, request: Request, *, wait_seconds: float = 10) -> Response:
    """Publish at most one request and wait for its matching daemon response, holding no lock.

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
    _, public_key = endpoint.require_daemon()
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
            response = _read_response(responses, name, request, public_key)
            if response is not None:
                if response.outcome != "busy":
                    return _return_response(responses, name, response)
                _clear_stale_busy(responses, name)

            if existing:
                _sleep_until_poll(deadline)
                continue

            # A broker can publish a response immediately before removing the
            # matching request. Check that response-after-request window once more
            # before this caller installs a new request.
            response = _read_response(responses, name, request, public_key)
            if response is not None:
                if response.outcome != "busy":
                    return _return_response(responses, name, response)
                _clear_stale_busy(responses, name)
            if time.monotonic() >= deadline:
                raise TimeoutError("daemon response deadline expired")
            requests.replace(name, encoded)
            published = True

        while True:
            response = _read_response(responses, name, request, public_key)
            if response is not None:
                return _return_response(responses, name, response)
            _sleep_until_poll(deadline)


__all__ = [
    "Endpoint",
    "decode_matching_response",
    "exchange",
    "prepare_request",
    "read_daemon",
    "read_exchange",
    "take_back",
]
