"""Strict file protocol of confined launches between a job, its manager and the rank helpers.

Under ``manager.confine=bwrap`` an attempt's ``HTTK_WORKFLOW_LAUNCH`` is the launch client. The client
publishes a :class:`LaunchRequest` in ``launch/`` of the attempt control directory, the manager starts the
rendered launch template around the rank helper and answers with a :class:`LaunchStatus`, and every rank
helper reads its :class:`TrustedLaunch` from a manager-owned directory that jobs can only read.

All three documents are canonical JSON: UTF-8 without a BOM, no duplicate keys, no non-finite numbers, the
exact field set, keys sorted, no insignificant whitespace and non-ASCII escaped. A decoder refuses any
document that is not byte-for-byte the canonical encoding of the value it decodes to.

This module uses the standard library only: the launch client and the inner exec import nothing else.
"""

import errno
import json
import os
import re
import secrets
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

#: The directory of launch files inside the attempt control directory.
LAUNCH_DIRECTORY = "launch"
#: The ``format`` member of a launch request.
REQUEST_FORMAT = "httk-workflow-launch-request"
#: The ``format`` member of a launch status.
STATUS_FORMAT = "httk-workflow-launch-status"
#: The ``format`` member of a trusted launch description.
TRUSTED_FORMAT = "httk-workflow-launch"
#: The version of all three documents.
FORMAT_VERSION = 1
#: The largest launch request document in bytes.
MAX_REQUEST_BYTES = 1024 * 1024
#: The largest launch status document in bytes.
MAX_STATUS_BYTES = 16 * 1024
#: The largest trusted launch description in bytes.
MAX_TRUSTED_BYTES = 1024 * 1024
#: The most argument vector entries of a request.
MAX_ARGUMENTS = 1024
#: The largest argument vector entry in bytes.
MAX_ARGUMENT_BYTES = 64 * 1024
#: The largest request ``cwd`` in bytes.
MAX_CWD_BYTES = 4096
#: The most environment entries of a request or of the trusted rank environment.
MAX_ENVIRONMENT_ENTRIES = 2048
#: The largest environment value in bytes.
MAX_ENVIRONMENT_VALUE_BYTES = 128 * 1024
#: The largest status ``error`` in bytes.
MAX_ERROR_BYTES = 4096
#: The largest absolute path in a trusted launch description in bytes.
MAX_PATH_BYTES = 4096
#: The most entries of each path list in a trusted launch description.
MAX_PATH_ENTRIES = 1024
#: Environment names a request never carries: process-manager, scheduler and *httk* variables.
RESERVED_ENVIRONMENT_PREFIXES = (
    "PMI_",
    "PMIX_",
    "OMPI_",
    "OPAL_",
    "SLURM_",
    "SLURMD_",
    "SRUN_",
    "SBATCH_",
    "SALLOC_",
    "HTTK_",
)
#: The states of a launch status.
LAUNCH_STATES = ("refused", "exited", "stopped", "uncertain")

type LaunchState = Literal["refused", "exited", "stopped", "uncertain"]

_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,255}\Z")
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_READ_CHUNK = 64 * 1024


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    raise ValueError("nonfinite JSON value")


def _validate_unicode(value: object) -> None:
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, list):
        for item in value:
            _validate_unicode(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _validate_unicode(key)
            _validate_unicode(item)


def _decode_object(data: bytes, kind: str, limit: int) -> dict[str, object]:
    if type(data) is not bytes or len(data) > limit:
        raise ValueError(f"invalid {kind} document")
    if any(data.startswith(bom) for bom in _BOMS):
        raise ValueError(f"{kind} must be UTF-8 without a BOM")
    try:
        value = json.loads(
            data.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
        _validate_unicode(value)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError(f"invalid {kind} document") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{kind} must be a JSON object")
    return value


def _encode_object(value: dict[str, object], kind: str, limit: int) -> bytes:
    try:
        data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError(f"{kind} cannot be encoded") from exc
    if len(data) > limit:
        raise ValueError(f"{kind} document exceeds {limit} bytes")
    return data


def _require_canonical(data: bytes, encoded: bytes, kind: str) -> None:
    if data != encoded:
        raise ValueError(f"{kind} document is not canonically encoded")


def _check_header(value: dict[str, object], kind: str, fmt: str, fields: set[str]) -> None:
    if set(value) != fields:
        raise ValueError(f"{kind} fields are missing or unknown")
    version = value["format_version"]
    if value["format"] != fmt or type(version) is not int or version != FORMAT_VERSION:
        raise ValueError(f"unsupported {kind} format or version")


def _identifier(value: object, name: str) -> str:
    if type(value) is not str or _ID_PATTERN.fullmatch(value) is None:
        raise ValueError(f"invalid {name}: expected 32 lowercase hexadecimal digits")
    return value


def _uuid(value: object, name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"invalid {name}")
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except ValueError as exc:
        raise ValueError(f"invalid {name}: expected a canonical UUID") from exc
    return value


def _bounded_string(value: object, name: str, limit: int, *, empty: bool = True) -> str:
    if type(value) is not str or "\0" in value or (not empty and not value):
        raise ValueError(f"invalid {name}")
    try:
        size = len(value.encode("utf-8", errors="strict"))
    except UnicodeError as exc:
        raise ValueError(f"invalid {name}") from exc
    if size > limit:
        raise ValueError(f"{name} exceeds {limit} bytes")
    return value


def _relative_path(value: object, name: str, *, root: str) -> str:
    text = _bounded_string(value, name, MAX_CWD_BYTES, empty=root == "")
    if text == root:
        return text
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != text or text == ".":
        raise ValueError(f"{name} must be {root!r} or a canonical relative path without '..'")
    return text


def _absolute_path(value: object, name: str) -> Path:
    text = _bounded_string(value, name, MAX_PATH_BYTES, empty=False)
    path = PurePosixPath(text)
    if not path.is_absolute() or ".." in path.parts or path.as_posix() != text or text.startswith("//"):
        raise ValueError(f"{name} must be a canonical absolute path without '..'")
    return Path(text)


def _path_list(value: object, name: str) -> tuple[Path, ...]:
    if not isinstance(value, tuple) or len(value) > MAX_PATH_ENTRIES:
        raise ValueError(f"{name} must be a tuple of at most {MAX_PATH_ENTRIES} paths")
    return tuple(_absolute_path(item, name) for item in value)


def is_reserved_environment(name: str) -> bool:
    """Return whether a request may not carry the environment variable *name*.

    :param name: The variable name.
    :return: Whether the name has one of :data:`RESERVED_ENVIRONMENT_PREFIXES`.
    """

    return name.startswith(RESERVED_ENVIRONMENT_PREFIXES)


def is_environment_name(name: str) -> bool:
    """Return whether *name* is a variable name the launch protocol can carry.

    :param name: The variable name.
    :return: Whether it matches ``[A-Za-z_][A-Za-z0-9_]{0,255}``.
    """

    return _ENVIRONMENT_NAME.fullmatch(name) is not None


def _environment(entries: object, name: str, *, reserved: bool) -> tuple[tuple[str, str], ...]:
    if not isinstance(entries, tuple) or len(entries) > MAX_ENVIRONMENT_ENTRIES:
        raise ValueError(f"{name} must be a tuple of at most {MAX_ENVIRONMENT_ENTRIES} pairs")
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise ValueError(f"{name} entries must be pairs")
        key = _bounded_string(entry[0], f"{name} name", 256, empty=False)
        text = _bounded_string(entry[1], f"{name} value", MAX_ENVIRONMENT_VALUE_BYTES)
        if not is_environment_name(key):
            raise ValueError(f"invalid {name} name {key!r}")
        if reserved and is_reserved_environment(key):
            raise ValueError(f"reserved {name} name {key!r}")
        if key in seen:
            raise ValueError(f"duplicate {name} name {key!r}")
        seen.add(key)
        result.append((key, text))
    return tuple(sorted(result))


def _pairs(value: object, name: str) -> tuple[tuple[object, object], ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list of pairs")
    result: list[tuple[object, object]] = []
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            raise ValueError(f"{name} entries must be pairs")
        result.append((item[0], item[1]))
    return tuple(result)


def new_request_id() -> str:
    """Return a fresh random launch request identifier.

    :return: 32 lowercase hexadecimal digits.
    """

    return secrets.token_hex(16)


def check_request_id(value: object) -> str:
    """Validate a launch request identifier.

    :param value: The candidate identifier.
    :return: The identifier.
    :raises ValueError: If it is not 32 lowercase hexadecimal digits.
    """

    return _identifier(value, "request_id")


def check_attempt_id(value: object) -> str:
    """Validate an attempt identifier.

    :param value: The candidate identifier.
    :return: The identifier.
    :raises ValueError: If it is not a canonical UUID.
    """

    return _uuid(value, "attempt_id")


def lock_name(request_id: str) -> str:
    """Return the name of the client's lock file in ``launch/``.

    :param request_id: The request identifier.
    :return: ``ID.lock``.
    """

    return f"{check_request_id(request_id)}.lock"


def request_name(request_id: str) -> str:
    """Return the name of the published request in ``launch/``.

    :param request_id: The request identifier.
    :return: ``ID.request.json``.
    """

    return f"{check_request_id(request_id)}.request.json"


def request_temporary_name(request_id: str) -> str:
    """Return the name the client writes the request under before renaming it into place.

    :param request_id: The request identifier.
    :return: ``.ID.request.tmp``.
    """

    return f".{check_request_id(request_id)}.request.tmp"


def stop_name(request_id: str) -> str:
    """Return the name of the stop marker the client creates when it is signalled.

    :param request_id: The request identifier.
    :return: ``ID.stop``.
    """

    return f"{check_request_id(request_id)}.stop"


def stdout_name(request_id: str) -> str:
    """Return the name of the launch's standard output, created by the manager.

    :param request_id: The request identifier.
    :return: ``ID.stdout``.
    """

    return f"{check_request_id(request_id)}.stdout"


def stderr_name(request_id: str) -> str:
    """Return the name of the launch's standard error, created by the manager.

    :param request_id: The request identifier.
    :return: ``ID.stderr``.
    """

    return f"{check_request_id(request_id)}.stderr"


def status_name(request_id: str) -> str:
    """Return the name of the launch status, written atomically by the manager.

    :param request_id: The request identifier.
    :return: ``ID.status.json``.
    """

    return f"{check_request_id(request_id)}.status.json"


def trusted_name(attempt_id: str, request_id: str) -> str:
    """Return the name of a launch's trusted directory below ``managers/<manager_id>/launches/``.

    Request identifiers are chosen by jobs and visible to other jobs, so the name also carries the attempt.

    :param attempt_id: The attempt identifier.
    :param request_id: The request identifier.
    :return: ``<attempt_id>.<request_id>``.
    """

    return f"{check_attempt_id(attempt_id)}.{check_request_id(request_id)}"


def request_relative_path(attempt_id: str, request_id: str) -> str:
    """Return the request's path relative to the job directory.

    :param attempt_id: The attempt identifier.
    :param request_id: The request identifier.
    :return: ``attempts/<attempt_id>/launch/ID.request.json``.
    """

    return f"attempts/{check_attempt_id(attempt_id)}/{LAUNCH_DIRECTORY}/{request_name(request_id)}"


def read_bounded(directory_fd: int, name: str, limit: int) -> bytes:
    """Read a bounded regular file below a directory descriptor without following or blocking.

    :param directory_fd: The directory descriptor.
    :param name: One entry name in that directory.
    :param limit: The largest accepted size in bytes.
    :return: The file content.
    :raises ValueError: If the entry is a symlink, not a regular file, or larger than *limit*.
    :raises OSError: If the entry is missing or cannot be read.
    """

    if not name or "/" in name or name in (".", ".."):
        raise ValueError(f"invalid entry name {name!r}")
    try:
        descriptor = os.open(name, _READ_FLAGS, dir_fd=directory_fd)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENXIO):
            raise ValueError(f"{name} is a symlink or special file") from exc
        raise
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{name} is not a regular file")
        data = bytearray()
        while chunk := os.read(descriptor, min(_READ_CHUNK, limit + 1 - len(data))):
            data.extend(chunk)
            if len(data) > limit:
                raise ValueError(f"{name} exceeds {limit} bytes")
        return bytes(data)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class LaunchRequest:
    """One launch requested by a confined attempt.

    :param request_id: The random request identifier, 32 lowercase hexadecimal digits.
    :param attempt_id: The requesting attempt's identifier, a canonical UUID.
    :param argv: The command to launch on every rank; nonempty, with a nonempty first entry.
    :param cwd: The working directory relative to the job directory: ``.`` for the job directory itself,
        otherwise a canonical relative path without ``..``.
    :param environment: Environment entries added on every rank, without reserved names; sorted by name.
    """

    request_id: str
    attempt_id: str
    argv: tuple[str, ...]
    cwd: str
    environment: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        """Validate and canonicalize the members."""

        check_request_id(self.request_id)
        check_attempt_id(self.attempt_id)
        if not isinstance(self.argv, tuple) or not self.argv or len(self.argv) > MAX_ARGUMENTS:
            raise ValueError(f"argv must be a tuple of 1 to {MAX_ARGUMENTS} entries")
        for index, argument in enumerate(self.argv):
            _bounded_string(argument, "argv entry", MAX_ARGUMENT_BYTES, empty=index != 0)
        _relative_path(self.cwd, "cwd", root=".")
        object.__setattr__(self, "environment", _environment(self.environment, "environment", reserved=True))


def encode_request(request: LaunchRequest) -> bytes:
    """Encode one launch request canonically.

    :param request: The validated request.
    :return: Canonical ASCII JSON bytes.
    :raises ValueError: If the value is not a request or exceeds :data:`MAX_REQUEST_BYTES`.
    """

    if type(request) is not LaunchRequest:
        raise ValueError("request must be a LaunchRequest")
    return _encode_object(
        {
            "format": REQUEST_FORMAT,
            "format_version": FORMAT_VERSION,
            "request_id": request.request_id,
            "attempt_id": request.attempt_id,
            "argv": list(request.argv),
            "cwd": request.cwd,
            "environment": [list(entry) for entry in request.environment],
        },
        "launch request",
        MAX_REQUEST_BYTES,
    )


def decode_request(data: bytes) -> LaunchRequest:
    """Decode one strict canonical launch request.

    :param data: The raw document.
    :return: The validated request.
    :raises ValueError: If the document is malformed, not canonical or outside the protocol.
    """

    value = _decode_object(data, "launch request", MAX_REQUEST_BYTES)
    fields = {"format", "format_version", "request_id", "attempt_id", "argv", "cwd", "environment"}
    _check_header(value, "launch request", REQUEST_FORMAT, fields)
    argv = value["argv"]
    if not isinstance(argv, list):
        raise ValueError("argv must be a list")
    request = LaunchRequest(
        request_id=value["request_id"],  # type: ignore[arg-type]
        attempt_id=value["attempt_id"],  # type: ignore[arg-type]
        argv=tuple(argv),
        cwd=value["cwd"],  # type: ignore[arg-type]
        environment=_pairs(value["environment"], "environment"),  # type: ignore[arg-type]
    )
    _require_canonical(data, encode_request(request), "launch request")
    return request


@dataclass(frozen=True, slots=True)
class LaunchStatus:
    """The manager's final answer to one launch request.

    :param request_id: The request identifier.
    :param state: ``refused`` (never started), ``exited`` (the launch exited by itself), ``stopped`` (the
        manager stopped it) or ``uncertain`` (it could not be confirmed reaped).
    :param exit_code: The launch's exit status from 0 to 255, ``128+N`` for signal ``N``; required for
        ``exited``, optional for ``stopped`` and absent (``None``) for ``refused`` and ``uncertain``.
    :param error: An optional diagnostic of at most :data:`MAX_ERROR_BYTES` bytes.
    """

    request_id: str
    state: LaunchState
    exit_code: int | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        """Validate the members."""

        check_request_id(self.request_id)
        if self.state not in LAUNCH_STATES:
            raise ValueError(f"launch state must be one of {', '.join(LAUNCH_STATES)}")
        code = self.exit_code
        if code is not None and (type(code) is not int or not 0 <= code <= 255):
            raise ValueError("exit_code must be an integer from 0 through 255 or null")
        if self.state == "exited" and code is None:
            raise ValueError("an exited launch needs an exit_code")
        if self.state in ("refused", "uncertain") and code is not None:
            raise ValueError(f"a {self.state} launch has no exit_code")
        if self.error is not None:
            _bounded_string(self.error, "error", MAX_ERROR_BYTES)


def encode_status(status: LaunchStatus) -> bytes:
    """Encode one launch status canonically.

    :param status: The validated status.
    :return: Canonical ASCII JSON bytes.
    :raises ValueError: If the value is not a status or exceeds :data:`MAX_STATUS_BYTES`.
    """

    if type(status) is not LaunchStatus:
        raise ValueError("status must be a LaunchStatus")
    return _encode_object(
        {
            "format": STATUS_FORMAT,
            "format_version": FORMAT_VERSION,
            "request_id": status.request_id,
            "state": status.state,
            "exit_code": status.exit_code,
            "error": status.error,
        },
        "launch status",
        MAX_STATUS_BYTES,
    )


def decode_status(data: bytes) -> LaunchStatus:
    """Decode one strict canonical launch status.

    :param data: The raw document.
    :return: The validated status.
    :raises ValueError: If the document is malformed, not canonical or outside the protocol.
    """

    value = _decode_object(data, "launch status", MAX_STATUS_BYTES)
    fields = {"format", "format_version", "request_id", "state", "exit_code", "error"}
    _check_header(value, "launch status", STATUS_FORMAT, fields)
    status = LaunchStatus(
        request_id=value["request_id"],  # type: ignore[arg-type]
        state=value["state"],  # type: ignore[arg-type]
        exit_code=value["exit_code"],  # type: ignore[arg-type]
        error=value["error"],  # type: ignore[arg-type]
    )
    _require_canonical(data, encode_status(status), "launch status")
    return status


@dataclass(frozen=True, slots=True)
class LaunchConfinement:
    """The rank sandbox settings of one trusted launch, fixed by the manager.

    :param bwrap: The Bubblewrap executable.
    :param block_userns: Whether rank sandboxes block nested user namespaces.
    :param readonly_paths: Paths bound read-only at their own paths.
    :param devices: Device nodes bound into rank sandboxes.
    :param pmix_roots: Approved parents of the per-step PMIx directory.
    :param shm_root: The node-local parent of the per-launch shared-memory directory.
    :param environment: ``confine.environment.*`` variables set in rank sandboxes, sorted by name.
    """

    bwrap: Path
    block_userns: bool
    readonly_paths: tuple[Path, ...]
    devices: tuple[Path, ...]
    pmix_roots: tuple[Path, ...]
    shm_root: Path
    environment: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        """Validate and canonicalize the members."""

        object.__setattr__(self, "bwrap", _absolute_path(_path_text(self.bwrap), "confine.bwrap"))
        if type(self.block_userns) is not bool:
            raise ValueError("confine.block_userns must be a boolean")
        for name in ("readonly_paths", "devices", "pmix_roots"):
            value = getattr(self, name)
            texts = tuple(_path_text(item) for item in value) if isinstance(value, tuple) else value
            object.__setattr__(self, name, _path_list(texts, f"confine.{name}"))
        object.__setattr__(self, "shm_root", _absolute_path(_path_text(self.shm_root), "confine.shm_root"))
        object.__setattr__(self, "environment", _environment(self.environment, "confine.environment", reserved=False))


def _path_text(value: object) -> object:
    return str(value) if isinstance(value, Path) else value


@dataclass(frozen=True, slots=True)
class TrustedLaunch:
    """The manager-owned description of one launch, read by every rank helper.

    :param request_id: The request identifier.
    :param attempt_id: The attempt identifier.
    :param workspace_id: The workspace identifier, a canonical UUID.
    :param workspace_root: The workspace root as an absolute path.
    :param placement: The job's placement relative to the workspace root, or ``""`` for none.
    :param job_key: The job key naming the job directory.
    :param request: The request path relative to the job directory, as :func:`request_relative_path`.
    :param confine: The rank sandbox settings.
    :param python: The interpreter that runs the inner exec inside each rank sandbox.
    :param token: A random identifier the manager chose for this launch, 32 lowercase hexadecimal digits;
        it names the per-launch shared-memory directory, which a job-chosen request identifier must not.
    """

    request_id: str
    attempt_id: str
    workspace_id: str
    workspace_root: Path
    placement: str
    job_key: str
    request: str
    confine: LaunchConfinement
    python: Path
    token: str

    def __post_init__(self) -> None:
        """Validate and canonicalize the members."""

        check_request_id(self.request_id)
        check_attempt_id(self.attempt_id)
        _identifier(self.token, "token")
        _uuid(self.workspace_id, "workspace_id")
        object.__setattr__(self, "workspace_root", _absolute_path(_path_text(self.workspace_root), "workspace_root"))
        _relative_path(self.placement, "placement", root="")
        key = _bounded_string(self.job_key, "job_key", 255, empty=False)
        if "/" in key or key in (".", ".."):
            raise ValueError("job_key must be one path component")
        if self.request != request_relative_path(self.attempt_id, self.request_id):
            raise ValueError("request must be attempts/<attempt_id>/launch/<request_id>.request.json")
        if type(self.confine) is not LaunchConfinement:
            raise ValueError("confine must be a LaunchConfinement")
        object.__setattr__(self, "python", _absolute_path(_path_text(self.python), "python"))


def encode_trusted(launch: TrustedLaunch) -> bytes:
    """Encode one trusted launch description canonically.

    :param launch: The validated description.
    :return: Canonical ASCII JSON bytes.
    :raises ValueError: If the value is not a description or exceeds :data:`MAX_TRUSTED_BYTES`.
    """

    if type(launch) is not TrustedLaunch:
        raise ValueError("launch must be a TrustedLaunch")
    confine = launch.confine
    return _encode_object(
        {
            "format": TRUSTED_FORMAT,
            "format_version": FORMAT_VERSION,
            "request_id": launch.request_id,
            "attempt_id": launch.attempt_id,
            "workspace_id": launch.workspace_id,
            "workspace_root": str(launch.workspace_root),
            "placement": launch.placement,
            "job_key": launch.job_key,
            "request": launch.request,
            "confine": {
                "bwrap": str(confine.bwrap),
                "block_userns": confine.block_userns,
                "readonly_paths": [str(path) for path in confine.readonly_paths],
                "devices": [str(path) for path in confine.devices],
                "pmix_roots": [str(path) for path in confine.pmix_roots],
                "shm_root": str(confine.shm_root),
                "environment": [list(entry) for entry in confine.environment],
            },
            "python": str(launch.python),
            "token": launch.token,
        },
        "trusted launch",
        MAX_TRUSTED_BYTES,
    )


def _string_list(value: object, name: str) -> tuple[object, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    return tuple(value)


def decode_trusted(data: bytes) -> TrustedLaunch:
    """Decode one strict canonical trusted launch description.

    :param data: The raw document.
    :return: The validated description.
    :raises ValueError: If the document is malformed, not canonical or outside the protocol.
    """

    value = _decode_object(data, "trusted launch", MAX_TRUSTED_BYTES)
    fields = {
        "format",
        "format_version",
        "request_id",
        "attempt_id",
        "workspace_id",
        "workspace_root",
        "placement",
        "job_key",
        "request",
        "confine",
        "python",
        "token",
    }
    _check_header(value, "trusted launch", TRUSTED_FORMAT, fields)
    raw = value["confine"]
    confine_fields = {"bwrap", "block_userns", "readonly_paths", "devices", "pmix_roots", "shm_root", "environment"}
    if not isinstance(raw, dict) or set(raw) != confine_fields:
        raise ValueError("confine fields are missing or unknown")
    confine = LaunchConfinement(
        bwrap=raw["bwrap"],
        block_userns=raw["block_userns"],
        readonly_paths=_string_list(raw["readonly_paths"], "confine.readonly_paths"),  # type: ignore[arg-type]
        devices=_string_list(raw["devices"], "confine.devices"),  # type: ignore[arg-type]
        pmix_roots=_string_list(raw["pmix_roots"], "confine.pmix_roots"),  # type: ignore[arg-type]
        shm_root=raw["shm_root"],
        environment=_pairs(raw["environment"], "confine.environment"),  # type: ignore[arg-type]
    )
    launch = TrustedLaunch(
        request_id=value["request_id"],  # type: ignore[arg-type]
        attempt_id=value["attempt_id"],  # type: ignore[arg-type]
        workspace_id=value["workspace_id"],  # type: ignore[arg-type]
        workspace_root=value["workspace_root"],  # type: ignore[arg-type]
        placement=value["placement"],  # type: ignore[arg-type]
        job_key=value["job_key"],  # type: ignore[arg-type]
        request=value["request"],  # type: ignore[arg-type]
        confine=confine,
        python=value["python"],  # type: ignore[arg-type]
        token=value["token"],  # type: ignore[arg-type]
    )
    _require_canonical(data, encode_trusted(launch), "trusted launch")
    return launch
