"""Lock-free durable ledger of the confined workspace daemon.

The ledger is a directory, ``<state>/ledger/``, that any number of daemon
instances share without a lock. Every decision is one atomic no-replace
operation: the rename of a prepared directory onto a name that must not
exist, or a ``link(2)`` of an fsynced temporary (:func:`httk.workflow._txn.link_new`).
Every other step is repeatable, and every file is validated canonically when
it is read.

======================================  ==========================================================
Path                                    Content
======================================  ==========================================================
``format``                              format name, version, workspace and enrollment identity
``prep/<nonce>.<rand>/``                an anchor prepared before admission
``req/<id>/envelope``                   canonical JSON: the request, the admitting instance's nonce
                                        (``owner``) and a start's minted ``handle`` (in the prepared anchor)
``req/<id>/decision``                   a start's ``submit <time_ns> <nonce>`` or ``refuse <reason> <nonce>``
``req/<id>/scheduler``                  ``<job id> <cluster>``, linked by the submit winner
``req/<id>/response``                   the canonical unsigned response
``handles/<handle>``                    the request id of a manager start
``slots/record.<k>``, ``slots/start.<k>``  ``<request id> <nonce>`` claiming one unit of a quota
``observations/<handle>.json``          the last scheduler observation (last writer wins)
======================================  ==========================================================

Every instance has a random nonce. Whatever is not idempotent carries it (the
envelope's ``owner``, the ``decision``, the slots), so an instance decides
"mine" by nonce equality and never by comparing content another instance may
have produced identically.

Durability order: each file is fsynced, then its directory; the anchor is
durable before any decision, ``decision=submit`` before the scheduler is
contacted, and ``response`` before the daemon publishes it.
"""

import errno
import itertools
import json
import logging
import os
import re
import secrets
import stat
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self

from ._daemon_protocol import Request, Response, decode_request, decode_response, encode_request, encode_response
from ._daemon_protocol import request_digest as canonical_request_digest
from ._txn import _hook, birth, is_link_temporary, link_new, remove_tree, rename_verified
from ._util import fsync_directory

_LOGGER = logging.getLogger(__name__)

_FORMAT = "httk-workspace-daemon-ledger"
_FORMAT_VERSION = 2
_ROOT = "ledger"
_SUBDIRECTORIES = ("prep", "req", "handles", "slots", "observations")
#: The files of the SQLite ledger and its instance lock that this ledger replaced.
_LEGACY = ("ledger.sqlite3", "daemon.lock")
_ANCHOR_FILES = frozenset({"envelope", "decision", "scheduler", "response"})
_HEX32 = re.compile(r"[0-9a-f]{32}\Z")
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_CLUSTER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_REASON = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_TIME_NS = re.compile(r"0|[1-9][0-9]{0,18}\Z")
_PREPARED = re.compile(r"([0-9a-f]{32})\.[0-9a-f]{16}\Z")
#: The prefix a prepared anchor is fenced to before the leftover sweep removes it.
_STALE = ".stale."
_SLOT = re.compile(r"(record|start)\.(0|[1-9][0-9]{0,5})\Z")
#: The largest stored request or response document, with room for the protocol's own bound.
_MAX_DOCUMENT = 16 * 1024
_MAX_LINE = 256
#: The largest stored anchor envelope: a request document plus the owner, handle and format fields.
_MAX_ENVELOPE = _MAX_DOCUMENT + 512
_ENVELOPE_FORMAT = "httk-daemon-anchor"
_ENVELOPE_VERSION = 1
_MAX_OBSERVATION = 64 * 1024
#: The absolute bound of every listing of the ledger, whatever the configured quotas.
_MAX_LISTING = 300_000
#: Requests can never exceed this many, whatever the configured quota.
_MAX_RECORDS = 100_000
#: Admission retries after losing an anchor race before the ledger is reported inconsistent.
_ADMISSION_ATTEMPTS = 8
#: Prepared anchors and link temporaries older than this are crash leftovers.
_LEFTOVER_NS = 3600 * 10**9
_PERMITTED = {
    "health": frozenset({"ready", "refused"}),
    "start_manager": frozenset({"submitted", "uncertain", "refused"}),
    "manager_status": frozenset({"status", "refused"}),
    "cancel_manager": frozenset({"cancel_requested", "refused"}),
}


class LedgerError(RuntimeError):
    """The daemon ledger is missing, corrupt, incompatible, or holds an inconsistent record."""


class ConflictError(RuntimeError):
    """A request identifier is already bound to different request content."""


class CapacityError(RuntimeError):
    """The enrollment's durable record or submission quota is exhausted."""


@dataclass(frozen=True, slots=True)
class Decision:
    """The single decision of a manager start: submit it, or refuse it.

    :param kind: ``"submit"`` or ``"refuse"``.
    :param nonce: The deciding instance's nonce: 32 lowercase hexadecimal digits.
    :param time_ns: For ``submit``, the decider's clock in integer UTC nanoseconds.
    :param reason: For ``refuse``, the protocol reason code.
    :raises ValueError: If the fields do not form a decision.
    """

    kind: str
    nonce: str
    time_ns: int | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        """Refuse fields that do not form a canonical decision."""

        if type(self.nonce) is not str or _HEX32.fullmatch(self.nonce) is None:
            raise ValueError("invalid decision nonce")
        if self.kind == "submit":
            if type(self.time_ns) is not int or not 0 <= self.time_ns < 1 << 63 or self.reason is not None:
                raise ValueError("a submit decision carries a time and no reason")
        elif self.kind == "refuse":
            if type(self.reason) is not str or _REASON.fullmatch(self.reason) is None or self.time_ns is not None:
                raise ValueError("a refuse decision carries a reason and no time")
        else:
            raise ValueError("invalid decision kind")

    def encode(self) -> bytes:
        """Return the canonical file content of this decision.

        :return: ``submit <time_ns> <nonce>`` or ``refuse <reason> <nonce>``, newline-terminated ASCII.
        """

        middle = str(self.time_ns) if self.kind == "submit" else str(self.reason)
        return f"{self.kind} {middle} {self.nonce}\n".encode("ascii")


@dataclass(frozen=True, slots=True)
class Entry:
    """One validated durable daemon request with everything recorded about it.

    :param request: Canonical admitted request.
    :param owner: Nonce of the instance that installed the anchor.
    :param handle: Opaque manager handle minted for a start request.
    :param decision: A start's decision, once one was recorded.
    :param job_id: Scheduler job identifier, once the submit winner recorded it.
    :param cluster: Scheduler cluster, once the submit winner recorded it.
    :param response: Durable unsigned response, once one was recorded.
    """

    request: Request
    owner: str
    handle: str | None
    decision: Decision | None
    job_id: str | None
    cluster: str | None
    response: Response | None

    @property
    def state(self) -> str:
        """The processing phase derived from which records exist.

        ``submitted`` whenever the scheduler identity is known (also after a premature ``uncertain``),
        otherwise the response outcome (``uncertain``, ``refused``, or ``done``), otherwise
        ``submitting`` or ``refused`` after a decision, otherwise ``received``.
        """

        if self.job_id is not None:
            return "submitted"
        if self.response is not None:
            return {"uncertain": "uncertain", "refused": "refused", "submitted": "submitted"}.get(
                self.response.outcome, "done"
            )
        if self.decision is None:
            return "received"
        return "submitting" if self.decision.kind == "submit" else "refused"


def refusal_response(request: Request, handle: str | None, reason: str) -> Response:
    """Return the deterministic unsigned refusal of a request.

    A refused start's response is a function of the request and the stored reason only, so any instance
    may link it after a crash of the instance that decided the refusal.

    :param request: The refused request.
    :param handle: The handle the response names: a start's minted handle, or the handle a manager
        operation names.
    :param reason: The protocol reason code.
    :return: The refusal.
    :raises ValueError: If the reason or handle is invalid.
    """

    return Response(
        request.request_id,
        request.workspace_id,
        request.enrollment_id,
        canonical_request_digest(request),
        "refused",
        handle=handle,
        reason=reason,
    )


def _legacy_message(path: Path) -> str:
    return (
        f"{path} belongs to the SQLite daemon ledger of an earlier httk-workflow; the daemon ledger is now a "
        "lock-free directory, and there is no migration. Reconcile the enrollment's outstanding manager jobs "
        "with squeue and scancel, then initialize a new enrollment with 'httk workspace daemon init' and a new "
        "--state (old per-user state can be removed with 'httk system reset')"
    )


def _read(path: Path, limit: int) -> bytes | None:
    """Read one regular file without following a symlink; ``None`` when it is absent."""

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise LedgerError(f"daemon ledger entry {path} is a symlink") from exc
        raise
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise LedgerError(f"daemon ledger entry {path} is not a regular file")
        data = bytearray()
        while chunk := os.read(descriptor, limit + 1 - len(data)):
            data.extend(chunk)
            if len(data) > limit:
                raise LedgerError(f"daemon ledger entry {path} exceeds {limit} bytes")
        return bytes(data)
    finally:
        os.close(descriptor)


def _line(data: bytes, path: Path) -> str:
    """Return the single canonical ASCII line of a small ledger file."""

    try:
        text = data.decode("ascii")
    except UnicodeDecodeError as exc:
        raise LedgerError(f"daemon ledger entry {path} is not ASCII") from exc
    if not text.endswith("\n") or "\n" in text[:-1] or "\r" in text:
        raise LedgerError(f"daemon ledger entry {path} is not one newline-terminated line")
    return text[:-1]


def _envelope_bytes(request_bytes: bytes, owner: str, handle: str | None) -> bytes:
    """Return the canonical envelope bytes of an anchor: the request, its owner and a start's handle."""

    fields = {
        "format": _ENVELOPE_FORMAT,
        "format_version": _ENVELOPE_VERSION,
        "handle": handle,
        "owner": owner,
        "request": json.loads(request_bytes),
    }
    return json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _write_new(path: Path, data: bytes) -> None:
    """Create one file exclusively, write it and fsync it (its directory is synchronized by the caller)."""

    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace(directory: Path, name: str, data: bytes) -> None:
    """Replace one file by an fsynced exclusive temporary and a rename, then fsync the directory."""

    temporary = directory / f".{name}.{uuid.uuid4().hex}"
    try:
        _write_new(temporary, data)
        os.rename(temporary, directory / name)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    fsync_directory(directory)


def _probe(path: Path) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None


def _link(directory: Path, name: str, data: bytes, *, nonce: bool = False) -> bool:
    """:func:`~httk.workflow._txn.link_new` a ledger file, retrying once when its temporary was swept away.

    The leftover sweep removes link temporaries older than an hour, so a link stalled that long finds its
    temporary gone (``ENOENT`` with nothing at *name*): the temporary is then written again, once.

    :param directory: The ledger directory.
    :param name: The new file's name.
    :param data: The exact content.
    :param nonce: Whether *data* carries this instance's nonce.
    :return: What :func:`~httk.workflow._txn.link_new` returns.
    :raises FileNotFoundError: If the directory is gone, or the retry fails the same way.
    """

    try:
        return link_new(directory, name, data, nonce=nonce, mode=0o600)
    except FileNotFoundError:
        if _probe(directory / name) is not None or _probe(directory) is None:
            raise
        _LOGGER.info("daemon_ledger_link_retried path=%s: its temporary was swept", directory / name)
        return link_new(directory, name, data, nonce=nonce, mode=0o600)


class Ledger:
    """Open the lock-free directory ledger of one daemon enrollment.

    Any number of instances may open the same ledger at once; each gets its own random :attr:`nonce`.

    :param directory: The protected daemon state directory holding ``ledger/``.
    :param workspace_id: Workspace identity pinned by protected policy.
    :param enrollment_id: Enrollment identity pinned by protected policy.
    :param initialize: Create the ledger; it must not exist yet.
    :param max_records: Maximum durable requests for this enrollment (cumulative).
    :param max_submissions: Maximum admitted manager starts for this enrollment (cumulative).
    :raises ValueError: If the path, identities or configured bounds are invalid.
    :raises FileExistsError: If *initialize* is set and a ledger already exists.
    :raises LedgerError: If the ledger is missing, malformed, belongs to another enrollment, or the state
        directory holds the SQLite ledger of an earlier version.
    :raises OSError: If the state directory cannot be opened.
    """

    def __init__(
        self,
        directory: Path,
        workspace_id: str,
        enrollment_id: str,
        *,
        initialize: bool = False,
        max_records: int = 4096,
        max_submissions: int = 128,
    ) -> None:
        if not isinstance(directory, Path) or not directory.is_absolute() or ".." in directory.parts:
            raise ValueError("ledger directory must be an absolute Path without '..'")
        if type(max_records) is not int or not 1 <= max_records <= _MAX_RECORDS:
            raise ValueError(f"max_records must be from 1 through {_MAX_RECORDS}")
        if type(max_submissions) is not int or not 1 <= max_submissions <= max_records:
            raise ValueError("max_submissions must be from 1 through max_records")
        if type(workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(workspace_id)) != workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        if type(enrollment_id) is not str or _HEX32.fullmatch(enrollment_id) is None:
            raise ValueError("invalid enrollment_id")

        self.workspace_id = workspace_id
        self.enrollment_id = enrollment_id
        self.max_records = max_records
        self.max_submissions = max_submissions
        #: This instance's nonce: what makes an anchor, a decision or a slot provably its own.
        self.nonce = secrets.token_hex(16)
        self._root = directory / _ROOT
        self._closed = False
        self._swept_at: int | None = None
        self._settled: dict[str, Entry] = {}
        self._open_state(directory)
        for name in _LEGACY:
            if _probe(directory / name) is not None:
                raise LedgerError(_legacy_message(directory / name))
        if initialize:
            self._initialize(directory)
        self._validate_layout()

    @staticmethod
    def _open_state(path: Path) -> None:
        """Require the state directory to be reachable without following any symlink."""

        descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            for component in path.parts[1:]:
                next_descriptor = os.open(
                    component, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=descriptor
                )
                os.close(descriptor)
                descriptor = next_descriptor
        finally:
            os.close(descriptor)

    def _format_bytes(self) -> bytes:
        return f"{_FORMAT} {_FORMAT_VERSION} {self.workspace_id} {self.enrollment_id}\n".encode("ascii")

    def _initialize(self, directory: Path) -> None:
        def build(staging: Path) -> None:
            os.chmod(staging, 0o700)
            for name in _SUBDIRECTORIES:
                os.mkdir(staging / name, 0o700)
            _write_new(staging / "format", self._format_bytes())

        if _probe(self._root) is not None or not birth(directory, _ROOT, build):
            raise FileExistsError(errno.EEXIST, "daemon ledger already exists", str(self._root))

    def _validate_layout(self) -> None:
        information = _probe(self._root)
        if information is None:
            raise LedgerError(
                f"daemon ledger {self._root} is missing; preserve this state and initialize a new enrollment"
            )
        if not stat.S_ISDIR(information.st_mode):
            raise LedgerError(f"daemon ledger {self._root} is not a directory")
        stored = _read(self._root / "format", _MAX_LINE)
        if stored is None:
            raise LedgerError(f"daemon ledger {self._root} has no format file")
        if stored != self._format_bytes():
            fields = stored.split(b" ")
            if len(fields) == 4 and fields[0] == _FORMAT.encode() and fields[1] == str(_FORMAT_VERSION).encode():
                raise LedgerError("daemon ledger identity mismatch: it belongs to another workspace or enrollment")
            raise LedgerError(
                "unsupported daemon ledger format; preserve this state, reconcile outstanding work, "
                "and initialize a new enrollment"
            )
        for name in _SUBDIRECTORIES:
            child = _probe(self._root / name)
            if child is None or not stat.S_ISDIR(child.st_mode):
                raise LedgerError(f"daemon ledger directory {self._root / name} is missing or not a directory")

    def __enter__(self) -> Self:
        """Return this open ledger."""

        self._check_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the ledger."""

        self.close()

    def close(self) -> None:
        """Close the ledger; it holds no lock and no descriptor, and repeated calls are harmless."""

        self._closed = True

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("ledger is closed")

    # ------------------------------------------------------------------
    # Reading and validation
    # ------------------------------------------------------------------

    def _anchor(self, request_id: str) -> Path:
        return self._root / "req" / request_id

    def _listing(self, directory: Path) -> Iterator[str]:
        """List one ledger directory within the absolute listing bound.

        Link temporaries and fenced prepared anchors (``.stale.*``) are skipped.

        :param directory: The ledger directory to list.
        :yields: Each remaining entry name, in directory order.
        :raises LedgerError: If the directory has more than the absolute listing bound of entries.
        """

        with os.scandir(directory) as entries:
            for count, entry in enumerate(entries, 1):
                if count > _MAX_LISTING:
                    raise LedgerError(f"daemon ledger directory {directory} exceeds {_MAX_LISTING} entries")
                if not is_link_temporary(entry.name) and not entry.name.startswith(_STALE):
                    yield entry.name

    def _response(self, data: bytes, path: Path) -> Response:
        try:
            response = decode_response(data)
        except ValueError as exc:
            raise LedgerError(f"invalid stored response {path}") from exc
        if encode_response(response) != data:
            raise LedgerError(f"noncanonical stored response {path}")
        return response

    def _check_response(self, entry: Entry, response: Response) -> str | None:
        """Return why *response* cannot be *entry*'s response, or ``None`` when it fits."""

        request = entry.request
        if response.operator_key is not None or response.signature is not None:
            return "ledger responses must be unsigned"
        if (
            response.request_id != request.request_id
            or response.workspace_id != request.workspace_id
            or response.enrollment_id != request.enrollment_id
            or response.request_digest != canonical_request_digest(request)
        ):
            return "response identity mismatch"
        expected_handle = entry.handle if request.operation == "start_manager" else request.handle
        if response.handle != expected_handle:
            return "response handle mismatch"
        if response.outcome not in _PERMITTED[request.operation]:
            return "response operation mismatch"
        if request.operation != "start_manager":
            return None
        decision = entry.decision
        if decision is None:
            return "a start's response requires a recorded decision"
        if decision.kind == "refuse":
            assert decision.reason is not None
            if response != refusal_response(request, entry.handle, decision.reason):
                return "a refused start's response must be its deterministic refusal"
        elif response.outcome == "refused":
            return "a start decided for submission cannot be refused"
        elif response.outcome == "submitted" and entry.job_id is None:
            return "a submitted response requires the recorded scheduler identity"
        return None

    @staticmethod
    def _envelope(anchor: Path) -> tuple[Request, str, str | None] | None:
        """Read and validate an anchor's envelope; ``None`` when it does not exist."""

        data = _read(anchor / "envelope", _MAX_ENVELOPE)
        if data is None:
            return None
        try:
            value = json.loads(data)
            if type(value) is not dict or set(value) != {"format", "format_version", "handle", "owner", "request"}:
                raise ValueError("unexpected envelope fields")
            if value["format"] != _ENVELOPE_FORMAT or type(value["format_version"]) is not int:
                raise ValueError("unsupported envelope")
            if value["format_version"] != _ENVELOPE_VERSION:
                raise ValueError("unsupported envelope version")
            owner, handle = value["owner"], value["handle"]
            if type(owner) is not str or _HEX32.fullmatch(owner) is None:
                raise ValueError("invalid owner")
            if handle is not None and (type(handle) is not str or _HEX32.fullmatch(handle) is None):
                raise ValueError("invalid handle")
            request_bytes = json.dumps(
                value["request"], sort_keys=True, separators=(",", ":"), ensure_ascii=True
            ).encode("ascii")
            request = decode_request(request_bytes)
            canonical = encode_request(request)
            if canonical != request_bytes or _envelope_bytes(canonical, owner, handle) != data:
                raise ValueError("noncanonical envelope")
        except (ValueError, RecursionError) as exc:  # JSONDecodeError and UnicodeDecodeError are ValueErrors
            raise LedgerError(f"invalid stored envelope {anchor}: {exc}") from exc
        return request, owner, handle

    def _entry(self, request_id: str) -> Entry | None:
        """Read and validate one anchor; ``None`` when it does not exist.

        The files are read in the reverse of their write order (response, scheduler, decision, then the
        anchor's envelope), so a file read later always existed when an earlier-read one was written.
        """

        anchor = self._anchor(request_id)
        information = _probe(anchor)
        if information is None:
            return None
        if not stat.S_ISDIR(information.st_mode):
            raise LedgerError(f"daemon ledger anchor {anchor} is not a directory")
        response_bytes = _read(anchor / "response", _MAX_DOCUMENT)
        scheduler_bytes = _read(anchor / "scheduler", _MAX_LINE)
        decision_bytes = _read(anchor / "decision", _MAX_LINE)
        envelope = self._envelope(anchor)
        if envelope is None:
            raise LedgerError(f"daemon ledger anchor {anchor} is incomplete")
        request, owner, handle = envelope
        if request.request_id != request_id:
            raise LedgerError(f"stored request {anchor} names another request identifier")
        if request.workspace_id != self.workspace_id or request.enrollment_id != self.enrollment_id:
            raise LedgerError(f"invalid stored enrollment identity in {anchor}")
        start = request.operation == "start_manager"
        if start != (handle is not None):
            raise LedgerError(f"invalid stored manager handle in {anchor}")

        decision: Decision | None = None
        if decision_bytes is not None:
            if not start:
                raise LedgerError(f"unexpected stored decision in {anchor}")
            decision = self._decision(_line(decision_bytes, anchor / "decision"), anchor)

        job_id: str | None = None
        cluster: str | None = None
        if scheduler_bytes is not None:
            if decision is None or decision.kind != "submit":
                raise LedgerError(f"unexpected stored scheduler identity in {anchor}")
            fields = _line(scheduler_bytes, anchor / "scheduler").split(" ")
            if len(fields) != 2 or _JOB_ID.fullmatch(fields[0]) is None or _CLUSTER.fullmatch(fields[1]) is None:
                raise LedgerError(f"invalid stored scheduler identity in {anchor}")
            job_id, cluster = fields

        entry = Entry(request, owner, handle, decision, job_id, cluster, None)
        if response_bytes is None:
            return entry
        response = self._response(response_bytes, anchor / "response")
        problem = self._check_response(entry, response)
        if problem is not None:
            raise LedgerError(f"invalid stored response in {anchor}: {problem}")
        return Entry(request, owner, handle, decision, job_id, cluster, response)

    @staticmethod
    def _decision(text: str, anchor: Path) -> Decision:
        fields = text.split(" ")
        try:
            if len(fields) != 3:
                raise ValueError("a decision has three fields")
            kind, middle, nonce = fields
            if kind == "submit":
                if _TIME_NS.fullmatch(middle) is None:
                    raise ValueError("noncanonical decision time")
                return Decision(kind, nonce, time_ns=int(middle))
            return Decision(kind, nonce, reason=middle)
        except ValueError as exc:
            raise LedgerError(f"invalid stored decision in {anchor}: {exc}") from exc

    def lookup_request(self, request_id: str) -> Entry | None:
        """Return one durable request without admitting or changing anything.

        :param request_id: Canonical 32-digit lowercase request identifier.
        :return: The validated entry, or ``None``.
        :raises ValueError: If the identifier is malformed.
        :raises LedgerError: If the stored entry is corrupt or inconsistent.
        """

        self._check_open()
        if type(request_id) is not str or _HEX32.fullmatch(request_id) is None:
            raise ValueError("invalid request identifier")
        return self._entry(request_id)

    def _handle_target(self, handle: str) -> str | None:
        data = _read(self._root / "handles" / handle, _MAX_LINE)
        if data is None:
            return None
        request_id = _line(data, self._root / "handles" / handle)
        if _HEX32.fullmatch(request_id) is None:
            raise LedgerError(f"invalid stored handle entry {handle}")
        return request_id

    def lookup_manager(self, handle: str) -> Entry | None:
        """Return the manager start that minted *handle*, cross-checked against its anchor.

        :param handle: The manager handle: 32 lowercase hexadecimal digits.
        :return: The start's entry, or ``None`` when no admitted start owns the handle.
        :raises ValueError: If the handle is malformed.
        :raises LedgerError: If a stored entry is corrupt.
        """

        self._check_open()
        if type(handle) is not str or _HEX32.fullmatch(handle) is None:
            raise ValueError("invalid manager handle")
        request_id = self._handle_target(handle)
        if request_id is None:
            return None
        entry = self._entry(request_id)
        if entry is None or entry.handle != handle:
            return None  # an orphan handle entry: ignored
        return entry

    def _starts(self, *, unanswered: bool = False) -> Iterator[Entry]:
        """Yield every manager start reachable from ``handles/``, cross-checked; orphans are skipped.

        A settled start (a response that nothing can change any more: ``refused``, ``submitted``, or
        ``uncertain`` with the scheduler identity recorded) is read and validated once and then served
        from memory, since every ledger file is written once and never replaced.

        :param unanswered: Skip every start whose ``response`` exists, found by a name lookup.
        :yields: Each start's validated entry, in handle order.
        """

        for handle in sorted(self._listing(self._root / "handles")):
            if _HEX32.fullmatch(handle) is None:
                raise LedgerError(f"unexpected entry {handle!r} in the daemon ledger's handles")
            request_id = self._handle_target(handle)
            if request_id is None:
                continue
            settled = self._settled.get(request_id)
            if settled is not None:
                if settled.handle == handle and not unanswered:
                    yield settled
                continue
            if unanswered and _probe(self._anchor(request_id) / "response") is not None:
                continue
            entry = self._entry(request_id)
            if entry is None or entry.handle != handle:
                _LOGGER.debug("daemon ledger handle %s is an orphan; ignored", handle)
                continue
            if entry.response is not None and (entry.job_id is not None or entry.response.outcome != "uncertain"):
                self._settled[request_id] = entry
            yield entry

    def managers(self) -> list[dict[str, str | None]]:
        """Return every manager start as ``handle``, ``profile``, ``request_id``, ``state`` and ``job_id``.

        The rows come from a listing of ``handles/``, each cross-checked against its anchor's ``handle``.

        :return: Rows ordered by request identifier.
        :raises LedgerError: If a stored entry is corrupt.
        """

        self._check_open()
        rows: list[dict[str, str | None]] = []
        for entry in self._starts():
            assert entry.handle is not None and entry.request.profile is not None
            rows.append(
                {
                    "handle": entry.handle,
                    "profile": entry.request.profile,
                    "request_id": entry.request.request_id,
                    "state": entry.state,
                    "job_id": entry.job_id,
                }
            )
        return sorted(rows, key=lambda row: str(row["request_id"]))

    def verify(self) -> int:
        """Validate every anchor and the shape of every ledger directory.

        :return: The number of admitted requests.
        :raises LedgerError: If anything stored is corrupt, unexpected, or beyond the absolute bound.
        """

        self._check_open()
        count = 0
        for request_id in self._listing(self._root / "req"):
            count += 1
            if count > _MAX_RECORDS or _HEX32.fullmatch(request_id) is None:
                raise LedgerError(f"unexpected entry {request_id!r} in the daemon ledger's requests")
            unknown = set(self._listing(self._anchor(request_id))) - _ANCHOR_FILES
            if unknown:
                raise LedgerError(f"unexpected files {sorted(unknown)} in daemon ledger anchor {request_id}")
            self._entry(request_id)
        for _start in self._starts():
            pass
        for name in self._listing(self._root / "slots"):
            if _SLOT.fullmatch(name) is None:
                raise LedgerError(f"unexpected entry {name!r} in the daemon ledger's slots")
        for name in self._listing(self._root / "prep"):
            if _PREPARED.fullmatch(name) is None:
                raise LedgerError(f"unexpected entry {name!r} in the daemon ledger's prepared anchors")
        return count

    # ------------------------------------------------------------------
    # Admission
    # ------------------------------------------------------------------

    def _claim(self, kind: str, limit: int, data: bytes) -> str:
        """Claim one quota slot by exclusive creation, scanning from a listing-count hint and wrapping."""

        slots = self._root / "slots"
        prefix = f"{kind}."
        hint = min(sum(1 for name in self._listing(slots) if name.startswith(prefix)), limit)
        for k in itertools.chain(range(hint, limit), range(hint)):
            name = f"{prefix}{k}"
            if _probe(slots / name) is not None:
                continue  # taken: a name lookup, never a negative drawn from the listing
            _hook("ledger.slot")
            if _link(slots, name, data, nonce=True):
                return name
        raise CapacityError(f"daemon {'record' if kind == 'record' else 'submission'} capacity exhausted")

    def _release(self, names: list[str], data: bytes) -> None:
        """Remove the named slots, but only those whose content is exactly this instance's claim."""

        slots = self._root / "slots"
        for name in names:
            if _read(slots / name, _MAX_LINE) == data:
                try:
                    os.unlink(slots / name)
                except FileNotFoundError:
                    pass
        if names:
            fsync_directory(slots)

    def _prepare(self, request: Request, handle: str | None) -> Path | None:
        """Build a prepared anchor; ``None`` when the leftover sweep fenced it away meanwhile."""

        for _attempt in range(_ADMISSION_ATTEMPTS):
            prepared = self._root / "prep" / f"{self.nonce}.{secrets.token_hex(8)}"
            try:
                os.mkdir(prepared, 0o700)
            except FileExistsError:
                continue  # a retransmitted mkdir, or a collision: take another name, never share one
            break
        else:
            raise LedgerError("no fresh name for a prepared anchor")
        try:
            _write_new(prepared / "envelope", _envelope_bytes(encode_request(request), self.nonce, handle))
            fsync_directory(prepared)
        except FileNotFoundError:
            if _probe(prepared) is not None or _probe(prepared.parent) is None:
                remove_tree(prepared)
                raise
            _LOGGER.info("daemon_ledger_prepared_fenced path=prep/%s: preparing again", prepared.name)
            return None
        except BaseException:
            remove_tree(prepared)
            raise
        _hook("ledger.prepared")
        return prepared

    def _install(self, prepared: Path, anchor: Path) -> bool:
        """Rename the prepared anchor into place.

        Losing (a non-empty destination, or the leftover sweep fenced the prepared anchor away) is not an
        error.

        :param prepared: The prepared anchor in ``prep/``.
        :param anchor: The destination ``req/<id>``.
        :return: Whether this call's rename installed the anchor.
        """

        try:
            moved = rename_verified(prepared, anchor)
        except OSError as exc:
            if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST}:
                remove_tree(prepared)
                raise
            moved = False
        if not moved:
            # Only this instance's own name: after a fencing rename by the sweep it is absent here.
            remove_tree(prepared)
        return moved

    def _link_handle(self, handle: str, request_id: str) -> None:
        handles = self._root / "handles"
        data = f"{request_id}\n".encode("ascii")
        if _read(handles / handle, _MAX_LINE) == data:
            return
        if not _link(handles, handle, data) and _read(handles / handle, _MAX_LINE) != data:
            raise LedgerError(f"manager handle {handle} is already bound to another request")
        _hook("ledger.handle")

    def admit(self, request: Request) -> Entry:
        """Durably admit a canonical request, or return its existing entry.

        An existing anchor is compared by canonical bytes. Otherwise a record slot (and, for a start, a
        start slot) is claimed, an anchor is prepared in ``prep/`` and renamed to ``req/<id>``. The anchor
        belongs to whichever instance's nonce its ``owner`` names; a losing instance removes only its own
        slots and reads the winner's anchor. A start's ``handles/<handle>`` entry is linked last, and
        relinked when an admission crashed before it.

        :param request: Validated request for this ledger identity.
        :return: The new or existing durable entry.
        :raises ValueError: If the request is of the wrong type, workspace or enrollment.
        :raises ConflictError: If the request identifier names different content.
        :raises CapacityError: If an applicable cumulative quota is exhausted.
        :raises LedgerError: If the stored state is corrupt or admission never settles.
        """

        self._check_open()
        if type(request) is not Request:
            raise ValueError("request must be a Request")
        if request.workspace_id != self.workspace_id:
            raise ValueError("wrong workspace")
        if request.enrollment_id != self.enrollment_id:
            raise ValueError("wrong enrollment")
        canonical = encode_request(request)
        start = request.operation == "start_manager"
        anchor = self._anchor(request.request_id)
        for _attempt in range(_ADMISSION_ATTEMPTS):
            existing = self._entry(request.request_id)
            if existing is not None:
                if encode_request(existing.request) != canonical:
                    raise ConflictError("request identifier conflict")
                if existing.handle is not None:
                    self._link_handle(existing.handle, request.request_id)
                if existing.response is None:
                    # Another instance may have renamed the anchor in just now: make its directory entry
                    # durable before this instance executes or decides anything for it.
                    fsync_directory(anchor.parent)
                return existing

            claim = f"{request.request_id} {self.nonce}\n".encode("ascii")
            handle = secrets.token_hex(16) if start else None
            claimed = [self._claim("record", self.max_records, claim)]
            try:
                if start:
                    claimed.append(self._claim("start", self.max_submissions, claim))
                prepared = self._prepare(request, handle)
                moved = prepared is not None and self._install(prepared, anchor)
            except BaseException:
                self._release(claimed, claim)  # nothing was installed: the claim is this admission's alone
                raise
            # From here the anchor may be installed: an error must not free its slots.
            if moved:
                fsync_directory(anchor.parent)
            stored = self._envelope(anchor)
            owner = None if stored is None else stored[1]
            if moved and owner != self.nonce:
                raise LedgerError(f"daemon ledger anchor {anchor} installed by this instance names another owner")
            if owner == self.nonce:
                _hook("ledger.anchored")
                if handle is not None:
                    self._link_handle(handle, request.request_id)
                entry = self._entry(request.request_id)
                if entry is None:
                    raise LedgerError(f"daemon ledger anchor {anchor} vanished after admission")
                return entry
            self._release(claimed, claim)
        raise LedgerError(f"admission of request {request.request_id} did not settle")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _start_entry(self, request_id: str) -> Entry:
        entry = self.lookup_request(request_id)
        if entry is None:
            raise ValueError("unknown request")
        if entry.request.operation != "start_manager":
            raise ValueError("request is not a manager start")
        return entry

    def decide(self, request_id: str, *, reason: str | None = None) -> tuple[Entry, bool]:
        """Record a start's single decision: submit it, or refuse it with *reason*.

        The decision carries this instance's nonce (and, for ``submit``, this clock's time), and is linked
        no-replace. The winner is the instance whose nonce the stored decision names, whatever error the
        link reported.

        :param request_id: The admitted start's request identifier.
        :param reason: A refusal reason code, or ``None`` to decide submission.
        :return: The entry with the stored decision, and whether this call's decision won. A start that
            already had a decision is returned unchanged and never won.
        :raises ValueError: If the request is not an admitted start or the reason is invalid.
        :raises LedgerError: If the stored state is corrupt.
        """

        entry = self._start_entry(request_id)
        if entry.decision is not None:
            return entry, False
        decision = (
            Decision("submit", self.nonce, time_ns=time.time_ns())
            if reason is None
            else Decision("refuse", self.nonce, reason=reason)
        )
        _link(self._anchor(request_id), "decision", decision.encode(), nonce=True)
        _hook("ledger.decided")
        stored = self._start_entry(request_id)
        assert stored.decision is not None
        return stored, stored.decision.nonce == self.nonce

    def record_scheduler(self, request_id: str, job_id: str, cluster: str) -> Entry:
        """Record the scheduler identity of a start this instance decided to submit.

        It is linked even when a response (recovery's premature ``uncertain``) already exists, and never
        changes that response.

        :param request_id: The start's request identifier.
        :param job_id: The numeric scheduler job identifier.
        :param cluster: The scheduler cluster.
        :return: The entry with the stored scheduler identity.
        :raises ValueError: If the identity is malformed or this instance did not win a submit decision.
        :raises LedgerError: If a different scheduler identity is already stored.
        """

        if type(job_id) is not str or _JOB_ID.fullmatch(job_id) is None:
            raise ValueError("invalid scheduler job identifier")
        if type(cluster) is not str or _CLUSTER.fullmatch(cluster) is None:
            raise ValueError("invalid scheduler cluster")
        entry = self._start_entry(request_id)
        if entry.decision is None or entry.decision.kind != "submit" or entry.decision.nonce != self.nonce:
            raise ValueError("only the submit winner records the scheduler identity")
        data = f"{job_id} {cluster}\n".encode("ascii")
        anchor = self._anchor(request_id)
        if not _link(anchor, "scheduler", data) and _read(anchor / "scheduler", _MAX_LINE) != data:
            raise LedgerError(f"a different scheduler identity is already stored for request {request_id}")
        _hook("ledger.scheduler")
        return self._start_entry(request_id)

    def finish(self, request_id: str, response: Response) -> Entry:
        """Link a request's unsigned response, or return the response another instance stored first.

        :param request_id: The admitted request's identifier.
        :param response: The validated unsigned response.
        :return: The entry with its stored response, which is *response* only when this call linked it.
        :raises ValueError: If the response is signed, does not belong to the request, or contradicts the
            recorded decision.
        :raises LedgerError: If the stored state is corrupt.
        """

        if type(response) is not Response:
            raise ValueError("response must be a Response")
        entry = self.lookup_request(request_id)
        if entry is None:
            raise ValueError("unknown request")
        if entry.response is not None:
            return entry
        problem = self._check_response(entry, response)
        if problem is not None:
            raise ValueError(problem)
        _link(self._anchor(request_id), "response", encode_response(response))
        _hook("ledger.response")
        stored = self.lookup_request(request_id)
        if stored is None or stored.response is None:
            raise LedgerError(f"the response of request {request_id} was not recorded")
        return stored

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def _sweep_leftovers(self, now_ns: int) -> None:
        """Remove crash leftovers: prepared anchors and slot or handle link temporaries older than an hour."""

        if self._swept_at is not None and 0 <= now_ns - self._swept_at < _LEFTOVER_NS:
            return
        self._swept_at = now_ns
        prep = self._root / "prep"
        with os.scandir(prep) as entries:
            names = [entry.name for _count, entry in zip(range(_MAX_LISTING), entries, strict=False)]
        for name in names:
            if name.startswith(_STALE):
                remove_tree(prep / name)  # fenced already (by this or a crashed sweep): nobody else uses it
                continue
            information = _probe(prep / name)
            if information is None or now_ns - information.st_mtime_ns <= _LEFTOVER_NS:
                continue
            # Fence first: the rename takes the directory from a slow (not dead) owner, whose own rename into
            # req/ then finds no source and loses cleanly. Removing in place could empty its anchor instead.
            fenced = prep / f"{_STALE}{secrets.token_hex(8)}"
            if rename_verified(prep / name, fenced):
                _LOGGER.info("daemon_ledger_leftover_removed path=prep/%s", name)
                remove_tree(fenced)
        for directory in (self._root / "slots", self._root / "handles"):
            with os.scandir(directory) as entries:
                for count, entry in enumerate(entries, 1):
                    if count > _MAX_LISTING:
                        break
                    if not is_link_temporary(entry.name):
                        continue
                    information = _probe(Path(entry.path))
                    if information is not None and now_ns - information.st_mtime_ns > _LEFTOVER_NS:
                        try:
                            os.unlink(entry.path)
                        except FileNotFoundError:
                            pass

    def recover(self, *, older_than: float, now_ns: int | None = None) -> list[Entry]:
        """Settle every start whose decider crashed before linking its response.

        A ``refuse`` decision gets its deterministic refusal. A ``submit`` decision older than
        *older_than* seconds (by the decider's clock) gets ``uncertain``/``submission_unconfirmed``: the
        scheduler may or may not have the job. That is safe even when premature, because a late winner
        still links ``scheduler``, which status and cancellation then use. Crash leftovers are swept at
        most once an hour.

        :param older_than: Age in seconds after which a submit decision without a response is unconfirmed.
        :param now_ns: The current time in integer UTC nanoseconds; the clock by default.
        :return: The entries this call settled.
        :raises LedgerError: If the stored state is corrupt.
        """

        self._check_open()
        now = time.time_ns() if now_ns is None else now_ns
        self._sweep_leftovers(now)
        settled: list[Entry] = []
        for entry in list(self._starts(unanswered=True)):
            decision = entry.decision
            if entry.response is not None or decision is None:
                continue
            assert entry.handle is not None
            if decision.kind == "refuse":
                assert decision.reason is not None
                response = refusal_response(entry.request, entry.handle, decision.reason)
            elif decision.time_ns is not None and now - decision.time_ns > older_than * 1e9:
                response = Response(
                    entry.request.request_id,
                    entry.request.workspace_id,
                    entry.request.enrollment_id,
                    canonical_request_digest(entry.request),
                    "uncertain",
                    handle=entry.handle,
                    reason="submission_unconfirmed",
                )
            else:
                continue
            settled.append(self.finish(entry.request.request_id, response))
            _LOGGER.warning(
                "daemon_ledger_recovered request_id=%s handle=%s decision=%s outcome=%s",
                entry.request.request_id,
                entry.handle,
                decision.kind,
                response.outcome,
            )
        return settled

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------

    def observation(self, handle: str) -> bytes | None:
        """Return the stored scheduler observation of one manager, unvalidated.

        :param handle: The manager handle.
        :return: The stored bytes, or ``None`` when there is no readable regular observation file.
        :raises ValueError: If the handle is malformed.
        """

        self._check_open()
        if type(handle) is not str or _HEX32.fullmatch(handle) is None:
            raise ValueError("invalid manager handle")
        try:
            return _read(self._root / "observations" / f"{handle}.json", _MAX_OBSERVATION)
        except (LedgerError, OSError):
            return None

    def record_observation(self, handle: str, data: bytes) -> None:
        """Replace one manager's scheduler observation; the last writer wins.

        :param handle: The manager handle.
        :param data: The observation document.
        :raises ValueError: If the handle is malformed or the document too large.
        :raises OSError: If the file cannot be replaced.
        """

        self._check_open()
        if type(handle) is not str or _HEX32.fullmatch(handle) is None:
            raise ValueError("invalid manager handle")
        if type(data) is not bytes or len(data) > _MAX_OBSERVATION:
            raise ValueError("invalid observation document")
        _replace(self._root / "observations", f"{handle}.json", data)


__all__ = ["CapacityError", "ConflictError", "Decision", "Entry", "Ledger", "LedgerError", "refusal_response"]
