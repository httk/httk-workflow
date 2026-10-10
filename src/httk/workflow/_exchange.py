"""The exchange extension: ``WORKSPACE/exchange``, a client-written inbox and a returning outbox.

A workspace with the ``exchange`` extension (``format.json`` ``extensions``)
has one directory a client may write, ``WORKSPACE/exchange``:

.. code-block:: text

    exchange/
    ├── exchange.json                 written at enable: format, format_version, workspace_id
    ├── status.json                   managers: the exchange jobs, their states and bounded progress
    ├── inbox/<name>/                 client -> workspace: fresh job bundles
    ├── outbox/<exchange-name>/       workspace -> client: the returned tree of one exchange job
    ├── outbox/rejected/<unique>/     one refused bundle: <name>/ and reason.json
    ├── requests/<id>.json            client: signed daemon actions and job actions
    ├── responses/<id>.json           daemon-signed for daemon actions, unsigned for job actions
    └── daemon.json, managers.json, managers/   the workspace daemon's documents

:func:`enable_exchange` creates it, and the workspace daemon installs
``daemon.json``, ``managers.json`` and the manager logs there
(:func:`install_document`, :func:`publish_file`).

Every unrestricted manager serving the exchange runs :class:`ExchangeService` in each tick:

- **Inbox.** It adopts one inbox entry per pass as an untrusted bundle
  (:func:`httk.workflow._moving.adopt`, which takes it by a descriptor-anchored contested move): fresh job
  UUIDs, the client's UUID kept as ``exchange_name``, ``origin: exchange``, and the root indexed in the trusted
  ``.httk-workspace/exchange-jobs/``. A refused bundle goes to ``outbox/rejected/<unique>/`` with its
  ``reason.json``.
- **Job control.** A ``stop_job``, ``cancel_job`` or ``eject_job`` request signed by a key of the workspace
  setting ``exchange.authorized_keys`` and inside its time window is translated into the workspace request
  ``requests/<job-uuid>.<request-uuid>.json`` (``pause``, ``cancel``, or ``eject`` of the tree to the outbox),
  answered with an unsigned ``responses/<id>.json`` and deleted. Every step is idempotent, so several managers
  may handle one request; the owner applies a translated request at most once (the index's ``translated/``
  set, see :func:`httk.workflow.removal.apply_requests`). Daemon actions are left to the daemon.
- **Returns.** A finished exchange tree (the indexed root and every member terminal) gets an ``eject`` request
  of the whole tree to ``outbox/<exchange-name>/``; its owner applies it. An outbox name the client has not
  emptied yet makes the eject roll back, and a later pass retries.
- **Status.** At most every :data:`STEP_INTERVAL` (and not when another manager wrote it just before),
  ``status.json`` lists every indexed exchange job with its state and a sanitized excerpt of the
  ``progress.json`` a job may keep in its payload, read live without following a symlink or blocking.

The writer of ``exchange/`` is whoever can write it, which the workspace cannot
verify, so the trusted side treats it as hostile territory: every access is
anchored at directory descriptors opened ``O_NOFOLLOW``, reads are bounded and
nonblocking, and a file goes in only by an exclusive temporary and a rename.
The client's open descriptors are not revoked: a client can still change the
bytes of a bundle it handed in after the adoption, which affects only its own
confined job.
"""

import errno
import hashlib
import json
import logging
import math
import os
import re
import stat
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from . import _fs, _kernel, _moving, _requests
from ._bundles import BundleError
from ._daemon_auth import check_request_time, verify_request
from ._daemon_mailbox import MAX_DIRECTORY_ENTRIES, MAX_DOCUMENT_BYTES
from ._daemon_protocol import (
    JOB_OPERATIONS,
    Request,
    Response,
    decode_request,
    encode_response,
    request_digest,
)
from ._kernel import OWNED, TERMINAL_STATES, JobRef
from ._state import encode_state, read_state_unowned
from ._util import json_bytes, read_json, utc_now, write_json_atomic
from .errors import FormatError, WorkflowError
from .introspection._reading import read_job_file
from .models import EXCHANGE_DIRECTORY, EXCHANGE_EXTENSION
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "AUTHORIZED_KEYS_SETTING",
    "EXCHANGE_DOCUMENT",
    "EXCHANGE_FORMAT",
    "EXCHANGE_SUBDIRECTORIES",
    "PROGRESS_DOCUMENT",
    "STATUS_DOCUMENT",
    "STATUS_FORMAT",
    "STATUS_LIMIT",
    "STEP_INTERVAL",
    "ExchangeService",
    "ExchangeUnavailableError",
    "enable_exchange",
    "exchange_directory",
    "install_document",
    "publish_file",
]

#: The identity document :func:`enable_exchange` writes at the exchange root.
EXCHANGE_DOCUMENT = "exchange.json"
EXCHANGE_FORMAT = "httk-workspace-exchange"
#: The directories of an enabled exchange, parents before children.
EXCHANGE_SUBDIRECTORIES = ("inbox", "outbox", "outbox/rejected", "requests", "responses", "managers")

#: The status document managers install at the exchange root.
STATUS_DOCUMENT = "status.json"
STATUS_FORMAT = "httk-workspace-exchange-status"
#: The bound of ``status.json``; the job list is cut to fit and ``truncated`` set.
STATUS_LIMIT = 1024 * 1024
#: The least time between two scans for finished exchange trees (unless the manager's own work changed something),
#: two ``status.json`` installs, and two return requests for one job.
STEP_INTERVAL = 10.0
#: The workspace setting holding the public keys whose signed job actions the managers accept.
AUTHORIZED_KEYS_SETTING = "exchange.authorized_keys"
#: The progress file a job may keep in its payload; its excerpt goes into ``status.json``.
PROGRESS_DOCUMENT = "progress.json"

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_PUBLICATION = re.compile(r"[0-9a-f]{32}\.json")
#: A job bundle name in the exchange; the reserved names are the exchange's own entries.
_BUNDLE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_RESERVED_NAMES = frozenset(
    {
        "daemon.json",
        "exchange.json",
        "status.json",
        "managers.json",
        "managers",
        "rejected",
        "requests",
        "responses",
        "inbox",
        "outbox",
    }
)
_PROGRESS_LIMIT = 4096
_PROGRESS_KEYS = 32
_PROGRESS_TEXT = 256
_ACTIONS = {"stop_job": "pause", "cancel_job": "cancel", "eject_job": "eject"}
#: The namespace of return request ids: uuid5(namespace, "<job-id>/<state digest>").
_RETURN_NAMESPACE = uuid.UUID("5d0c8f2e-8f5e-4a43-9a51-7f2b0c3e9b61")


def exchange_directory(workspace: Workspace) -> Path:
    """Return the workspace's exchange directory.

    :param workspace: The workspace.
    :return: ``WORKSPACE/exchange``.
    """

    return workspace.root / EXCHANGE_DIRECTORY


def _lexists(name: str | Path, dir_fd: int | None = None) -> bool:
    try:
        os.lstat(name, dir_fd=dir_fd)
    except (FileNotFoundError, NotADirectoryError):
        return False
    return True


class ExchangeUnavailableError(WorkflowError):
    """An exchange directory is not a real directory (a symlink, a file, missing), so it is not served."""


def _open_directory(name: str, dir_fd: int, *, label: str) -> int:
    """Open one exchange directory relative to its parent's descriptor, never following a symlink.

    :param name: The directory's name in its parent.
    :param dir_fd: The parent directory's descriptor.
    :param label: The directory as reported (``exchange/inbox``).
    :return: The directory's descriptor.
    :raises ExchangeUnavailableError: If it is absent, a symlink or not a directory.
    """

    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=dir_fd)
    except FileNotFoundError:
        raise ExchangeUnavailableError(f"{label} is missing") from None
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR, errno.EMLINK}:
            raise ExchangeUnavailableError(f"{label} is not a real directory (a symlink or another file)") from None
        raise


def _open_relative(exchange_fd: int, relative: str) -> int:
    """Open ``exchange/<relative>`` component by component, never following a symlink.

    :param exchange_fd: The exchange directory's descriptor.
    :param relative: A ``/``-separated path below it (``outbox/rejected``).
    :return: The directory's descriptor.
    :raises ExchangeUnavailableError: If a component is absent, a symlink or not a directory.
    """

    descriptor = exchange_fd
    walked = EXCHANGE_DIRECTORY
    try:
        for part in relative.split("/"):
            walked = f"{walked}/{part}"
            inner = _open_directory(part, descriptor, label=walked)
            if descriptor != exchange_fd:
                os.close(descriptor)
            descriptor = inner
    except BaseException:
        if descriptor != exchange_fd:
            os.close(descriptor)
        raise
    return descriptor


# ---------------------------------------------------------------------------
# Enabling the extension
# ---------------------------------------------------------------------------


def _exchange_document(workspace: Workspace) -> bytes:
    return json_bytes({"format": EXCHANGE_FORMAT, "format_version": 1, "workspace_id": workspace.workspace_id}) + b"\n"


def _complete_layout(workspace: Workspace, exchange_fd: int) -> None:
    """Create every missing exchange subdirectory and ``exchange.json``, by descriptor, never following a link."""

    for relative in EXCHANGE_SUBDIRECTORIES:
        parent_name, _, name = relative.rpartition("/")
        parent = exchange_fd if not parent_name else _open_relative(exchange_fd, parent_name)
        try:
            try:
                os.mkdir(name, 0o755, dir_fd=parent)
            except FileExistsError:
                pass
            os.close(_open_directory(name, parent, label=f"{EXCHANGE_DIRECTORY}/{relative}"))
        finally:
            if parent != exchange_fd:
                os.close(parent)
    if not _lexists(EXCHANGE_DOCUMENT, exchange_fd):
        # Every writer writes the same document.
        _fs.write_file(
            _fs.anchored(exchange_fd, EXCHANGE_DOCUMENT), _exchange_document(workspace), durable=workspace.durable
        )


def _layout_complete(workspace: Workspace) -> bool:
    """Report whether the exchange directory and all its entries exist as real directories (read only)."""

    try:
        root = os.open(workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError:
        return False
    try:
        exchange = _open_directory(EXCHANGE_DIRECTORY, root, label=EXCHANGE_DIRECTORY)
    except (ExchangeUnavailableError, OSError):
        return False
    finally:
        os.close(root)
    try:
        for relative in EXCHANGE_SUBDIRECTORIES:
            try:
                os.close(_open_relative(exchange, relative))
            except (ExchangeUnavailableError, OSError):
                return False
        return _lexists(EXCHANGE_DOCUMENT, exchange)
    finally:
        os.close(exchange)


def _rename_probe(workspace: Workspace) -> None:
    """Rename a probe directory from ``tmp/`` into ``exchange/``: every exchange move is a rename.

    Bind mounts defeat a device comparison, so the rename itself is the test.

    :param workspace: The workspace.
    :raises ExchangeUnavailableError: If the rename crosses filesystems or misbehaves.
    """

    try:
        _fs.rename_probe(workspace.control / "tmp", exchange_directory(workspace))
    except _fs.FilesystemUnsupported as exc:
        raise ExchangeUnavailableError(
            f"{exchange_directory(workspace)} and {workspace.control / 'tmp'} cannot exchange bundles by rename "
            f"({exc}); the exchange must live on the workspace's own filesystem and mount"
        ) from None


def enable_exchange(workspace: Workspace) -> bool:
    """Enable the exchange extension: create ``WORKSPACE/exchange`` and record the extension.

    The directory and every subdirectory are created by descriptor, never
    following a symlink, with ``exchange.json``; a rename probe between
    ``exchange/`` and ``.httk-workspace/tmp/`` then proves that bundles can move
    between them, and only then is ``"exchange"`` added to ``format.json``
    ``extensions`` (an ordinary read-modify-write) and verified by re-reading
    it. From then on every manager of the workspace must confine its attempts.
    Enabling an enabled workspace changes nothing; a half-made exchange left by
    an interrupted call is completed. There is no ``disable``.

    :param workspace: The workspace.
    :return: ``True`` when this call enabled (or completed) the extension,
        ``False`` when it was already enabled.
    :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
    :raises ExchangeUnavailableError: If ``WORKSPACE/exchange`` or one of its
        directories exists and is not a real directory, or the rename probe crosses filesystems.
    :raises httk.workflow.errors.WorkflowError: If ``format.json`` does not keep the extension.
    """

    from .seals import require_cli_modifiable

    require_cli_modifiable(workspace)
    workspace.refresh_format()
    if EXCHANGE_EXTENSION in workspace.extensions and _layout_complete(workspace):
        return False
    directory = exchange_directory(workspace)
    try:
        os.mkdir(directory, 0o755)
    except FileExistsError:
        pass
    root = os.open(workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        exchange = _open_directory(EXCHANGE_DIRECTORY, root, label=str(directory))
    finally:
        os.close(root)
    try:
        _complete_layout(workspace, exchange)
    finally:
        os.close(exchange)
    _rename_probe(workspace)
    if EXCHANGE_EXTENSION not in workspace.extensions:
        path = workspace.control / "format.json"
        stored = read_json(path)
        extensions = stored.get("extensions", [])
        if not isinstance(extensions, list):
            raise FormatError("workspace extensions must be an array of strings")
        stored["extensions"] = sorted({*extensions, EXCHANGE_EXTENSION})
        write_json_atomic(path, stored, durable=workspace.durable)
        workspace.refresh_format()
        if EXCHANGE_EXTENSION not in workspace.extensions:
            raise WorkflowError(
                f"{path} does not record the exchange extension after it was written (a concurrent writer "
                "replaced it); run the command again"
            )
    _LOGGER.info(
        "enabled the exchange extension of workspace %s at %s",
        workspace.workspace_id,
        directory,
        extra={"event": "exchange_enabled", "workspace_id": workspace.workspace_id},
    )
    return True


# ---------------------------------------------------------------------------
# Installing files
# ---------------------------------------------------------------------------


def _open_child(parent_fd: int, name: str, *, label: str) -> int:
    """Open one exchange subdirectory, recreating it (by descriptor) when a client removed it."""

    try:
        return _open_directory(name, parent_fd, label=label)
    except ExchangeUnavailableError:
        if _lexists(name, parent_fd):
            raise
    try:
        os.mkdir(name, 0o755, dir_fd=parent_fd)
    except FileExistsError:
        pass
    return _open_directory(name, parent_fd, label=label)


def _install(directory_fd: int, name: str, data: bytes, *, durable: bool, mode: int = 0o644) -> None:
    """Install one file at *name* by an exclusive temporary and a rename, both relative to *directory_fd*.

    The rename replaces whatever is at *name* (a client's symlink is replaced,
    never followed); a directory there refuses the rename. *mode* is the
    file's permission bits (``0o644`` for documents a client reads).
    """

    _fs.write_file(_fs.anchored(directory_fd, name), data, durable=durable, mode=mode)


def publish_file(exchange_fd: int, name: str, data: bytes, *, directory: str | None = None) -> None:
    """Install one file into the exchange, or into one of its direct subdirectories, by descriptor.

    The subdirectory is opened (and recreated, by descriptor, when a client removed it) without following a
    symlink, and the file goes in through :func:`_install`. Used by the daemon for ``managers.json`` and
    ``managers/<handle>.log``; nothing in the exchange is ever read back.

    :param exchange_fd: The exchange directory's descriptor.
    :param name: The file's name in the target directory.
    :param data: The file's bytes.
    :param directory: A direct subdirectory of the exchange, or ``None`` for the exchange root.
    :raises ExchangeUnavailableError: If the subdirectory is a symlink or another file.
    :raises OSError: If the file cannot be written.
    """

    if directory is None:
        _install(exchange_fd, name, data, durable=True)
        return
    target = _open_child(exchange_fd, directory, label=f"{EXCHANGE_DIRECTORY}/{directory}")
    try:
        _install(target, name, data, durable=True)
    finally:
        os.close(target)


def install_document(workspace_root: Path, name: str, data: bytes) -> None:
    """Install one document at the exchange root of the workspace at *workspace_root*.

    The exchange is opened from a workspace-root descriptor without following any
    symlink, and the document goes in through :func:`_install` (an exclusive
    temporary and a rename relative to the exchange descriptor), so whatever a
    client left at *name* is replaced, never followed. Used for ``daemon.json``.

    :param workspace_root: The workspace root directory.
    :param name: The document's name in the exchange root.
    :param data: The document's bytes.
    :raises ExchangeUnavailableError: If the exchange is absent, a symlink or not a directory.
    :raises OSError: If the document cannot be written.
    """

    root = os.open(workspace_root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        exchange = _open_directory(EXCHANGE_DIRECTORY, root, label=str(workspace_root / EXCHANGE_DIRECTORY))
    finally:
        os.close(root)
    try:
        _install(exchange, name, data, durable=True)
    finally:
        os.close(exchange)


# ---------------------------------------------------------------------------
# The exchange pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Directories:
    """The exchange directories of one pass, each opened without following a symlink."""

    exchange: int
    inbox: int
    requests: int
    responses: int


@contextmanager
def _opened(workspace: Workspace) -> Iterator[_Directories]:
    root = os.open(workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        exchange = _open_directory(EXCHANGE_DIRECTORY, root, label=str(exchange_directory(workspace)))
    finally:
        os.close(root)
    opened = [exchange]
    try:
        for name in ("inbox", "requests", "responses"):
            opened.append(_open_child(exchange, name, label=f"{EXCHANGE_DIRECTORY}/{name}"))
        yield _Directories(*opened)
    finally:
        for descriptor in opened:
            os.close(descriptor)


def _listing(directory_fd: int) -> list[str]:
    """Return a directory's names, sorted, from a listing bounded to ``MAX_DIRECTORY_ENTRIES``."""

    with os.scandir(directory_fd) as entries:
        return sorted(entry.name for _count, entry in zip(range(MAX_DIRECTORY_ENTRIES), entries, strict=False))


def _eligible(name: str) -> bool:
    """Report whether an inbox entry name is one a manager adopts (dot, reserved and scratch names never are)."""

    return _BUNDLE_NAME.fullmatch(name) is not None and name not in _RESERVED_NAMES | _moving.RESERVED_NAMES


def _authorized_keys(workspace: Workspace) -> frozenset[str]:
    """Return the keys of the setting ``exchange.authorized_keys``: one string of comma- or space-separated keys."""

    value = workspace.read_settings().get(AUTHORIZED_KEYS_SETTING)
    return frozenset(value.replace(",", " ").split()) if isinstance(value, str) else frozenset()


def _excerpt(data: bytes | None) -> dict[str, object] | None:
    """Return the sanitized excerpt of a job's ``progress.json``: its scalar members, bounded, or ``None``."""

    if data is None:
        return None
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeError, ValueError, RecursionError):
        return None
    if not isinstance(value, dict):
        return None
    excerpt: dict[str, object] = {}
    for key, item in value.items():
        if len(excerpt) >= _PROGRESS_KEYS:
            break
        if not isinstance(key, str) or not key.isprintable() or len(key) > 64:
            continue
        if isinstance(item, str):
            excerpt[key] = "".join(character for character in item if character.isprintable())[:_PROGRESS_TEXT]
        elif isinstance(item, bool | int) or (isinstance(item, float) and math.isfinite(item)):
            excerpt[key] = item
    return excerpt


@dataclass(frozen=True)
class _ExchangeJob:
    """One indexed exchange job as a pass found it."""

    exchange_name: str
    job_id: str
    placement: PurePosixPath
    ref: JobRef | None


class ExchangeService:
    """The exchange pass of one manager: inbox adoption, job control, returns and ``status.json``.

    Every pass opens the exchange directories anew from the workspace root, each ``O_NOFOLLOW``, so a client
    that replaces one by a symlink stops the pass (it is reported) instead of steering it. Each step reports and
    swallows its own failures, so nothing a client writes stops the manager; only a lost owner propagates.

    :param workspace: The workspace the manager serves.
    :param owner: The manager's owner.
    """

    def __init__(self, workspace: Workspace, owner: _kernel.Owner) -> None:
        self.workspace = workspace
        self.owner = owner
        self._last: dict[str, float] = {}
        self._dirty = True
        # The monotonic time of the last return request posted per job.
        self._posted: dict[str, float] = {}
        # Request names found to be daemon actions, which managers leave alone.
        self._ignored: set[str] = set()
        # The last problem reported per subject, so a persisting one is logged once.
        self._reported: dict[str, str] = {}

    def invalidate(self) -> None:
        """Note that workflow state changed, so the next pass looks for finished exchange trees at once."""

        self._dirty = True

    def _report(self, subject: str, message: str, *, event: str, level: int = logging.WARNING) -> None:
        if self._reported.get(subject) == message:
            return
        self._reported[subject] = message
        _LOGGER.log(level, "%s", message, extra={"event": event, "workspace_id": self.workspace.workspace_id})

    def _due(self, step: str, now: float) -> bool:
        last = self._last.get(step)
        if last is not None and 0.0 <= now - last < STEP_INTERVAL:
            return False
        self._last[step] = now
        return True

    def run(self) -> bool:
        """Run one pass: adopt one inbox entry, translate job actions, return finished trees, write the status.

        :return: Whether the pass changed workflow state (adopted, refused, translated or requested a return).
        :raises httk.workflow._kernel.OwnerLost: When this manager's owner was recovered (fail-stop).
        """

        try:
            with _opened(self.workspace) as directories:
                self._reported.pop("unavailable", None)
                changed = False
                for name, step in (
                    ("adoption", self._adopt),
                    ("job control", self._job_control),
                    ("return", self._returns),
                ):
                    try:
                        changed |= step(directories)
                    except _kernel.OwnerLost:
                        raise
                    except Exception as exc:
                        _LOGGER.debug("the exchange %s step failed", name, exc_info=True)
                        self._report(
                            f"step:{name}",
                            f"the exchange {name} step failed: {type(exc).__name__}: {exc}",
                            event="exchange_step_failed",
                            level=logging.ERROR,
                        )
                    else:
                        self._reported.pop(f"step:{name}", None)
                return changed
        except (ExchangeUnavailableError, OSError) as exc:
            self._report("unavailable", f"the exchange is not served: {exc}", event="exchange_unavailable")
            return False

    # -- inbox ------------------------------------------------------------------------------------------------------

    def _adopt(self, directories: _Directories) -> bool:
        """Adopt (or refuse) the first inbox entry another manager does not take first."""

        rejected = exchange_directory(self.workspace) / "outbox" / "rejected"
        for name in _listing(directories.inbox):
            if not _eligible(name):
                continue
            try:
                report = _moving.adopt(
                    self.workspace,
                    self.owner,
                    _fs.anchored(directories.inbox, name),
                    untrusted=True,
                    refused_to=rejected,
                )
            except BundleError as exc:
                _LOGGER.warning(
                    "exchange inbox entry %s: %s", name, exc, extra={"event": "exchange_refused", "entry": name}
                )
                return True
            if report is None:
                continue  # another manager took it
            self._dirty = True
            _LOGGER.info(
                "exchange inbox entry %s: %s",
                name,
                "already adopted" if report.already_adopted else f"adopted {len(report.published)} jobs",
                extra={"event": "exchange_adopted", "entry": name},
            )
            if report.missing_workflows:
                _LOGGER.warning(
                    "exchange inbox entry %s needs workflows that are not installed: %s",
                    name,
                    ", ".join(report.missing_workflows),
                )
            return True
        return False

    # -- job control ------------------------------------------------------------------------------------------------

    def _job_control(self, directories: _Directories) -> bool:
        """Translate, answer and delete every pending job action of the request mailbox."""

        durable = self.workspace.durable
        keys: frozenset[str] | None = None
        changed = False
        names = [name for name in _listing(directories.requests) if _PUBLICATION.fullmatch(name)]
        self._ignored &= set(names)
        for name in names:
            if name in self._ignored or _fs.exists(_fs.anchored(directories.responses, name)):
                continue
            try:
                data = _fs.read_bounded(_fs.anchored(directories.requests, name), MAX_DOCUMENT_BYTES, nonblock=True)
                request = None if data is None else decode_request(data)
            except (_fs.UnsafePath, _fs.TooLarge, ValueError):
                request = None  # not a valid publication: the daemon discards it
            if request is None:
                continue
            if request.operation not in JOB_OPERATIONS or name != f"{request.request_id}.json":
                self._ignored.add(name)
                continue
            if keys is None:
                keys = _authorized_keys(self.workspace)
            response = self._translate(request, keys)
            # Unsigned, and the same bytes from every manager; then the request goes.
            _fs.write_file(_fs.anchored(directories.responses, name), encode_response(response), durable=durable)
            _fs.remove_file(_fs.anchored(directories.requests, name), durable=durable)
            _LOGGER.info(
                "exchange job action %s %s for %s: %s%s",
                request.request_id,
                request.operation,
                request.job,
                response.outcome,
                "" if response.reason is None else f" ({response.reason})",
                extra={"event": "exchange_job_action", "request_id": request.request_id},
            )
            changed = True
        return changed

    def _translate(self, request: Request, keys: frozenset[str]) -> Response:
        """Post the workspace request of one job action, or say why it is refused."""

        def answer(outcome: str, reason: str | None = None) -> Response:
            return Response(
                request.request_id,
                request.workspace_id,
                request.enrollment_id,
                request_digest(request),
                outcome,
                reason=reason,
            )

        try:
            verify_request(request, keys)
        except ValueError:
            return answer("refused", "request_unauthorized")
        try:
            check_request_time(request)
        except ValueError:
            return answer("refused", "request_expired")
        if request.workspace_id != self.workspace.workspace_id:
            return answer("refused", "wrong_workspace")
        assert request.job is not None
        indexed = _kernel.exchange_index(self.workspace, request.job)
        if indexed is None:
            return answer("refused", "unknown_job")
        job_id, placement, _nonce = indexed
        action = _ACTIONS[request.operation]
        eject = action == "eject"
        outbox = exchange_directory(self.workspace) / "outbox" / request.job
        # Deterministic name and content, so every translating manager writes the same request: the operator
        # "exchange" marks it exchange-derived for the owner's at-most-once guard.
        _requests.post(
            self.workspace,
            action=action,
            job_id=job_id,
            placement=placement,
            operator="exchange",
            reason=f"{request.operation} signed by {request.operator_key}",
            request_id=str(uuid.UUID(request.request_id)),
            created_at=datetime.fromtimestamp(request.created_at, UTC).isoformat(),
            destination=str(outbox) if eject else None,
            tree=eject,
        )
        return answer("accepted")

    # -- returns and status -----------------------------------------------------------------------------------------

    def _scan(self) -> list[_ExchangeJob]:
        """List the indexed exchange jobs, each located at its indexed placement."""

        directory = self.workspace.control / "exchange-jobs"
        try:
            names = sorted(os.listdir(directory))
        except FileNotFoundError:
            return []
        jobs, cache = [], _kernel.ListingCache()
        for name in names:
            try:
                indexed = _kernel.exchange_index(self.workspace, name)
            except FormatError:
                indexed = None  # not an exchange name: a leftover the kernel never writes
            if indexed is None:
                continue
            job_id, placement, _nonce = indexed
            ref = _kernel.locate(self.workspace, job_id, placement_hint=placement, cache=cache)
            jobs.append(_ExchangeJob(name, job_id, placement, ref))
        return jobs

    def _returns(self, directories: _Directories) -> bool:
        now = time.monotonic()
        returns = self._dirty or self._due("returns", now)
        status = self._due("status", now) and not self._status_fresh(directories)
        if not (returns or status):
            return False
        self._dirty = False
        jobs = self._scan()
        changed = False
        if returns:
            for job in jobs:
                changed |= self._return(job, now)
        if status:
            document = self._status_document(jobs)
            _install(
                directories.exchange, STATUS_DOCUMENT, json_bytes(document) + b"\n", durable=self.workspace.durable
            )
        return changed

    def _return(self, job: _ExchangeJob, now: float) -> bool:
        """Post the ``eject`` request of one finished exchange tree to ``outbox/<exchange-name>/``."""

        ref = job.ref
        if ref is None or ref.state not in TERMINAL_STATES:
            return False
        last = self._posted.get(job.job_id)
        if last is not None and 0.0 <= now - last < STEP_INTERVAL:
            return False  # an earlier request may still be applied, or its eject rolled back (occupied outbox)
        doc, damaged = read_state_unowned(ref.path / "state.json")
        if damaged or doc is None or doc.origin != "exchange":
            return False
        try:
            members = _moving.tree_members(self.workspace, job.job_id, doc)
        except _moving.Busy:
            return False
        if any(member.state not in TERMINAL_STATES for member in members):
            return False
        # One id per state of the root: every manager that sees this state posts the same request, and a rolled
        # back eject (which records the applied request) changes the state, so the retry is a fresh request.
        digest = hashlib.sha256(encode_state(doc)).hexdigest()
        _requests.post(
            self.workspace,
            action="eject",
            job_id=job.job_id,
            placement=job.placement,
            operator="exchange-return",
            reason="return the finished exchange tree to the client",
            destination=str(exchange_directory(self.workspace) / "outbox" / job.exchange_name),
            tree=True,
            request_id=str(uuid.uuid5(_RETURN_NAMESPACE, f"{job.job_id}/{digest}")),
        )
        self._posted[job.job_id] = now
        _LOGGER.info(
            "requested the return of finished exchange job %s (%s)",
            ref.job_key,
            job.exchange_name,
            extra={"event": "exchange_return_requested", "job_key": ref.job_key},
        )
        return True

    def _status_fresh(self, directories: _Directories) -> bool:
        """Report whether ``status.json`` was installed (by any manager) less than :data:`STEP_INTERVAL` ago."""

        try:
            info = os.stat(STATUS_DOCUMENT, dir_fd=directories.exchange, follow_symlinks=False)
        except OSError:
            return False
        return stat.S_ISREG(info.st_mode) and 0.0 <= time.time() - info.st_mtime < STEP_INTERVAL

    def _status_document(self, jobs: list[_ExchangeJob]) -> dict[str, object]:
        """Render ``status.json``: every present exchange job with its state and progress excerpt, bounded."""

        listed: list[dict[str, object]] = []
        size, truncated = 512, False
        for job in jobs:
            ref = job.ref
            if ref is None:
                continue
            entry: dict[str, object] = {
                "exchange_name": job.exchange_name,
                "job_id": job.job_id,
                "job_key": ref.job_key,
                "state": "running" if ref.state == OWNED else ref.state,
            }
            try:
                progress = _excerpt(read_job_file(ref.path, PROGRESS_DOCUMENT, _PROGRESS_LIMIT))
            except (_fs.UnsafePath, _fs.TooLarge, OSError):
                progress = None
            if progress is not None:
                entry["progress"] = progress
            size += len(json_bytes(entry)) + 1
            if size > STATUS_LIMIT:
                truncated = True
                break
            listed.append(entry)
        return {
            "format": STATUS_FORMAT,
            "format_version": 2,
            "workspace_id": self.workspace.workspace_id,
            "updated_at": utc_now(),
            "jobs": listed,
            "truncated": truncated,
        }
