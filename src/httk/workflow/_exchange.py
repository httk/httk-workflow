"""The exchange extension: ``WORKSPACE/exchange``, a client-written inbox and a returning outbox.

A workspace with the ``exchange`` extension (``format.json`` ``extensions``)
has one directory a client may write, ``WORKSPACE/exchange``:

.. code-block:: text

    exchange/
    ├── exchange.json          written at enable: format, format_version, workspace_id
    ├── status.json            managers: job states, informational
    ├── inbox/<name>/          client -> workspace: ejected job bundles
    ├── outbox/<job-key>/      workspace -> client: finished exchange-origin trees
    ├── outbox/rejected/<unique>/   one refused bundle: <name>/ and reason.json
    ├── requests/  responses/  the signed daemon mailbox
    └── managers/              daemon-published manager logs

:func:`enable_exchange` creates it. Every manager without a placement-prefix or
pool restriction then runs :func:`exchange_pass` first in each tick: it adopts
inbox bundles through the adoption chain (:mod:`~httk.workflow._adoption`),
ejects finished exchange-origin trees into the outbox through the sealing
transaction (:mod:`~httk.workflow._sealing`), and publishes ``status.json``.

The writer of ``exchange/`` is whoever can write it, which the workspace cannot
verify, so the trusted side treats it as hostile territory: every access is
anchored at directory descriptors opened ``O_NOFOLLOW`` on every pass, nothing
there is ever read back, and the only operations on it are a claim rename out
of ``inbox/``, a commit rename into ``outbox/``, a refused bundle's rename into
a fresh ``outbox/rejected/<unique>/``, and the ``status.json`` install.
"""

import errno
import logging
import os
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import _txn
from ._adoption import (
    ExchangeDescriptors,
    Source,
    adopt,
    claims_directory,
    pending_lineages,
    recover_lineages,
    resume_owned,
)
from ._daemon_mailbox import MAX_DIRECTORY_ENTRIES
from ._daemon_protocol import _BUNDLE_NAME, _RESERVED_NAMES
from ._sealing import ejection_members, fenced_transactions, recover
from ._util import json_bytes, read_json, timestamp_seconds, write_json_atomic
from .errors import FormatError, SealedError, WorkflowError
from .manifests import read_maintenance_lock
from .models import EXCHANGE_DIRECTORY, EXCHANGE_EXTENSION, STATE_KINDS, TERMINAL_KINDS, JobDefinition, Marker
from .transfers import (
    _require_tree_boundary,
    _unresolved_join_reference,
    eject_job,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "EXCHANGE_DOCUMENT",
    "EXCHANGE_FORMAT",
    "EXCHANGE_SUBDIRECTORIES",
    "RETURN_GRACE_SECONDS",
    "STATUS_DOCUMENT",
    "STATUS_FORMAT",
    "STATUS_LIMIT",
    "STEP_INTERVAL",
    "ExchangeService",
    "ExchangeUnavailableError",
    "enable_exchange",
    "exchange_directory",
]

#: The identity document :func:`enable_exchange` writes at the exchange root.
EXCHANGE_DOCUMENT = "exchange.json"
EXCHANGE_FORMAT = "httk-workspace-exchange"
#: The informational status document managers install at the exchange root.
STATUS_DOCUMENT = "status.json"
STATUS_FORMAT = "httk-workspace-exchange-status"
#: The directories of an enabled exchange, parents before children.
EXCHANGE_SUBDIRECTORIES = ("inbox", "outbox", "outbox/rejected", "requests", "responses", "managers")

#: The bound of ``status.json``; the job list is cut to fit and ``truncated`` set.
STATUS_LIMIT = 1024 * 1024
#: The least time between two recovery runs, two scans for finished trees and
#: two ``status.json`` installs of one manager. Adoption and the return of an
#: already found tree run on every pass, one each.
STEP_INTERVAL = 10.0
#: How long a finished tree stays before it is returned, so an attempt process
#: whose outcome a manager committed before reaping it has exited before its
#: payload moves.
RETURN_GRACE_SECONDS = 60.0

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_PROBE_PREFIX = ".httk-probe-"


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
        _txn.link_new(exchange_fd, EXCHANGE_DOCUMENT, _exchange_document(workspace), durable=workspace.durable)


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


def _rename_probe(workspace: Workspace, exchange_fd: int) -> None:
    """Rename a probe file from ``tmp/`` into ``exchange/`` and back: every exchange move is a rename.

    Bind mounts defeat a device comparison, so the rename itself is the test.

    :param workspace: The workspace.
    :param exchange_fd: The exchange directory's descriptor.
    :raises ExchangeUnavailableError: If the rename crosses filesystems (``EXDEV``).
    """

    tmp = os.open(workspace.control / "tmp", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    name = f"{_PROBE_PREFIX}{uuid.uuid4().hex}"
    try:
        os.close(os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=tmp))
        try:
            os.rename(name, name, src_dir_fd=tmp, dst_dir_fd=exchange_fd)
            os.rename(name, name, src_dir_fd=exchange_fd, dst_dir_fd=tmp)
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                raise ExchangeUnavailableError(
                    f"{exchange_directory(workspace)} and {workspace.control / 'tmp'} are on different filesystems "
                    "(or different mounts of one), so bundles cannot move between them by rename; the exchange "
                    "must live on the workspace's own filesystem and mount"
                ) from None
            raise
        finally:
            for directory in (exchange_fd, tmp):
                try:
                    os.unlink(name, dir_fd=directory)
                except FileNotFoundError:
                    pass
    finally:
        os.close(tmp)


def enable_exchange(workspace: Workspace) -> bool:
    """Enable the exchange extension: create ``WORKSPACE/exchange`` and record the extension.

    The directory is built privately below ``tmp/`` with every subdirectory and
    ``exchange.json`` and renamed into place complete; a rename probe between
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

    workspace._require_unsealed()
    workspace.refresh_format()
    if EXCHANGE_EXTENSION in workspace.extensions and _layout_complete(workspace):
        return False
    directory = exchange_directory(workspace)
    if not _lexists(directory):
        document = _exchange_document(workspace)

        def build(staging: Path) -> None:
            for relative in EXCHANGE_SUBDIRECTORIES:
                (staging / relative).mkdir(0o755)
            (staging / EXCHANGE_DOCUMENT).write_bytes(document)

        try:
            _txn.birth(
                workspace.control / "tmp", EXCHANGE_DIRECTORY, build, durable=workspace.durable, parent=workspace.root
            )
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                raise ExchangeUnavailableError(
                    f"{workspace.root} and {workspace.control / 'tmp'} are on different filesystems, so the "
                    "exchange cannot be served"
                ) from None
            raise
    root = os.open(workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        exchange = _open_directory(EXCHANGE_DIRECTORY, root, label=str(directory))
    finally:
        os.close(root)
    try:
        _complete_layout(workspace, exchange)
        _rename_probe(workspace, exchange)
    finally:
        os.close(exchange)
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
# The exchange pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Descriptors:
    """The exchange directories of one pass, each opened ``O_NOFOLLOW`` relative to its parent."""

    exchange: int
    inbox: int
    outbox: int
    rejected: int

    def adoption(self) -> ExchangeDescriptors:
        return ExchangeDescriptors(inbox_fd=self.inbox, rejected_fd=self.rejected)


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


@contextmanager
def _opened(workspace: Workspace) -> Iterator[_Descriptors]:
    """Open ``exchange``, ``inbox``, ``outbox`` and ``outbox/rejected`` for one pass, never following a link.

    :param workspace: The workspace.
    :yield: The descriptors, closed on exit.
    :raises ExchangeUnavailableError: If ``exchange`` (or a subdirectory) is not a real directory.
    """

    opened: list[int] = []
    root = os.open(workspace.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        exchange = _open_directory(EXCHANGE_DIRECTORY, root, label=f"{exchange_directory(workspace)}")
        opened.append(exchange)
        inbox = _open_child(exchange, "inbox", label=f"{EXCHANGE_DIRECTORY}/inbox")
        opened.append(inbox)
        outbox = _open_child(exchange, "outbox", label=f"{EXCHANGE_DIRECTORY}/outbox")
        opened.append(outbox)
        rejected = _open_child(outbox, "rejected", label=f"{EXCHANGE_DIRECTORY}/outbox/rejected")
        opened.append(rejected)
        yield _Descriptors(exchange, inbox, outbox, rejected)
    finally:
        for descriptor in reversed(opened):
            os.close(descriptor)
        os.close(root)


def _eligible(name: str) -> bool:
    """Report whether an inbox entry name is one a manager adopts (dot and reserved names never are)."""

    return not name.startswith(".") and name not in _RESERVED_NAMES and _BUNDLE_NAME.fullmatch(name) is not None


def _inbox_names(inbox_fd: int) -> list[str]:
    """Return the eligible inbox entry names, sorted, from a listing bounded to ``MAX_DIRECTORY_ENTRIES``."""

    names: list[str] = []
    with os.scandir(inbox_fd) as entries:
        for count, entry in enumerate(entries):
            if count >= MAX_DIRECTORY_ENTRIES:
                break
            if _eligible(entry.name):
                names.append(entry.name)
    return sorted(names)


def _timestamp(now: float) -> str:
    return datetime.fromtimestamp(now, UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _install(directory_fd: int, name: str, data: bytes, *, durable: bool) -> None:
    """Install one file at *name* by an exclusive temporary and a rename, both relative to *directory_fd*.

    The rename replaces whatever is at *name* (a client's symlink is replaced,
    never followed); a directory there refuses the rename.
    """

    temporary = f".{name}.{uuid.uuid4().hex}"
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o644, dir_fd=directory_fd
    )
    try:
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fchmod(descriptor, 0o644)
            if durable:
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.rename(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except BaseException:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        raise
    if durable:
        os.fsync(directory_fd)


def status_document(workspace: Workspace, now: float) -> dict[str, Any]:
    """Render ``status.json``: every job of the workspace with its state, bounded to :data:`STATUS_LIMIT`.

    :param workspace: The workspace.
    :param now: The epoch second the document is dated.
    :return: The document; when the jobs do not all fit, the longest prefix
        (in job-key order) that does, with ``truncated`` set.
    """

    markers = sorted(workspace.scan_markers(STATE_KINDS), key=lambda item: item.job_key)
    jobs = [{"job_id": item.job_id, "job_key": item.job_key, "state": item.kind} for item in markers]
    document: dict[str, Any] = {
        "format": STATUS_FORMAT,
        "format_version": 1,
        "workspace_id": workspace.workspace_id,
        "updated_at": _timestamp(now),
        "jobs": jobs,
        "truncated": False,
    }
    if len(json_bytes(document)) + 1 <= STATUS_LIMIT:
        return document
    document["truncated"] = True
    low, high = 0, len(jobs)
    while low < high:
        middle = (low + high + 1) // 2
        document["jobs"] = jobs[:middle]
        if len(json_bytes(document)) + 1 <= STATUS_LIMIT:
            low = middle
        else:
            high = middle - 1
    document["jobs"] = jobs[:low]
    return document


class ExchangeService:
    """The exchange pass of one manager: adopt from the inbox, return to the outbox, publish the status.

    One instance belongs to one manager (one owner token, one thread). Every
    pass opens the exchange directories anew from the workspace root, each
    ``O_NOFOLLOW``, and does all its work relative to those descriptors, so a
    client that replaces ``exchange``, ``inbox``, ``outbox`` or
    ``outbox/rejected`` by a symlink stops the pass (it is reported) instead
    of steering it. A sealed or maintenance-locked workspace is skipped: the
    bundles wait in the inbox. Each step reports and swallows its own failures,
    so nothing a client writes stops the manager.

    :param workspace: The workspace the manager serves.
    :param owner: The manager's owner token (:func:`~httk.workflow._txn.owner_token`).
    :param pace: Called regularly while a bundle is verified (the manager's heartbeat).
    :raises httk.workflow.errors.FormatError: If *owner* is not an owner token.
    """

    def __init__(self, workspace: Workspace, *, owner: str, pace: Callable[[], None] | None = None) -> None:
        _txn.parse_owner_token(owner)
        self.workspace = workspace
        self.owner = owner
        self.pace = pace
        self._last: dict[str, float] = {}
        # The last scan for finished exchange-origin roots: each root's marker and
        # the time its current frame was written (None when unreadable), when it
        # ran, and whether a change since makes it too old for the census.
        self._roots: dict[str, tuple[Marker, float | None]] = {}
        self._scanned_at: float | None = None
        self._stale = True
        # Root job ids of the last scan still to be looked at by the return step.
        self._returns: list[str] = []
        # Inbox names whose claim failed with an OS error, until when they are left alone.
        self._backoff: dict[str, float] = {}
        # The last problem reported per subject, so a persisting one is logged once.
        self._reported: dict[str, str] = {}

    # -- reporting ------------------------------------------------------------------------

    def _report(self, subject: str, message: str, *, event: str, level: int = logging.WARNING) -> None:
        if self._reported.get(subject) == message:
            return
        self._reported[subject] = message
        _LOGGER.log(level, "%s", message, extra={"event": event, "workspace_id": self.workspace.workspace_id})

    def _clear(self, subject: str) -> None:
        self._reported.pop(subject, None)

    def _due(self, step: str, now: float) -> bool:
        last = self._last.get(step)
        if last is not None and 0.0 <= now - last < STEP_INTERVAL:
            return False
        self._last[step] = now
        return True

    # -- the pass -------------------------------------------------------------------------

    def _paused(self) -> str | None:
        """Say why the workspace must not be changed now (sealed or maintenance-locked), or ``None``."""

        try:
            self.workspace._require_unsealed()
        except SealedError as exc:
            return str(exc)
        lock = read_maintenance_lock(self.workspace)
        if lock is not None and not lock.is_stale():
            return f"the maintenance lock is held by {lock.describe()}"
        return None

    def run(self, now: float | None = None) -> bool:
        """Run one exchange pass: recovery (rate limited), one adoption, one return, the status document.

        :param now: The current epoch second (the clock when omitted).
        :return: Whether the pass adopted, refused or returned a bundle.
        """

        now = time.time() if now is None else now
        paused = self._paused()
        if paused is not None:
            self._report("paused", f"the exchange waits: {paused}", event="exchange_paused", level=logging.INFO)
            return False
        self._clear("paused")
        try:
            with _opened(self.workspace) as descriptors:
                self._clear("unavailable")
                changed = self._steps(descriptors, now)
        except (ExchangeUnavailableError, OSError) as exc:
            self._report("unavailable", f"the exchange is not served: {exc}", event="exchange_unavailable")
            return False
        if changed:
            self._stale = True  # an adopted tree may be finished already
        return changed

    def _steps(self, descriptors: _Descriptors, now: float) -> bool:
        changed = False
        steps: tuple[tuple[str, Callable[[], bool]], ...] = (
            ("recovery", lambda: self._recover(descriptors, now)),
            ("adoption", lambda: self._adopt_one(descriptors, now)),
            ("return", lambda: self._return_one(descriptors, now)),
            ("status", lambda: self._publish_status(descriptors, now)),
        )
        for name, step in steps:
            try:
                changed |= step()
            except Exception as exc:
                # Reported once while it persists (a client's directory named
                # status.json refuses every install); the traceback goes to debug.
                _LOGGER.debug("the exchange %s step failed", name, exc_info=True)
                self._report(
                    f"step:{name}",
                    f"the exchange {name} step failed: {type(exc).__name__}: {exc}",
                    event="exchange_step_failed",
                    level=logging.ERROR,
                )
            else:
                self._clear(f"step:{name}")
            if self.pace is not None:
                self.pace()
        return changed

    def invalidate(self) -> None:
        """Note that workflow state changed, so the next census scans for finished trees afresh.

        The manager calls this after every tick that changed something: a job
        that just finished must be counted before an until-idle manager may exit.
        """

        self._stale = True

    def _recover(self, descriptors: _Descriptors, now: float) -> bool:
        """(a) Recovery at most every :data:`STEP_INTERVAL`, with the exchange's descriptors for exchange lineages."""

        if not self._due("recovery", now):
            return False
        exchange = descriptors.adoption()
        # transfers.recover_transfers, with the exchange's descriptors for exchange-inbox lineages.
        recover_lineages(self.workspace, owner=self.owner, exchange=exchange)
        recover(self.workspace)
        resume_owned(self.workspace, self.owner, exchange=exchange, pace=self.pace)
        return False

    def _adopt_one(self, descriptors: _Descriptors, now: float) -> bool:
        """(b) Adopt the first eligible inbox entry through the adoption chain, claimed by descriptor."""

        for name, until in list(self._backoff.items()):
            if until <= now:
                del self._backoff[name]
        for name in _inbox_names(descriptors.inbox):
            if name in self._backoff:
                continue
            source = Source(
                "exchange_inbox",
                exchange_directory(self.workspace) / "inbox" / name,
                directory_fd=descriptors.inbox,
                rejected_fd=descriptors.rejected,
            )
            try:
                result = adopt(self.workspace, source, owner=self.owner, pace=self.pace)
            except (WorkflowError, OSError, ValueError) as exc:
                self._backoff[name] = now + STEP_INTERVAL
                self._report(
                    f"adopt:{name}",
                    f"cannot adopt exchange inbox entry {name}: {exc}",
                    event="exchange_adopt_failed",
                )
                return False
            self._clear(f"adopt:{name}")
            _LOGGER.info(
                "exchange inbox entry %s: %s%s",
                name,
                result.status,
                "" if result.reason is None else f" ({result.reason})",
                extra={"event": "exchange_adopted", "entry": name, "status": result.status, "job_key": result.job_key},
            )
            return result.status != "lost"
        return False

    # -- returning finished trees -------------------------------------------------------------

    def _frame_time(self, marker: Marker) -> float | None:
        try:
            return timestamp_seconds(str(self.workspace.read_state(marker)["created_at"]))
        except (WorkflowError, OSError, KeyError, ValueError):
            return None

    def _scan_due(self, now: float) -> bool:
        return self._scanned_at is None or not 0.0 <= now - self._scanned_at < STEP_INTERVAL

    def _scan(self, now: float) -> None:
        """Find the terminal jobs whose current frame carries exchange origin; a damaged frame is skipped."""

        roots: dict[str, tuple[Marker, float | None]] = {}
        for marker in self.workspace.scan_markers(tuple(sorted(TERMINAL_KINDS))):
            try:
                frame = self.workspace.read_state(marker)
            except (WorkflowError, OSError):
                continue  # one damaged job never stops the others; the manager reports it
            if frame.get("origin") != "exchange":
                continue
            try:
                finished: float | None = timestamp_seconds(str(frame["created_at"]))
            except (KeyError, ValueError):
                finished = None
            roots[marker.job_id] = (marker, finished)
        self._roots = dict(sorted(roots.items(), key=lambda item: item[1][0].job_key))
        self._returns = list(self._roots)
        self._scanned_at = now
        self._stale = False

    def _waiting_map(self) -> dict[str, set[str]]:
        """Map join children to their waiting parents, skipping (and reporting) a damaged waiting frame.

        A child of a waiting job whose frame cannot be read is not known to be
        in a join; that job's join is broken already and cannot be resolved.
        """

        parents: dict[str, set[str]] = {}
        for waiting in self.workspace.scan_markers(("waiting",)):
            try:
                state = self.workspace.read_state(waiting)
            except (WorkflowError, OSError) as exc:
                self._report(
                    f"waiting:{waiting.job_id}",
                    f"the join of waiting job {waiting.job_key} cannot be read, so its children are not "
                    f"known to the exchange: {exc}",
                    event="exchange_waiting_unreadable",
                )
                continue
            self._clear(f"waiting:{waiting.job_id}")
            join = state.get("join")
            children = join.get("children", []) if isinstance(join, Mapping) else []
            for child in children if isinstance(children, list) else []:
                if not isinstance(child, Mapping) or child.get("workspace_id") != self.workspace.workspace_id:
                    continue
                child_id = child.get("job_id")
                if isinstance(child_id, str):
                    parents.setdefault(child_id, set()).add(waiting.job_id)
        return parents

    def _in_grace(self, marker: Marker, now: float) -> bool:
        finished = self._frame_time(marker)
        return finished is None or now - finished < RETURN_GRACE_SECONDS

    def _returnable(self, marker: Marker, waiting: Mapping[str, set[str]], now: float) -> bool:
        """(c) Decide that a finished exchange-origin root may return now, with its whole tree.

        It must be a top-level root, terminal for :data:`RETURN_GRACE_SECONDS`,
        outside any unresolved join, with every member paused or terminal (the
        terminal ones for the grace period too), every member of exchange
        origin (arrived with the tree, or registered here as a child of one of
        its jobs), no member transferring, no per-job claim on any job of the
        tree, and its ``job.json`` parent (if any) not transferring. A tree
        that may not return yet is pending, not an error; a tree that can never
        leave, or whose state cannot be read, is reported once.
        """

        try:
            return self._returnable_tree(marker, waiting, now)
        except (WorkflowError, OSError) as exc:
            self._report(
                f"return:{marker.job_id}",
                f"finished exchange job {marker.job_key} cannot be returned now: {exc}",
                event="exchange_return_unreadable",
            )
            return False

    def _returnable_tree(self, marker: Marker, waiting: Mapping[str, set[str]], now: float) -> bool:
        if self._in_grace(marker, now):
            return False
        workspace = self.workspace
        job = JobDefinition.from_path(workspace.payload_path(marker.placement, marker.job_key) / "job.json")
        parent = job.parent
        if parent is not None:
            parent_id = parent.get("job_id")
            if isinstance(parent_id, str) and workspace.find_marker_by_id(parent_id, kinds=("transferring",)):
                return False
        try:
            if _unresolved_join_reference(workspace, marker, waiting):
                return False
            _require_tree_boundary(workspace, marker, with_tree=True)
            members = ejection_members(workspace, marker, waiting)
        except FormatError as exc:
            self._report(
                f"return:{marker.job_id}",
                f"finished exchange job {marker.job_key} is never returned: {exc}",
                event="exchange_return_impossible",
            )
            return False
        except ValueError:
            return False
        for member in members:
            if workspace.read_state(member.marker).get("origin") != "exchange":
                # Not part of what the client sent nor registered here as a child
                # of the tree (an orphan a forged spawn record names): it stays,
                # and so does the tree.
                self._report(
                    f"return:{marker.job_id}",
                    f"finished exchange job {marker.job_key} is never returned: its tree member "
                    f"{member.marker.job_key} did not come through the exchange",
                    event="exchange_return_impossible",
                )
                return False
        claims = claims_directory(workspace)
        for job_id in (marker.job_id, *(member.marker.job_id for member in members)):
            if _lexists(claims / job_id):
                return False
        return not any(
            member.marker.kind in TERMINAL_KINDS and self._in_grace(member.marker, now) for member in members
        )

    def _return_one(self, descriptors: _Descriptors, now: float) -> bool:
        """(c) Eject the next finished exchange-origin tree into the outbox (at most one per pass)."""

        if self._scan_due(now):
            self._scan(now)
        if not self._returns:
            return False
        waiting = self._waiting_map()
        while self._returns:
            job_id = self._returns.pop(0)
            found = self._roots.get(job_id)
            if found is None or not _lexists(found[0].path):
                continue  # returned, removed or moved on since the scan
            marker = found[0]
            if not self._returnable(marker, waiting, now):
                continue
            if _lexists(marker.job_key, descriptors.outbox):
                # The client has not fetched an earlier copy yet: retried once it is gone.
                continue
            try:
                eject_job(
                    self.workspace,
                    marker.job_id,
                    exchange_directory(self.workspace) / "outbox",
                    marker=marker,
                    owner=self.owner,
                    waiting_parent_map=waiting,
                )
            except FileExistsError:
                return False
            except (WorkflowError, OSError, ValueError) as exc:
                self._report(
                    f"return:{marker.job_id}",
                    f"cannot return finished exchange job {marker.job_key}: {exc}",
                    event="exchange_return_failed",
                )
                return False
            self._clear(f"return:{marker.job_id}")
            self._roots.pop(job_id, None)
            _LOGGER.info(
                "returned finished exchange job %s to the outbox",
                marker.job_key,
                extra={"event": "exchange_returned", "job_key": marker.job_key},
            )
            return True
        return False

    def _publish_status(self, descriptors: _Descriptors, now: float) -> bool:
        """(d) Install ``status.json`` at the exchange root at most every :data:`STEP_INTERVAL`."""

        if not self._due("status", now):
            return False
        document = status_document(self.workspace, now)
        _install(descriptors.exchange, STATUS_DOCUMENT, json_bytes(document) + b"\n", durable=self.workspace.durable)
        return False

    # -- until-idle ---------------------------------------------------------------------------

    def outstanding(self, now: float | None = None) -> int:
        """Count the exchange work that keeps an until-idle manager running.

        Eligible inbox entries, finished exchange-origin trees still inside
        their grace period or ready to return, adoption lineages below
        ``tmp/``, and ejections still in flight (``transferring`` markers of
        transactions that are not addressed transfers waiting for their
        acknowledgement). The finished trees come from the last scan, made at
        most every :data:`STEP_INTERVAL` and again after every change
        (:meth:`invalidate`), so a quiet census does not re-read every
        finished job. A damaged frame is skipped, never raised.

        :param now: The current epoch second (the clock when omitted).
        :return: The number of outstanding items; zero while the pass is paused or unavailable.
        """

        now = time.time() if now is None else now
        if self._paused() is not None:
            return 0
        try:
            with _opened(self.workspace) as descriptors:
                count = len(_inbox_names(descriptors.inbox))
                if self._stale or self._scan_due(now):
                    self._scan(now)
                waiting: dict[str, set[str]] | None = None
                for marker, finished in self._roots.values():
                    if finished is None:
                        continue  # never returned; reported by the return step
                    if now - finished < RETURN_GRACE_SECONDS:
                        count += 1
                        continue
                    if not _lexists(marker.path) or _lexists(marker.job_key, descriptors.outbox):
                        continue
                    if waiting is None:
                        waiting = self._waiting_map()
                    if self._returnable(marker, waiting, now):
                        count += 1
        except (ExchangeUnavailableError, WorkflowError, OSError) as exc:
            self._report(
                "outstanding", f"cannot count the exchange's outstanding work: {exc}", event="exchange_unavailable"
            )
            return 0
        try:
            count += len(pending_lineages(self.workspace))
            for fenced in fenced_transactions(self.workspace).values():
                root = fenced.root
                outgoing = root[1].get("outgoing") if root is not None else None
                target = outgoing.get("target") if isinstance(outgoing, Mapping) else None
                if not (isinstance(target, Mapping) and target.get("kind") == "outgoing"):
                    count += 1
        except (WorkflowError, OSError) as exc:
            self._report(
                "outstanding", f"cannot count the exchange's transfers in flight: {exc}", event="exchange_unavailable"
            )
        return count
