"""The exchange extension: ``WORKSPACE/exchange``, a client-written inbox and a returning outbox.

A workspace with the ``exchange`` extension (``format.json`` ``extensions``)
has one directory a client may write, ``WORKSPACE/exchange``:

.. code-block:: text

    exchange/
    ├── exchange.json          written at enable: format, format_version, workspace_id
    ├── status.json            managers: job states, informational (from phase D)
    ├── inbox/<name>/          client -> workspace: ejected job bundles
    ├── outbox/<job-key>/      workspace -> client: finished exchange-origin trees
    ├── outbox/rejected/<unique>/   one refused bundle: <name>/ and reason.json
    ├── requests/  responses/  the signed daemon mailbox
    └── managers/              daemon-published manager logs

:func:`enable_exchange` creates it, and the workspace daemon installs
``daemon.json``, ``managers.json`` and the manager logs there
(:func:`install_document`, :func:`publish_file`). The exchange pass, by which
managers adopt inbox bundles and return finished trees, is unavailable in this
development version: it returns in phase D, rebuilt on the filesystem kernel.

The writer of ``exchange/`` is whoever can write it, which the workspace cannot
verify, so the trusted side treats it as hostile territory: every access is
anchored at directory descriptors opened ``O_NOFOLLOW``, nothing there is ever
read back, and a file goes in only by an exclusive temporary and a rename.
"""

import errno
import logging
import os
import uuid
from pathlib import Path

from . import _txn
from ._util import json_bytes, read_json, write_json_atomic
from .errors import FormatError, WorkflowError
from .models import EXCHANGE_DIRECTORY, EXCHANGE_EXTENSION
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "EXCHANGE_DOCUMENT",
    "EXCHANGE_FORMAT",
    "EXCHANGE_SUBDIRECTORIES",
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

    temporary = f".{name}.{uuid.uuid4().hex}"
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=directory_fd
    )
    try:
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fchmod(descriptor, mode)
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
