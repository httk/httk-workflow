"""The receiving side of a pushed tar stream: one landing, extracted, validated and made durable before any use.

The far side of a push (the transfer session's ``stage`` op), its ``install`` op and the near side of a pull over a
transfer session read a tar stream into a landing directory with :func:`receive_tar`. The stream is any binary file:
a transfer session's upload for an op, its download for a pull. This module
imports nothing of the command line.
"""

import ast
import io
import os
import re
import stat
import tarfile
from collections.abc import Buffer, Callable
from functools import partial
from pathlib import Path
from typing import IO, BinaryIO

from . import _fs
from .errors import StreamAborted, TransferProtocolError
from .workspace import Workspace

__all__ = ["CommitFailed", "Progress", "StreamRefused", "receive_tar"]


class StreamRefused(ValueError):
    """A stream refused whole: nothing of it was used, and its landing was discarded.

    :param message: Why.
    :param bundle: The top-level directory (bundle) whose member the extraction refused, when the refusal is one
        member's; ``None`` for anything else (a malformed stream, a filesystem error, an aborted stream).
    :param stream_aborted: The stream itself was incomplete: its sender aborted it, or the input ended inside it.
    """

    def __init__(self, message: str, *, bundle: str | None = None, stream_aborted: bool = False) -> None:
        super().__init__(message)
        self.bundle = bundle
        self.stream_aborted = stream_aborted


class CommitFailed(StreamRefused):
    """A stream refused because its sync barrier's ``syncfs`` commit failed under ``io.sync_errors`` ``fail``.

    A writeback error on the landing's filesystem, possibly another user's, so a delivery with ``--sync fsync``
    (whose fsyncs no foreign error can fail) may succeed. The ``job adopt --json`` refusal document says so with
    ``"sync_failed": true``.
    """


#: A Python string literal at the start of a refusal's message: the member name the extraction quotes with ``!r``.
_QUOTED = re.compile(r"""'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*\"""")


def _offending_bundle(exc: BaseException) -> str | None:
    """The top-level name of the member a refusal is about, or ``None`` when it cannot be attributed to one.

    A refused member is a :class:`tarfile.FilterError`: its subclasses carry the member (``tarinfo``), and every
    refusal of :func:`httk.workflow._fs.extract_tar` starts its message with the member's name as a Python literal.

    :param exc: What refused the stream.
    :return: The first component of the member's name (as the extraction normalizes it), unless that is ``..``.
    """

    if isinstance(exc, StreamRefused):
        return exc.bundle
    if not isinstance(exc, tarfile.FilterError):
        return None
    member = getattr(exc, "tarinfo", None)
    if isinstance(member, tarfile.TarInfo):
        name = member.name
    else:
        match = _QUOTED.match(str(exc))
        if match is None:
            return None
        try:
            name = ast.literal_eval(match.group())
        except (ValueError, SyntaxError):
            return None
        if not isinstance(name, str):
            return None
    parts = [part for part in name.split("/") if part not in ("", ".")]
    return parts[0] if parts and parts[0] != ".." else None


#: Told a phase and its count so far (a transfer session's :meth:`Request.progress`).
type Progress = Callable[[str, int | None], object]


class _Counted(io.RawIOBase, BinaryIO):
    """A readable stream reporting ``("extract", bytes read so far)`` to *progress* as it is read.

    :param stream: The stream read.
    :param progress: Told the phase and count.
    """

    def __init__(self, stream: IO[bytes], progress: Progress) -> None:
        super().__init__()
        self._stream = stream
        self._progress = progress
        self._read = 0

    def readable(self) -> bool:
        """Return true: the stream is readable."""

        return True

    def readinto(self, buffer: Buffer, /) -> int:
        """Read into *buffer* from the wrapped stream.

        :param buffer: The writable buffer.
        :return: The number of bytes read.
        """

        view = memoryview(buffer).cast("B")
        data = self._stream.read(len(view))
        view[: len(data)] = data
        self._read += len(data)
        self._progress("extract", self._read)
        return len(data)


def _extract_landing(
    workspace: Workspace,
    landing: Path,
    stream: IO[bytes],
    *,
    durable: bool,
    progress: Progress | None,
) -> tuple[list[str], int]:
    """Extract *stream* into *landing*: the top-level names it carried, sorted, each checked to be a directory, and
    how many regular files the stream wrote; *progress* is told ``extract`` with the bytes read, then ``sync`` for the
    extraction's tail."""

    names: set[str] = set()
    source: IO[bytes] = stream
    tail: Callable[[], object] | None = None
    if progress is not None:
        source = _Counted(stream, progress)
        progress("extract", 0)
        tail = partial(progress, "sync", None)  # the writes still running, the directory times and fsyncs
    count = _fs.extract_tar(
        source, landing, workers=workspace.io_workers, durable=durable, top_level=names, read_done=tail
    )
    for name in names:
        found = _fs.lstat(_fs.loc(landing / name))
        if found is None or not stat.S_ISDIR(found.st_mode):
            raise StreamRefused(f"not a directory: {name!r}", bundle=name)
    return sorted(names), count


def receive_tar(
    workspace: Workspace,
    landing: Path,
    stream: IO[bytes],
    *,
    barrier: bool = False,
    force_fsync: bool = False,
    finish: Callable[[], object] | None = None,
    progress: Progress | None = None,
    quiet: bool = False,
    primed_devices: set[int] | None = None,
) -> list[str]:
    """Extract the tar *stream* into *landing* (created when absent): the top-level directories it carried, sorted.

    The anchored extraction of the adapter's pull (:func:`httk.workflow._fs.extract_tar`) is the trust boundary,
    writing small files with up to the workspace's ``io.workers`` threads; every top-level entry of the stream must
    be a directory. A stream that fails either is refused whole and *landing* discarded (only after every write of
    the extraction finished), so nothing of it is used. Only the top-level names this stream carried are returned:
    any other entry of *landing* is left in place and never used (every caller lands in a fresh directory).

    :param workspace: The workspace *landing* belongs to (its ``tmp/`` takes the discarded landing).
    :param landing: The directory to extract into.
    :param stream: The binary tar stream, read once.
    :param barrier: The landing is what gets published (the far side of a push): in a durable workspace it is
        made durable before this returns, through a :class:`~httk.workflow._fs.Barrier` entered before the first
        member is extracted (``io.sync`` and ``io.sync_errors`` read afresh); a failed commit refuses the stream
        like a refused member. The ``install`` op leaves it off: its landing is copied into the store, not
        published.
    :param force_fsync: Use the strategy ``fsync`` whatever ``io.sync`` says, with *barrier*: every landed file is
        fsynced on its writing descriptor as it is extracted (a ``stage`` with ``sync: fsync``, a push's one retry after a
        failed commit); nothing more in a non-durable workspace. In any workspace, durable or not, *landing* must
        not exist yet: its parents are created, then *landing* itself exclusively, and an existing one (or one
        created concurrently) refuses the stream before anything is extracted or discarded.
    :param finish: Called after the extraction (every pooled write finished) and before the commit: a transfer
        session's :meth:`~httk.workflow._frames.StreamReader.finish`, which requires the upload's successful
        terminal. When it raises (an aborted upload, the end of the input), the stream is refused like a refused
        member.
    :param progress: Told each phase and its count, for a transfer session's progress: ``sync`` while the landing
        is made and the barrier entered (directory fsyncs, its prime's ``syncfs``), ``extract`` with the stream bytes
        read, then ``sync`` again once the reading ends, however it ends (the writes still running, the directory
        times and fsyncs, the terminal and the commit, or a refused stream's discard).
    :param quiet: Print no progress line of the sync barrier (``job transfer -q`` receiving a pull).
    :param primed_devices: With *barrier*, the transfer's set of filesystems a barrier of it primed already (see
        :class:`~httk.workflow._fs.Barrier`): a transfer session's ``adopt`` and ``stage``, a pull's landings. The
        barrier then primes only the first landing on a filesystem; by default it always primes.
    :return: The names of the top-level directories the stream carried, sorted.
    :raises StreamRefused: For a malformed, aborted or cut stream, a refused member (naming its bundle) or a failed
        sync barrier (:class:`CommitFailed` when the barrier's ``syncfs`` commit failed under ``io.sync_errors``
        ``fail``).
    :raises ValueError: With *force_fsync*, when *landing* exists already (nothing is changed then).
    """

    if progress is not None:
        progress("sync", None)  # the landing's directories are fsynced, then the barrier may prime with a syncfs
    if force_fsync:
        _fs.make_dirs(landing.parent, durable=workspace.durable)
        try:
            os.mkdir(landing)
        except FileExistsError:
            raise ValueError(
                f"--sync fsync needs a fresh landing, and {landing.name} exists already; nothing was used"
            ) from None
        if workspace.durable:
            parent = os.open(landing.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(parent)  # the landing's own entry, as make_dirs makes it durable
            finally:
                os.close(parent)
    else:
        _fs.make_dirs(landing, durable=workspace.durable)
    extract = partial(_extract_landing, workspace, landing, stream, progress=progress)
    try:
        if barrier and workspace.durable:
            # --sync fsync only strengthens: inline fsync on the writing descriptors, which no foreign error fails.
            policy: tuple[object, _fs.SyncErrors] = ("fsync", "fail") if force_fsync else workspace.io_sync_policy()
            with _fs.Barrier(
                landing, policy[0], errors=policy[1], inline=True, quiet=quiet, primed_devices=primed_devices
            ) as sync:
                if force_fsync:
                    sync.reason = "fsync (--sync fsync)"
                names, count = extract(durable=sync.fsync_inline)
                if finish is not None:
                    finish()  # the stream is whole before anything of it is committed
                try:
                    # Inline, so no walk: only a syncfs commit can fail here, and under ignore only for an
                    # unsupported syncfs, which a retry would not help.
                    sync.commit(count)
                except _fs.SyncFailed as exc:
                    if sync.errors != "fail":
                        raise
                    raise CommitFailed(str(exc)) from exc
        else:
            names, _count = extract(durable=False)
            if finish is not None:
                finish()
    except (tarfile.TarError, EOFError, OSError, ValueError, StreamAborted, TransferProtocolError) as exc:
        # Still in the sync phase the end of the reading reported: the discard walks and removes the landing.
        _fs.discard(_fs.loc(landing), trash_dir=workspace.control / "tmp", durable=workspace.durable)
        message = f"refused the stream, nothing of it was used: {exc}"
        if isinstance(exc, CommitFailed):
            raise CommitFailed(message) from None
        aborted = isinstance(exc, StreamAborted)
        raise StreamRefused(message, bundle=_offending_bundle(exc), stream_aborted=aborted) from None
    return names
