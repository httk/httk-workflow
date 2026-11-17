"""The hidden far-side :command:`workflow transfer-session` command.

One long-lived process answers typed transfer requests in binary frames (``httk.workflow._frames``) over its stdin
and stdout, so a transfer reaches a remote workspace through one connection and one ``httk`` start. It writes one
hello line naming the format, its version and the ops it serves, then answers one request at a time until the end
of its input or a ``bye`` request.

An op is a function of one :class:`Request` returning its response members; it refuses by raising :class:`Refused`.
The serve loop alone sets the reserved members ``ok`` and ``fatal``. Its upload (when the request announced
one) is :attr:`Request.upload`; its download is opened with :meth:`Request.download` once the upload was read to
its terminal. From the request until just before its response, a ticker thread sends the request's progress every
``_frames.TICK`` seconds. An op that answers ``ok`` must have read its upload to a successful terminal. Anything
escaping an op (or a response that cannot be sent) is fatal: it is answered ``{"ok": false, "fatal": true}`` and the
process exits at once, so whatever the op held is released by process death and recoverable as usual.

Accepted limitation: the far side has no read bound. A client that vanishes without closing the connection (a
half-open TCP connection) leaves it waiting for input until the transport notices (ssh's keepalives) and ends it.
"""

import argparse
import contextlib
import json
import os
import sys
import threading
import traceback
from collections.abc import Callable, Mapping
from typing import IO, Any, Self

from httk.core.cli import CLIContext

from .. import _frames
from .._frames import PROGRESS, REQUEST, RESPONSE, FrameReader, FrameWriter, StreamReader, StreamWriter
from ..adapters import TRANSFER_SESSION_FORMAT, TRANSFER_SESSION_FORMAT_VERSION
from ..errors import StreamAborted, TransferProtocolError
from ._common import _leaf

__all__ = [
    "OPS",
    "Op",
    "Refused",
    "Request",
    "build_transfer_session_parser",
    "handle_transfer_session",
    "serve",
]

#: The most bytes the ``echo`` op returns; it holds them in memory.
_ECHO_LIMIT = 1 << 20
#: The exit status after a fatal op, a protocol error, or an input that ended inside a request.
_FAILED = 1
#: The response members only the serve loop sets.
_RESERVED = frozenset({"ok", "fatal"})


class Refused(Exception):
    """Raised by an op to refuse its request: the response is ``ok: false`` with *error* and *members*.

    The request is answered normally (its unread upload discarded, an open download aborted), and the session goes
    on. Nothing may have changed when an op refuses.

    :param error: Why the request was refused.
    :param **members: Further response members; ``ok`` and ``fatal`` are reserved.
    """

    def __init__(self, error: str, **members: Any) -> None:
        super().__init__(error)
        self.error = error
        self.members = members


class Request:
    """One request as an op sees it.

    :param request_id: The request id.
    :param header: The decoded request document (``op`` and the op's own members).
    :param upload: The request's upload stream, when it announced one.
    :param frames: The writer of the response direction.
    """

    def __init__(
        self, request_id: int, header: Mapping[str, Any], upload: StreamReader | None, frames: FrameWriter
    ) -> None:
        self.id = request_id
        self.header = header
        self.upload = upload
        self._frames = frames
        self._download: StreamWriter | None = None
        self._lock = threading.Lock()
        self._phase: tuple[str, int | None] = (str(header.get("op")), None)
        self._discarding = False

    @property
    def phase(self) -> tuple[str, int | None]:
        """The current phase and count, as the next progress frame reports them."""

        with self._lock:
            if self._discarding and self.upload is not None:
                return ("discard", self.upload.bytes)
            return self._phase

    @property
    def download_writer(self) -> StreamWriter | None:
        """The download opened by :meth:`download`, if any."""

        return self._download

    def progress(self, phase: str, count: int | None = None) -> None:
        """Report the phase the op is in and its count so far.

        :param phase: The phase name (``extract``, ``settle``, ``sync``, ``adopt``, ...).
        :param count: How far the phase got; a client fails the request when neither it nor the data moves for its
            bound, outside the ``sync`` and ``build`` phases.
        """

        with self._lock:
            self._phase = (phase, count)

    def advance(self, n: int = 1) -> None:
        """Add *n* to the current phase's count.

        :param n: The increment.
        """

        with self._lock:
            phase, count = self._phase
            self._phase = (phase, (count or 0) + n)

    def download(self) -> StreamWriter:
        """Open the request's download stream; at most once, and only after the upload's terminal was read.

        :return: The stream writer; the serve loop aborts it if the op returns without closing it.
        :raises RuntimeError: If the download was opened already or the upload is still unread.
        """

        if self._download is not None:
            raise RuntimeError("a request has at most one download")
        if self.upload is not None and not self.upload.done:
            raise RuntimeError("a download starts only after the upload was read to its terminal")
        self._download = StreamWriter(self._frames, self.id)
        return self._download

    def discard_upload(self) -> None:
        """Read and drop what is left of the upload, reporting the bytes as the ``discard`` phase."""

        if self.upload is None or self.upload.done:
            return
        with self._lock:
            self._discarding = True
        try:
            self.upload.discard()
        finally:
            with self._lock:
                self._discarding = False


#: An op: takes the request and returns its response members (``ok`` defaults to true).
type Op = Callable[[Request], Mapping[str, Any]]


def _echo(request: Request) -> Mapping[str, Any]:
    """Return the upload unchanged as the download (the client's check that the channel is binary-transparent)."""

    if request.upload is None:
        raise Refused("echo needs an upload")
    data = bytearray()
    try:
        while len(data) <= _ECHO_LIMIT and (chunk := request.upload.read(_ECHO_LIMIT + 1 - len(data))):
            data += chunk
        if len(data) > _ECHO_LIMIT:
            raise Refused(f"echo carries at most {_ECHO_LIMIT} bytes")
        request.upload.finish()
    except StreamAborted as exc:
        raise Refused(f"echo upload aborted: {exc}") from None
    with request.download() as stream:
        stream.write(data)
    return {"bytes": len(data)}


def _bye(request: Request) -> Mapping[str, Any]:
    """Answer, after which the session ends."""

    return {}


#: The ops of format version 1 this far side serves, by name.
OPS: dict[str, Op] = {"echo": _echo, "bye": _bye}


class _Ticker:
    """Send the request's progress every ``_frames.TICK`` seconds until :meth:`stop`, and never after it."""

    def __init__(self, request: Request, frames: FrameWriter) -> None:
        self._request = request
        self._frames = frames
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="httk-transfer-ticker", daemon=True)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.stop()

    def stop(self) -> None:
        # Set under the lock: a tick waiting for it rechecks and sends nothing.
        with self._frames.lock:
            self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(_frames.TICK):
            phase, count = self._request.phase
            with self._frames.lock:
                if self._stop.is_set():
                    return
                try:
                    self._frames.send_json(PROGRESS, self._request.id, {"phase": phase, "count": count})
                except (OSError, ValueError):
                    return  # the output is gone; the op notices on its own writes


def _answer(ops: Mapping[str, Op], request: Request) -> dict[str, Any]:
    """Run the request's op and complete the grammar it left open.

    :param ops: The ops served, by name.
    :param request: The request.
    :return: The response document.
    :raises RuntimeError: If the op answered ok over an upload it did not read to a successful terminal (fatal).
    :raises TypeError: If the op returned no mapping or set a reserved member (fatal).
    """

    name = request.header.get("op")
    op = ops.get(name) if isinstance(name, str) else None
    if op is None:
        request.discard_upload()
        return {"ok": False, "error": f"unknown op {name!r}"}
    if name == "bye" and request.upload is not None:
        request.discard_upload()
        return {"ok": False, "error": "bye takes no upload"}
    try:
        result = op(request)
    except Refused as refusal:
        result, ok = {**refusal.members, "error": refusal.error}, False
    else:
        if not isinstance(result, Mapping):
            raise TypeError(f"op {name!r} returned {type(result).__name__}, not a mapping")
        ok = True
    reserved = sorted(_RESERVED & set(result))
    if reserved:
        raise TypeError(f"op {name!r} set the reserved response members {', '.join(reserved)}")
    response = {"ok": ok, **result}
    upload = request.upload
    if upload is not None:
        if response["ok"] and not upload.succeeded:
            raise RuntimeError(f"op {name!r} answered ok over an upload it did not read to a successful terminal")
        request.discard_upload()  # the far side answers only after it read the upload's terminal
    download = request.download_writer
    if download is not None and not download.closed:
        download.close(ok=False, abort="the op returned without completing its download")
    return response


def serve(requests: IO[bytes], responses: IO[bytes], *, ops: Mapping[str, Op] | None = None) -> int:
    """Answer framed requests until the end of the input or a ``bye`` request.

    Writes the hello line, then answers each request with exactly one response frame (preceded by its
    progress and download frames). An unknown op is answered ``ok: false`` (after its upload is discarded) and
    changes nothing. Anything escaping an op, a response that cannot be sent, or a protocol error the op swallowed is
    answered ``fatal`` and ends the loop with a nonzero status.

    :param requests: The protocol input stream.
    :param responses: The protocol output stream.
    :param ops: The ops served, by name; :data:`OPS` when omitted.
    :return: 0 after a ``bye`` or at the end of the input between requests; nonzero after a fatal op, a
        protocol error, or an input that ended inside a request.
    """

    served = OPS if ops is None else ops
    hello = {
        "format": TRANSFER_SESSION_FORMAT,
        "format_version": TRANSFER_SESSION_FORMAT_VERSION,
        "ops": sorted(served),
    }
    try:
        responses.write(json.dumps(hello, separators=(",", ":")).encode("utf-8") + b"\n")
        responses.flush()
    except OSError as exc:
        print(f"transfer session: {exc}", file=sys.stderr)
        return _FAILED
    reader = FrameReader(requests)
    frames = FrameWriter(responses.write, responses.flush)
    last_id = 0
    try:
        while (frame := reader.read_frame()) is not None:
            if frame.kind != REQUEST:
                raise TransferProtocolError(f"a {frame.kind} frame outside a request")
            if frame.id <= last_id:
                raise TransferProtocolError(f"request id {frame.id} does not follow {last_id}")
            last_id = frame.id
            assert frame.document is not None
            upload = StreamReader(reader.read_frame, frame.id) if frame.document.get("upload") is True else None
            request = Request(frame.id, frame.document, upload, frames)
            ticker = _Ticker(request, frames)
            with ticker:
                try:
                    response = _answer(served, request)
                    failure = reader.failure or (upload.failure if upload is not None else None)
                    if failure is not None:
                        raise failure  # the op swallowed a broken protocol
                    with frames.lock:
                        ticker.stop()
                        frames.send_json(RESPONSE, frame.id, response)
                except (Exception, SystemExit) as exc:
                    # Fail-stop: the op may have changed anything; ending the process releases what it held.
                    ticker.stop()
                    traceback.print_exc(file=sys.stderr)
                    error = f"{type(exc).__name__}: {exc}"
                    with contextlib.suppress(Exception):  # the answer is best effort; the exit is what matters
                        frames.send_json(RESPONSE, frame.id, {"ok": False, "fatal": True, "error": error})
                    return _FAILED
            if frame.document.get("op") == "bye" and response["ok"]:
                return 0
            if reader.ended:
                return _FAILED  # the input ended inside the request: nothing more can arrive
    except TransferProtocolError as exc:
        print(f"transfer session: {exc}", file=sys.stderr)
        return _FAILED
    except OSError as exc:  # the output is gone
        print(f"transfer session: {exc}", file=sys.stderr)
        return _FAILED
    return 0


def _isolate_streams() -> tuple[IO[bytes], IO[bytes]]:
    """Take fds 0 and 1 private as the binary protocol pipe and point the standard streams elsewhere.

    As for :command:`workflow session`: afterwards fd 0 and ``sys.stdin`` read ``/dev/null``, and fd 1 and
    ``sys.stdout`` write to stderr, so a stray ``print`` or a child inheriting fd 1 cannot corrupt a frame.

    :return: The protocol input and output streams.
    """

    requests = os.fdopen(os.dup(0), "rb")
    responses = os.fdopen(os.dup(1), "wb")
    null = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null, 0)
    os.close(null)
    os.dup2(2, 1)
    sys.stdin = os.fdopen(0, "r", encoding="utf-8", closefd=False)
    sys.stdout = sys.stderr
    return requests, responses


def handle_transfer_session(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Serve transfer requests over this process's stdin and stdout until the end of the input or ``bye``.

    A failed session exits at once (``os._exit``): a thread an op left behind must not keep the process, and what
    it holds, alive.
    """

    try:
        status = serve(*_isolate_streams())
    except BaseException:  # whatever escaped, the process must not linger on a thread an op left behind
        with contextlib.suppress(BaseException):  # best effort: a broken stderr must not skip the exit
            traceback.print_exc(file=sys.stderr)
        status = _FAILED
    if status:
        with contextlib.suppress(BaseException):
            sys.stderr.flush()
        os._exit(status)
    return 0


def build_transfer_session_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the hidden transfer-session command."""

    _leaf(
        subparsers,
        "transfer-session",
        summary="serve transfer requests in binary frames over stdin and stdout",
        description="Serve typed transfer requests as binary frames over stdin and stdout",
        handler=handle_transfer_session,
        hidden=True,
    )
