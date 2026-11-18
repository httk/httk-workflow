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

A push's segments may be staged (``stage``: received, committed and the first scan of their settle taken) and
adopted later on the same connection (``adopt_staged``, one token a request, in staging order; plan
WORKFLOW_TRANSFER_SESSION §7.10). Tokens are connection-local; a ``bye`` (before it answers) or the end of the input
abandons the stages whose adoption never began (their landings discarded); after a fatal op, or when the process
ends otherwise, they are left to recovery, which discards them.

Accepted limitation: the far side has no read bound. A client that vanishes without closing the connection (a
half-open TCP connection) leaves it waiting for input until the transport notices (ssh's keepalives) and ends it.
"""

import argparse
import contextlib
import io
import json
import logging
import os
import re
import stat
import sys
import tarfile
import threading
import traceback
from collections.abc import Buffer, Callable, Iterator, Mapping
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, BinaryIO, NamedTuple, Self, cast

from httk.core.cli import CLIContext

from .. import _frames, _fs
from .._frames import (
    PROGRESS,
    REQUEST,
    RESPONSE,
    FrameReader,
    FrameWriter,
    StreamReader,
    StreamWriter,
)
from ..adapters import TRANSFER_SESSION_FORMAT, TRANSFER_SESSION_FORMAT_VERSION
from ..errors import StreamAborted, TransferProtocolError
from ._common import _ERRORS, _leaf, _local_root, _modifiable

if TYPE_CHECKING:  # pragma: no cover
    from .._moving import Stage

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
    :param session: The state the session's requests share (a pull's selection), one mapping per connection.
    """

    def __init__(
        self,
        request_id: int,
        header: Mapping[str, Any],
        upload: StreamReader | None,
        frames: FrameWriter,
        session: dict[str, Any] | None = None,
    ) -> None:
        self.id = request_id
        self.header = header
        self.upload = upload
        #: The state the session's requests share, kept for the connection.
        self.session: dict[str, Any] = {} if session is None else session
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
            bound, outside the ``sync``, ``settle`` and ``build`` phases.
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
    """Abandon the session's stages whose adoption never began (their landings discarded), then answer, after which
    the session ends."""

    _abandon(request.session)
    return {}


@contextlib.contextmanager
def _captured() -> Iterator[io.StringIO]:
    """Capture what a request writes to standard error, for its response: what a command line would show.

    Standard error is redirected (process-wide, so the pooled threads of the request's work write there too), which
    also takes the records Python's last-resort handler renders when no handler is configured; and every console
    handler of the ``httk`` loggers (a plain stream handler on this process's standard error) writes to the capture
    for the request's time. Nothing else of the logging setup changes: each handler keeps its filters, formatting
    and level, and other consumers (files, an active :func:`httk.core.report.collect_reports`) still receive the
    records. So each record reaches the capture at most once, and this process's own standard error not at all.
    """

    capture = io.StringIO()
    consoles = {id(sys.stderr), id(sys.__stderr__)}
    loggers = [logging.getLogger("httk")]
    loggers += [
        logger
        for name, logger in logging.Logger.manager.loggerDict.items()
        if name.startswith("httk.") and isinstance(logger, logging.Logger)
    ]
    redirected = [
        handler
        for logger in loggers
        for handler in logger.handlers
        if type(handler) is logging.StreamHandler and id(handler.stream) in consoles
    ]
    saved = [handler.setStream(capture) for handler in redirected]
    try:
        with contextlib.redirect_stderr(capture):
            yield capture
    finally:
        for handler, stream in zip(redirected, saved, strict=True):
            handler.setStream(stream)


def _text(header: Mapping[str, Any], key: str, *, required: bool = False) -> str | None:
    value = header.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise Refused(f"{header.get('op')} needs {key} as a nonempty string")
    return value


def _texts(header: Mapping[str, Any], key: str) -> list[str]:
    value = header.get(key, [])
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise Refused(f"{header.get('op')} needs {key} as a list of nonempty strings")
    return value


def _flag(header: Mapping[str, Any], key: str) -> bool:
    value = header.get(key, False)
    if not isinstance(value, bool):
        raise Refused(f"{header.get('op')} needs {key} as a boolean")
    return value


def _workspace_arguments(name: str) -> argparse.Namespace:
    """The arguments a command line naming *name* with ``--workspace`` resolves its workspace from."""

    return argparse.Namespace(workspace=name, by_path=False, no_durable=False)


def _adopt(request: Request) -> Mapping[str, Any]:
    """Adopt the bundles the upload carries: ``job adopt --json --stdin-tar --token T [--sync fsync] --workspace WS
    [--built-for ID ...]`` (:func:`~httk.workflow.workflow_cli._job.adopt_stream`).

    Header: ``workspace`` (a registered name, resolved as the command line resolves it), ``token``, ``sync``
    (``"fsync"`` or absent) and ``built_for`` (workflow ids). The response carries that command's JSON
    ``document``, its ``returncode`` and its captured ``stderr``; it is ``ok`` exactly when the document is no
    ``refused`` one (a refusal is answered ``ok: false`` with those members, and ``error`` its reason). An error
    after the first adoption began is fatal.
    """

    header = request.header
    workspace = _text(header, "workspace", required=True)
    assert workspace is not None
    token = _text(header, "token")
    sync = header.get("sync")
    if sync not in (None, "fsync"):
        raise Refused(f"adopt takes sync fsync or none, not {sync!r}")
    built_for = _texts(header, "built_for")
    upload = request.upload
    if upload is None:
        raise Refused("adopt needs an upload")
    from ._job import adopt_stream

    context = CLIContext("httk", Path.cwd())
    with _captured() as stderr:
        document, returncode = adopt_stream(
            lambda: _modifiable(_workspace_arguments(workspace), context, action="adopt jobs into it"),
            upload,
            token=token,
            force_fsync=sync == "fsync",
            built_for=built_for,
            finish=upload.finish,
            progress=request.progress,
        )
    members = {"document": document, "returncode": returncode, "stderr": stderr.getvalue()}
    if "refused" in document:
        raise Refused(str(document["refused"]), **members)
    return members


#: The most segments one session keeps staged and not yet adopted (plan WORKFLOW_TRANSFER_SESSION §7.10, 1a), ...
_STAGES = 8
#: ... and the most landed regular-file bytes they hold (each staged segment's actual landed bytes), the incoming
#: segment's declaration included: a segment beyond either is refused before its data is used (``capacity``). A single
#: bundle larger than this is staged only when nothing else is, and refuses every later stage until it is adopted.
_STAGED_BYTES = 32 << 30


class _Staged(NamedTuple):
    """A staged segment of this session, by its token: the stage, the workspace and the transfer ids it is bound to,
    and the regular-file bytes that landed."""

    stage: "Stage"
    root: Path
    transfer_ids: tuple[str, ...]
    size: int


def _stages(request: Request) -> dict[str, _Staged]:
    """The session's staged segments not yet adopted, by token, in staging order."""

    return cast(dict[str, _Staged], request.session.setdefault("stages", {}))


def _stage(request: Request) -> Mapping[str, Any]:
    """Receive and commit the bundles the upload carries and take the first scan of their settle, for a later
    ``adopt_staged`` (:func:`~httk.workflow.workflow_cli._job.stage_stream`; plan WORKFLOW_TRANSFER_SESSION §7.10).

    Header: ``workspace``, ``sync`` (``"fsync"`` or absent), ``transfer_ids`` (the bundles the upload carries, each
    once, none staged already in this session) and ``bytes`` (their landed regular-file bytes). A header that breaks
    a rule, or a segment beyond the session's capacity (:data:`_STAGES` stages, :data:`_STAGED_BYTES` bytes counting
    the incoming declaration and the staged segments' actual landed bytes; ``capacity: true``; beyond the bytes,
    only a single bundle with nothing staged is admitted), is refused before the upload is used. The stage then
    records its actual landed regular-file bytes (hard-link copies counted), so the session's staged bytes exceed
    the cap by at most one segment. The answer is a ``token`` (connection-local, bound to
    the workspace and *transfer_ids*), the ``transfer_ids``, the ``workspace_id``, the landed ``bytes`` and ``matures_in``
    (advisory seconds until its deadline has run), with the captured ``stderr``. A refused stream is answered as
    ``adopt`` answers it (``document``, ``returncode``, ``stderr``); anything failing after the commit is fatal, and
    so is an upload carrying other bundles than *transfer_ids*.
    """

    header = request.header
    workspace = _text(header, "workspace", required=True)
    assert workspace is not None
    sync = header.get("sync")
    if sync not in (None, "fsync"):
        raise Refused(f"stage takes sync fsync or none, not {sync!r}")
    transfer_ids = _texts(header, "transfer_ids")
    if not transfer_ids or len(set(transfer_ids)) != len(transfer_ids):
        raise Refused("stage needs transfer_ids, each once")
    if not all(_TRANSFER_ID.fullmatch(transfer_id) for transfer_id in transfer_ids):
        raise Refused("stage needs transfer_ids, each a transfer id")
    size = header.get("bytes")
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise Refused("stage needs bytes as a nonnegative integer")
    upload = request.upload
    if upload is None:
        raise Refused("stage needs an upload")
    stages = _stages(request)
    if staged := sorted(set(transfer_ids) & {key for entry in stages.values() for key in entry.transfer_ids}):
        raise Refused(f"already staged in this session and not adopted: {', '.join(staged)}")
    held = sum(entry.size for entry in stages.values())
    if stages and (len(stages) >= _STAGES or held + size > _STAGED_BYTES):
        raise Refused(
            f"this session holds {len(stages)} staged segment(s) of {held} byte(s): adopt one first", capacity=True
        )
    if size > _STAGED_BYTES and len(transfer_ids) > 1:
        # Only a single bundle larger than the cap (never split) is staged beyond it, alone.
        raise Refused(f"{size} byte(s) of {len(transfer_ids)} bundles exceed a session's staged bytes", capacity=True)
    from ._job import stage_stream
    from ._transfer import _measure

    context = CLIContext("httk", Path.cwd())
    with _captured() as stderr:
        document, stage = stage_stream(
            lambda: _modifiable(_workspace_arguments(workspace), context, action="adopt jobs into it"),
            upload,
            force_fsync=sync == "fsync",
            finish=upload.finish,
            progress=request.progress,
        )
    if stage is None:
        raise Refused(str(document["refused"]), document=document, returncode=2, stderr=stderr.getvalue())
    if document["bundles"] != sorted(transfer_ids):
        # After the commit: fatal, its owner left for recovery (which discards the landing).
        raise RuntimeError(f"the upload carried the bundles {document['bundles']}, not those its header named")
    # Admitted by the declared bytes; later admissions count what actually landed (hard-link copies included).
    landed = sum(_measure(bundle)[0] for bundle in stage.bundles)
    token = _fs.fresh_token()
    stages[token] = _Staged(stage, stage.workspace.root.resolve(), tuple(transfer_ids), landed)
    return {
        "token": token,
        "transfer_ids": transfer_ids,
        "workspace_id": document["workspace_id"],
        "bytes": landed,
        "matures_in": stage.remaining(),
        "stderr": stderr.getvalue(),
    }


def _adopt_staged(request: Request) -> Mapping[str, Any]:
    """Adopt one staged segment once its visibility deadline has run, waiting out what is left of it
    (:func:`~httk.workflow.workflow_cli._job.adopt_stage`; plan WORKFLOW_TRANSFER_SESSION §7.10).

    Header: ``workspace``, ``token`` and ``transfer_ids``, which must name the session's oldest staged segment and
    what it was staged with; anything else (an unknown or consumed token, another workspace or other ids, another
    stage first, a workspace sealed since) is refused before anything is touched, and the stage stays. The token is
    consumed once accepted:
    the response is ``adopt``'s (``document``, ``returncode``, ``stderr``), and an error from then on is fatal.
    """

    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    token = _text(header, "token", required=True)
    assert workspace_name is not None and token is not None
    transfer_ids = _texts(header, "transfer_ids")
    if request.upload is not None:
        raise Refused("adopt_staged takes no upload")
    stages = _stages(request)
    entry = stages.get(token)
    if entry is None:
        raise Refused("no stage of this session has this token (unknown, or adopted already); nothing was adopted")
    if token != next(iter(stages)):
        raise Refused("stages are adopted in the order they were staged; nothing was adopted")
    if tuple(transfer_ids) != entry.transfer_ids:
        raise Refused("the stage was made of other transfer ids; nothing was adopted")
    context = CLIContext("httk", Path.cwd())
    try:
        # Still modifiable: a workspace sealed since the stage adopts nothing (the stage stays).
        root = _modifiable(_workspace_arguments(workspace_name), context, action="adopt jobs into it").root
    except _ERRORS as exc:
        raise Refused(f"{exc}; nothing was adopted") from None
    if Path(root).resolve() != entry.root:
        raise Refused("the stage was made in another workspace; nothing was adopted")
    from ._job import adopt_stage

    del stages[token]  # consumed: from here on, an error is fatal
    with _captured() as stderr:
        document, returncode = adopt_stage(entry.stage, progress=request.progress)
    return {"document": document, "returncode": returncode, "stderr": stderr.getvalue()}


def _abandon(state: dict[str, Any]) -> None:
    """Abandon the session's stages whose adoption never began (the end of its input, a ``bye``): each landing
    discarded, each owner closed. One that cannot be is left for recovery."""

    for entry in cast(dict[str, _Staged], state.pop("stages", {})).values():
        try:
            entry.stage.abandon()
        except Exception:  # its owner stays registered: recovery discards the landing once this process ended
            traceback.print_exc(file=sys.stderr)


def _install(request: Request) -> Mapping[str, Any]:
    """Install the store entries the upload carries and build: ``workflow install --json [--stdin-tar
    --source-label L --build] [--also-build ID ...] --workspace WS``
    (:func:`~httk.workflow.workflow_cli._describe.install_copies`).

    Header: ``workspace``, ``source_label``, ``build`` and ``also_build`` (installed ids to build); the upload, when
    there is one, is the tar of the entries (``<directory>/install.json`` and ``<directory>/package/``). The response
    carries the command's JSON ``document``, its ``returncode`` and its captured ``stderr``; a build's own output
    goes to this process's standard error, not the response. An error before the first install refuses the request
    (``ok: false``, its ``stderr`` kept); one after it is fatal.
    """

    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    assert workspace_name is not None
    source_label = _text(header, "source_label")
    build = _flag(header, "build")
    also_build = _texts(header, "also_build")
    upload = request.upload
    if upload is None and not also_build:
        raise Refused("install needs an upload or also_build ids")
    from ..workspace import Workspace
    from ._describe import install_copies

    context = CLIContext("httk", Path.cwd())
    begun = False

    def progress(phase: str, count: int | None) -> None:
        nonlocal begun
        begun = begun or phase in {"install", "build"}  # told once the command's owner is registered
        request.progress(phase, count)

    with _captured() as stderr:
        try:
            root = _local_root(_workspace_arguments(workspace_name), context, action="install workflows into it")
            document, returncode = install_copies(
                Workspace(root),
                stream=upload,
                source_label=source_label,
                build=build,
                also_build=also_build,
                finish=None if upload is None else upload.finish,
                progress=progress,
            )
        except _ERRORS as exc:
            if begun:
                raise  # an install may have begun, and the owner is left for recovery: fail-stop
            error = str(exc)
        else:
            return {"document": document, "returncode": returncode, "stderr": stderr.getvalue()}
    raise Refused(error, stderr=stderr.getvalue())


def _workflows(request: Request) -> Mapping[str, Any]:
    """Report the workflows *ids* as ``job adopt --built-for`` reports them: ``present``, ``unbuilt`` or
    ``missing``, each with its ``tree_sha256`` (read only).

    Header: ``workspace`` and ``ids``. The response carries ``workflows``, the rows in the order of *ids*; an
    unknown workspace refuses the request.
    """

    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    assert workspace_name is not None
    ids = _texts(header, "ids")
    if request.upload is not None:
        raise Refused("workflows takes no upload")
    from ..workspace import Workspace
    from ._job import _built_for

    context = CLIContext("httk", Path.cwd())
    with _captured() as stderr:
        try:
            root = _local_root(_workspace_arguments(workspace_name), context, action="report its workflows")
            rows = _built_for(Workspace(root), ids)
        except _ERRORS as exc:  # read only: nothing changed
            error = str(exc)
        else:
            return {"workflows": rows, "stderr": stderr.getvalue()}
    raise Refused(error, stderr=stderr.getvalue())


#: The encoded bytes a whole answer of ``hold_page`` or ``holds_page`` carries at most: a hold (or error) that does not
#: fit goes in the next answer; a first hold that does not fit the answer even with nothing else in it is named by id
#: only (``oversized``).
_ANSWER_BUDGET = 16 << 20
#: The most characters of a request's captured standard error such an answer carries: its end, marked.
_STDERR_TAIL = 64 << 10
#: The most bytes one error takes in such an answer, as encoded: its start, marked.
_ERROR_LIMIT = 4 << 10
#: The diagnostics of an answer that gave way to its items.
_GIVEN_WAY = "[diagnostics left out: they did not fit the answer]\n"

#: An item of an answer: a hold (``"hold"``, the hold) or an error (``"error"``, its text).
type _Item = tuple[str, Any]


def _measure(item: object) -> int:
    """The encoded size of one hold (or error, or any document) in an answer."""

    as_mapping = getattr(item, "as_mapping", None)
    return len(_frames.json_payload({"item": item if as_mapping is None else as_mapping()}))


def _doc_size(document: Mapping[str, Any]) -> int:
    """The encoded size of one hold's document as an item of an answer's ``holds`` list."""

    return len(_frames.json_payload(document))


def _tail(text: str) -> str:
    """The end of a request's captured standard error, at most :data:`_STDERR_TAIL` characters, marked when cut."""

    if len(text) <= _STDERR_TAIL:
        return text
    cut = len(text) - _STDERR_TAIL
    return f"[{cut} characters of diagnostics left out]\n{text[cut:]}"


def _cut(error: str, limit: int = _ERROR_LIMIT) -> str:
    """The start of one error whose JSON string, as an answer encodes it (``\\`` and non-ASCII escaped), takes at
    most *limit* bytes, marked when cut (the mark is reserved within *limit*; a *limit* smaller than the mark leaves
    the mark alone).

    :param error: The error.
    :param limit: The most encoded bytes, quotes included.
    :return: The error, or its marked start.
    """

    if len(json.dumps(error)) <= limit:
        return error
    room = limit - len(json.dumps(f" [{len(error)} characters left out]"))  # the widest mark
    size = 2  # the quotes
    end = 0
    for character in error:
        width = len(json.dumps(character)) - 2
        if size + width > room:
            break
        size += width
        end += 1
    return f"{error[:end]} [{len(error) - end} characters left out]"


def _encoded(answer: Mapping[str, Any]) -> int:
    """The encoded size of a whole answer, as the serve loop sends it (``ok`` included)."""

    return len(_frames.json_payload({"ok": True, **answer}))


def _fit(answer: dict[str, Any], items: list[_Item], optional: tuple[str, ...] = ("note", "stderr")) -> list[_Item]:
    """Put as many of *items*, in order, into ``answer["holds"]`` and ``answer["errors"]`` as the whole encoded answer
    (``ok`` and every other member included) carries within :data:`_ANSWER_BUDGET`: the items that did not fit, for
    the next answer.

    The first item always goes: a hold that does not fit the answer as it is sent with nothing else taken (its
    *optional* members given way: no note, the diagnostics replaced by a marker) goes by id only (``transfer_id``,
    ``member_count``, ``oversized``), and an error that does not fit is cut to what does. Every other hold goes as
    ``transfer status --json`` lists it, each error cut to :data:`_ERROR_LIMIT` characters. The optional members come
    back, the note first, as far as they fit beside the items taken; the whole answer is then measured again, and
    what still does not fit (but the first item) is left for the next answer too.

    :param answer: The answer's other members; ``holds`` (and ``errors``, with error items or an ``errors`` member)
        are set.
    :param items: The items to answer, in order.
    :param optional: The members that give way to the items.
    :return: The items left for the next answer.
    """

    with_errors = "errors" in answer or any(kind == "error" for kind, _ in items)
    saved = {key: answer[key] for key in optional if answer.get(key)}
    for key in saved:
        answer[key] = _GIVEN_WAY if key == "stderr" else None
    holds: list[Any] = []
    errors: list[str] = []
    answer["holds"] = holds
    if with_errors:
        answer["errors"] = errors
    total = _encoded(answer)
    taken: list[str] = []
    for kind, item in items:
        target: list[Any] = holds if kind == "hold" else errors
        comma = 1 if target else 0
        if kind == "hold":
            entry: Any = item.as_mapping()
            size = _doc_size(entry)
            if not taken and total + size > _ANSWER_BUDGET:
                entry = {"transfer_id": item.transfer_id, "member_count": len(item.members), "oversized": True}
                size = _doc_size(entry)
        else:
            entry = _cut(item)
            size = len(json.dumps(entry))
            if not taken and total + size > _ANSWER_BUDGET:
                entry = _cut(item, _ANSWER_BUDGET - total)  # the first error goes, cut to what fits (and its mark)
                size = len(json.dumps(entry))
        if taken and total + comma + size > _ANSWER_BUDGET:
            break
        target.append(entry)
        taken.append(kind)
        total += comma + size
    for key in ("note", "stderr"):
        if key in saved:
            answer[key] = saved[key]
            if _encoded(answer) > _ANSWER_BUDGET:
                answer[key] = _GIVEN_WAY if key == "stderr" else None
    while len(taken) > 1 and _encoded(answer) > _ANSWER_BUDGET:
        (holds if taken.pop() == "hold" else errors).pop()  # the whole answer, as the serve loop encodes it
    if taken == ["error"] and (over := _encoded(answer) - _ANSWER_BUDGET) > 0:
        # A sole error, cut once more to what the answer leaves it.
        errors[0] = _cut(items[0][1], len(json.dumps(errors[0])) - over)
    return items[len(taken) :]


def _limit(header: Mapping[str, Any], key: str, maximum: int) -> int:
    """The positive integer *key* of *header*, clamped to *maximum* (*maximum* when absent)."""

    value = header.get(key, maximum)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise Refused(f"{header.get('op')} needs {key} as a positive integer")
    return min(value, maximum)


def _select(request: Request) -> Mapping[str, Any]:
    """Select the jobs a pull holds page by page (plan §7.8, step 0): check the selection and resolve its ``--job``
    selectors, kept for the connection's ``hold_page`` requests
    (:class:`~httk.workflow.workflow_cli._transfer._Selection`); the listing of ``state``/``step`` is taken at the
    first ``hold_page``, after the client drove the holds an earlier run left.

    Header: ``workspace``, ``jobs`` (selectors), ``state``, ``step`` (lists), ``placement``, ``tree``,
    ``destination`` (the locator each hold records) and ``destination_id``, as ``job transfer --hold`` takes them.
    Nothing is held. A bad request or selector refuses it with nothing changed and clears an earlier selection;
    otherwise the selection replaces an earlier one. While holds of the current selection are made but not reported
    yet, it is refused with nothing changed. The response counts the ``candidates`` named by ``jobs``.
    Progress: ``list`` with the selectors resolved.
    """

    if any(kind == "hold" for kind, _ in request.session.get("carried", [])):
        raise Refused("select: holds of the current selection are not reported yet; ask hold_page for them first")
    for key in ("selection", "carried", "finished", "answered"):
        request.session.pop(key, None)
    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    destination = _text(header, "destination", required=True)
    assert workspace_name is not None and destination is not None
    jobs, states, steps = _texts(header, "jobs"), _texts(header, "state"), _texts(header, "step")
    placement = _text(header, "placement")
    tree = _flag(header, "tree")
    if request.upload is not None:
        raise Refused("select takes no upload")
    if not (jobs or states or steps):
        raise Refused("select needs jobs, state or step")
    from ..models import canonical_uuid, normalize_placement
    from ._common import _step_name
    from ._transfer import _MOVABLE_STATES, _Selection

    if unknown := sorted(set(states) - set(_MOVABLE_STATES)):
        raise Refused(f"select takes the states {', '.join(_MOVABLE_STATES)}, not {', '.join(unknown)}")
    try:
        steps = [_step_name(step) for step in steps]
        destination_id = header.get("destination_id")
        if destination_id is not None:
            destination_id = canonical_uuid(str(destination_id), "destination_id")
        if placement is not None:
            if not (states or (steps and not jobs)):
                raise ValueError("placement narrows state, or step without jobs; it needs one of them")
            if not normalize_placement(placement).parts:
                raise ValueError("placement needs a placement prefix")
    except (argparse.ArgumentTypeError, *_ERRORS) as exc:
        raise Refused(f"select: {exc}") from None
    arguments = argparse.Namespace(jobs=jobs, state=states, step=steps or None, placement=placement, tree=tree)
    context = CLIContext("httk", Path.cwd())
    with _captured() as stderr:
        try:
            workspace = _modifiable(_workspace_arguments(workspace_name), context, action="hold jobs in it")
            selection = _Selection(
                workspace,
                context.cwd,
                arguments,
                locator=destination,
                destination_id=destination_id,
                workflows=True,
                tell=request.progress,
            )
        except _ERRORS as exc:  # nothing held
            raise Refused(str(exc), stderr=_tail(stderr.getvalue())) from None
    request.session["selection"] = selection
    return {"candidates": selection.candidates, "stderr": _tail(stderr.getvalue())}


def _hold_page(request: Request) -> Mapping[str, Any]:
    """Hold the next page of the connection's selection (``select``) and answer it (plan §7.8).

    Header: ``limit_candidates`` (looked at), ``limit_holds`` (attempted) and ``limit_members`` (job keys), each
    clamped to this side's maxima; a page also closes at 256 errors and near :data:`_ANSWER_BUDGET`. The first
    request takes the selection's listing (step 2). The response carries ``holds`` (as ``transfer status --json``
    lists them, with their workflows) and ``errors`` (each cut to :data:`_ERROR_LIMIT` characters), the whole answer
    within :data:`_ANSWER_BUDGET` (see :func:`_fit`): what does not fit goes first in the next answer, which holds
    nothing new; ``changed`` (listed jobs left out as changed since the listing), ``remaining`` (selected jobs no page
    attempted yet, those that moved with an earlier hold not counted), ``consumed`` (the candidates, attempts and
    items this side finished with, cumulative: it grows with every answer that is not ``done``), ``carried`` (the
    jobs of the holds made but not reported yet), ``done``, ``note`` (with ``done``: the jobs left to move with their
    root, or ``null``) and the end of the captured ``stderr`` (jobs left out by ``--step``). Without a selection it
    is refused, nothing changed; after ``done`` (which frees the selection) it answers ``done`` with no holds.
    Progress: ``list`` with the candidates looked at, then ``hold`` with the attempts made. An error before the
    page's owner is registered refuses it with the selection unchanged; one from then on is fatal.
    """

    from ._transfer import _PAGE_ERRORS, _PAGE_HOLDS, _PAGE_MEMBERS, _PAGE_SCANNED

    header = request.header
    if request.upload is not None:
        raise Refused("hold_page takes no upload")
    limits = {
        "scanned": _limit(header, "limit_candidates", _PAGE_SCANNED),
        "holds": _limit(header, "limit_holds", _PAGE_HOLDS),
        "members": _limit(header, "limit_members", _PAGE_MEMBERS),
    }
    state = request.session
    if state.get("finished"):
        return {
            "holds": [],
            "errors": [],
            "changed": 0,
            "remaining": 0,
            "consumed": state.get("consumed", 0),
            "carried": 0,
            "done": True,
            "note": None,
            "stderr": "",
        }
    selection = state.get("selection")
    if selection is None:
        raise Refused("hold_page needs a selection: select first")
    carried: list[_Item] = state.get("carried", [])
    begun = False

    def progress(phase: str, count: int | None) -> None:
        nonlocal begun
        begun = begun or phase == "hold"  # told right before the page's owner is registered
        request.progress(phase, count)

    items: list[_Item] = []
    changed = 0
    with _captured() as stderr:
        try:
            if not carried:  # else what an earlier answer could not carry goes first, and nothing new is held
                selection.snapshot(tell=progress)
                page = selection.next_page(
                    errors=_PAGE_ERRORS, measure=_measure, budget=_ANSWER_BUDGET, tell=progress, **limits
                )
                items = [*(("error", error) for error in page.errors), *(("hold", hold) for hold in page.holds)]
                changed = page.changed
        except _ERRORS as exc:
            if begun:
                raise  # the owner may be registered and a hold made, left for recovery: fail-stop
            raise Refused(str(exc), stderr=_tail(stderr.getvalue())) from None
    exhausted = selection.done
    queue = [*carried, *items]
    note = selection.note() if exhausted else None
    answer: dict[str, Any] = {
        "errors": [],
        "changed": changed,
        "remaining": selection.remaining(),
        "consumed": selection.consumed + state.get("answered", 0) + len(queue),  # its widest, for the fit
        "carried": sum(len(item.members) for kind, item in queue if kind == "hold"),  # its widest, for the fit
        "done": False,
        "note": note,
        "stderr": _tail(stderr.getvalue()),
    }
    state["carried"] = rest = _fit(answer, queue)
    answered = state["answered"] = state.get("answered", 0) + len(queue) - len(rest)
    state["consumed"] = answer["consumed"] = selection.consumed + answered
    answer["carried"] = sum(len(item.members) for kind, item in rest if kind == "hold")
    if exhausted and not rest and answer["note"] == note:
        answer["done"] = True
        for key in ("selection", "carried", "answered"):
            state.pop(key, None)  # the snapshot is freed
        state["finished"] = True
    else:
        answer["note"] = None  # a note that gave way goes with the next answer
    return answer


def _holds_page(request: Request) -> Mapping[str, Any]:
    """Report a page of the workspace's holds (read only; plan §7.8): a stateless cursor over the live listing of
    ``transfers/outgoing/``.

    Header: ``workspace``, ``destination`` (the locator the holds record; ``null`` for every hold), ``after`` (the
    transfer id the page follows, ``null`` for the first) and ``limit_holds``, ``limit_members`` (clamped). The
    names after ``after`` are read in order, at most 4096 a request (a name bound elsewhere or gone since the
    listing is passed over), until the page has its holds, job keys or :data:`_ANSWER_BUDGET`. The response carries
    ``holds`` (as ``transfer status --json`` lists them, with their workflows; the whole answer within
    :data:`_ANSWER_BUDGET`, see :func:`_fit`), ``next``: the ``after`` of the next page (the name before the first
    hold not answered), ``null`` when the listing is exhausted, and the end of the captured ``stderr``. Progress:
    ``list`` with the holds read.
    """

    from .. import _moving
    from ..workspace import Workspace
    from ._transfer import _PAGE_HOLDS, _PAGE_MEMBERS, _PAGE_SCANNED

    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    destination = _text(header, "destination")
    after = header.get("after")
    assert workspace_name is not None
    if request.upload is not None:
        raise Refused("holds_page takes no upload")
    if after is not None and not (isinstance(after, str) and _TRANSFER_ID.fullmatch(after)):
        raise Refused("holds_page needs after as a transfer id or null")
    limit_holds = _limit(header, "limit_holds", _PAGE_HOLDS)
    limit_members = _limit(header, "limit_members", _PAGE_MEMBERS)
    context = CLIContext("httk", Path.cwd())
    #: The holds read, each with the index of its name.
    holds: list[tuple[int, Any]] = []
    following: str | None = None
    with _captured() as stderr:
        try:
            workspace = Workspace(_local_root(_workspace_arguments(workspace_name), context, action="list its holds"))
            names = [name for name in _moving.held_names(workspace) if after is None or name > after]
            members = size = 0
            request.progress("list", 0)
            for index, name in enumerate(names):
                if index >= _PAGE_SCANNED or len(holds) >= limit_holds or members >= limit_members:
                    following = names[index - 1]
                    break
                if size >= _ANSWER_BUDGET:
                    following = names[index - 1]
                    break
                hold = _moving.held_one(workspace, name, workflows=True)
                request.advance()
                if hold is None or destination not in (None, hold.destination_locator):
                    continue
                holds.append((index, hold))
                members += len(hold.members)
                size += _measure(hold)
        except _ERRORS as exc:  # read only: nothing changed
            raise Refused(str(exc), stderr=_tail(stderr.getvalue())) from None
    answer: dict[str, Any] = {"next": "0" * 32, "stderr": _tail(stderr.getvalue())}  # a cursor's size, for the fit
    rest = _fit(answer, [("hold", hold) for _, hold in holds], optional=("stderr",))
    if rest:
        # The next page starts at the first hold this answer did not carry; the first hold always goes.
        first = holds[len(holds) - len(rest)][0]
        assert first > 0 and len(rest) < len(holds), "an answer of holds_page carries its first hold"
        following = names[first - 1]
    answer["next"] = following
    return answer


#: A held bundle's name: its transfer id.
_TRANSFER_ID = re.compile(r"[0-9a-f]{32}")


class _SendFailed(Exception):
    """The download of ``send`` could not be written (the connection is gone): fatal, never a refusal."""


class _Changed(OSError):
    """A source file of ``send`` changed while it was archived (replaced, or grown past the size read)."""


class _Counting(io.RawIOBase, BinaryIO):
    """The writable side ``tarfile`` writes a download through: each write goes to *stream*.

    :param stream: The download.
    """

    def __init__(self, stream: _frames.StreamWriter) -> None:
        super().__init__()
        self._stream = stream

    def writable(self) -> bool:
        """Return true: the stream is writable."""

        return True

    def write(self, data: Buffer, /) -> int:
        """Send *data* as part of the download.

        :param data: The bytes.
        :return: How many bytes were taken (all of them).
        :raises _SendFailed: If the download cannot be written (fatal).
        """

        try:
            return self._stream.write(data)
        except (OSError, ValueError) as exc:
            raise _SendFailed(str(exc)) from exc


class _Reading(io.RawIOBase, BinaryIO):
    """A source file as ``tarfile`` copies it into the archive, telling *read* how many bytes each read took.

    :param source: The open source file.
    :param read: Told the bytes of each read.
    """

    def __init__(self, source: IO[bytes], read: Callable[[int], object]) -> None:
        super().__init__()
        self._source = source
        self._read = read

    def readable(self) -> bool:
        """Return true: the stream is readable."""

        return True

    def readinto(self, buffer: Buffer, /) -> int:
        """Read into *buffer* from the source.

        :param buffer: The writable buffer.
        :return: The number of bytes read.
        """

        view = memoryview(buffer).cast("B")
        data = self._source.read(len(view))
        view[: len(data)] = data
        self._read(len(data))
        return len(data)


class _Descriptor:
    """An open directory as :meth:`tarfile.TarFile.gettarinfo` takes a file object: a name and a descriptor.

    :param name: The path it was opened by.
    :param descriptor: The descriptor.
    """

    def __init__(self, name: str, descriptor: int) -> None:
        self.name = name
        self._descriptor = descriptor

    def fileno(self) -> int:
        """Return the descriptor.

        :return: The descriptor.
        """

        return self._descriptor


def _archive(
    archive: tarfile.TarFile,
    path: str,
    arcname: str,
    member: Callable[[tarfile.TarInfo], object],
    read: Callable[[int], object],
) -> None:
    """Add *path* (recursively) to *archive* as *arcname*, as :meth:`tarfile.TarFile.add` does, checking each regular
    file against its own descriptor.

    A regular file is opened (never through a symlink), and its descriptor's device, inode and size must be those of
    the ``lstat`` its entry came from, and the size its archive entry records that size (else it was replaced or
    changed size); after that size was copied, one more read must find its end (else it grew). A regular file or a
    directory takes one path ``lstat`` and one ``open``, as ``tarfile`` would; a symlink or a special file takes two
    path stats (its entry comes from the second), and a type other than the first one saw is a change. Same-size
    changes in place are not detected.

    :param archive: The archive.
    :param path: The source path.
    :param arcname: Its name in the archive.
    :param member: Told each member before it is written.
    :param read: Told the bytes of each read of a source file.
    :raises OSError: If a source changed (:class:`_Changed`) or vanished, or cannot be read.
    """

    found = os.lstat(path)
    if stat.S_ISREG(found.st_mode):
        with contextlib.ExitStack() as owned:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            owned.callback(os.close, descriptor)  # closed however this ends, also when it cannot be wrapped
            source = owned.enter_context(open(descriptor, "rb", closefd=False))
            opened = os.fstat(source.fileno())
            if (opened.st_dev, opened.st_ino, opened.st_size) != (found.st_dev, found.st_ino, found.st_size):
                raise _Changed(f"{arcname} was replaced or changed size while it was sent")
            info = archive.gettarinfo(arcname=arcname, fileobj=source)
            if info.isreg() and info.size != found.st_size:  # it changed size since the descriptor's check
                raise _Changed(f"{arcname} changed size while it was sent")
            member(info)
            archive.addfile(info, _Reading(source, read) if info.isreg() else None)
            if info.isreg() and source.read(1):
                raise _Changed(f"{arcname} grew while it was sent")
        return
    if stat.S_ISDIR(found.st_mode):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = archive.gettarinfo(arcname=arcname, fileobj=cast(IO[bytes], _Descriptor(path, descriptor)))
            names = sorted(os.listdir(descriptor))
        finally:
            os.close(descriptor)
        member(info)
        archive.addfile(info)
        for name in names:
            _archive(archive, os.path.join(path, name), f"{arcname}/{name}", member, read)
        return
    # A symlink or a special file, as tarfile adds it (a socket it skips): its entry comes from a second stat, which
    # must see the same type, or a regular file's header would go without its payload, a directory's without its tree.
    special = archive.gettarinfo(path, arcname)
    expected = _SPECIAL_TYPES.get(stat.S_IFMT(found.st_mode))
    if (special.type if special is not None else None) != expected:
        raise _Changed(f"{arcname} changed its type while it was sent")
    if special is not None:
        member(special)
        archive.addfile(special)


#: The archive entry type of each file type the special branch of :func:`_archive` adds (others: none, skipped).
_SPECIAL_TYPES = {
    stat.S_IFLNK: tarfile.SYMTYPE,
    stat.S_IFIFO: tarfile.FIFOTYPE,
    stat.S_IFCHR: tarfile.CHRTYPE,
    stat.S_IFBLK: tarfile.BLKTYPE,
}


def _send(request: Request) -> Mapping[str, Any]:
    """Download held bundles or store entries as one tar archive.

    Header: ``workspace``, ``kind`` (``holds`` or ``store``), ``names`` and, for ``holds``, optional ``limit_bytes``
    and ``limit_members``. For ``holds`` each name is a transfer id, and ``transfers/outgoing/<id>/`` goes as
    ``<id>/...``, in the given order, stopping after the bundle with which the regular-file bytes the receiver lands
    (a hard link counted with its target's size) reach ``limit_bytes``, or the archive's members (directories and
    hard links included) reach ``limit_members``; a bundle is never split. For ``store`` each name is one plain store
    directory name, and ``workflows/<name>/install.json`` and ``workflows/<name>/package/`` go as ``<name>/...``.
    The response lists the names carried, in order (``sent``), and the archive's ``bytes``.

    A name that does not exist (another actor released the hold) refuses the request before any data, naming it
    (``missing``). A bundle that cannot be read whole while it is sent (a file replaced, grown, shrunk or vanished)
    aborts the download, so that no partial set ever ends successfully, and refuses the request naming it
    (``bundle``); a file changed in place without changing its size is not detected. A download that cannot be
    written is fatal. Progress: ``send`` with the source bytes read and members written. Read only.
    """

    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    kind = header.get("kind")
    names = _texts(header, "names")
    limits = {key: header.get(key) for key in ("limit_bytes", "limit_members")}
    assert workspace_name is not None
    if request.upload is not None:
        raise Refused("send takes no upload")
    if kind not in ("holds", "store"):
        raise Refused(f"send takes kind holds or store, not {kind!r}")
    if not names or len(set(names)) != len(names):
        raise Refused("send needs names, each once")
    for key, limit in limits.items():
        if limit is not None and (
            kind != "holds" or isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
        ):
            raise Refused(f"send takes a positive integer {key}, for holds only")
    if kind == "holds":
        malformed = [name for name in names if not _TRANSFER_ID.fullmatch(name)]
    else:
        malformed = [name for name in names if name in (".", "..") or "/" in name or "\0" in name]
    if malformed:
        raise Refused(f"send: not a {'transfer id' if kind == 'holds' else 'store name'}: {malformed[0]!r}")
    from .. import _moving
    from .._store import _workflows
    from ..workspace import Workspace

    context = CLIContext("httk", Path.cwd())
    with _captured() as stderr:
        try:
            workspace = Workspace(_local_root(_workspace_arguments(workspace_name), context, action="send from it"))
        except _ERRORS as exc:
            raise Refused(str(exc), stderr=stderr.getvalue()) from None
        base = _moving._outgoing(workspace) if kind == "holds" else _workflows(workspace)
        missing = []
        for name in names:
            try:
                found = os.lstat(base / name)
            except OSError:
                found = None
            if found is None or not stat.S_ISDIR(found.st_mode):
                missing.append(name)
        if missing:
            what = "no such hold" if kind == "holds" else "no such store entry"
            raise Refused(
                f"{what} in {workspace.root}: {', '.join(missing)}", missing=missing, stderr=stderr.getvalue()
            )
        sent = _send_archive(request, base, kind, names, limits)
    return {**sent, "stderr": stderr.getvalue()}


def _send_archive(
    request: Request, base: Path, kind: str, names: list[str], limits: Mapping[str, int | None]
) -> dict[str, Any]:
    """Write the download of :func:`_send`: the ``sent`` names and the archive's ``bytes``.

    :param request: The request.
    :param base: The directory holding the names.
    :param kind: ``holds`` or ``store``.
    :param names: The names, in order.
    :param limits: ``limit_bytes`` and ``limit_members``, each ``None`` for none.
    :return: ``{"sent", "bytes"}``.
    :raises Refused: Naming the bundle that could not be sent whole (the download aborted).
    :raises _SendFailed: If the download cannot be written.
    """

    sent: list[str] = []
    sizes: dict[str, int] = {}
    landed = members = progressed = 0

    def member(info: tarfile.TarInfo) -> None:
        nonlocal landed, members, progressed
        if info.isreg():
            sizes[info.name] = info.size
            landed += info.size
        elif info.islnk():
            landed += sizes.get(info.linkname, 0)  # landed as a copy
        members += 1
        progressed += 1
        request.progress("send", progressed)

    def read(count: int) -> None:
        nonlocal progressed
        progressed += count
        request.progress("send", progressed)

    request.progress("send", progressed)
    stream = request.download()
    current = names[0]
    try:
        with tarfile.open(fileobj=_Counting(stream), mode="w|", bufsize=_frames.DATA_LIMIT) as archive:
            for current in names:
                if kind == "holds":
                    _archive(archive, str(base / current), current, member, read)
                else:
                    for part in ("install.json", "package"):
                        _archive(archive, str(base / current / part), f"{current}/{part}", member, read)
                sent.append(current)
                if any(
                    limit is not None and reached >= limit
                    for limit, reached in ((limits["limit_bytes"], landed), (limits["limit_members"], members))
                ):
                    break
    except (OSError, tarfile.TarError) as exc:
        # A source could not be read whole: the download is aborted, and what was sent of it is never used.
        try:
            stream.close(ok=False, abort=f"{current} could not be sent whole")
        except (OSError, ValueError) as broken:
            # The abort may not have arrived: the session cannot go on.
            raise _SendFailed(f"the download's abort could not be written: {broken}") from broken
        raise Refused(f"{current} could not be sent whole (it changed or vanished): {exc}", bundle=current) from None
    except BaseException:
        with contextlib.suppress(Exception):
            stream.close(ok=False, abort="the send failed")
        raise
    try:
        stream.close()  # the last data and the successful terminal
    except (OSError, ValueError) as exc:
        raise _SendFailed(f"the download could not be written: {exc}") from exc
    return {"sent": sent, "bytes": stream.bytes}


def _release(request: Request) -> Mapping[str, Any]:
    """Release holds: ``job transfer --release T ... [--keep [--destination-id ID]] WS``
    (:func:`~httk.workflow.workflow_cli._transfer.release_holds`).

    Header: ``workspace``, ``transfer_ids``, ``keep`` and ``destination_id`` (the adopting workspace's id away copies
    record when a hold records none; with ``keep`` only). The response lists the ids ``released`` and those
    ``missing`` (gone already), in order. Progress: ``release`` with the holds done. An error before the command's
    owner is registered refuses the request; one from then on is fatal.
    """

    header = request.header
    workspace_name = _text(header, "workspace", required=True)
    assert workspace_name is not None
    transfer_ids = _texts(header, "transfer_ids")
    keep = _flag(header, "keep")
    destination_id = header.get("destination_id")
    if request.upload is not None:
        raise Refused("release takes no upload")
    if not transfer_ids or not all(_TRANSFER_ID.fullmatch(transfer_id) for transfer_id in transfer_ids):
        raise Refused("release needs transfer_ids, each a transfer id")
    from ..models import canonical_uuid
    from ..workspace import Workspace
    from ._transfer import release_holds

    context = CLIContext("httk", Path.cwd())
    begun = False

    def progress(phase: str, count: int | None) -> None:
        nonlocal begun
        begun = True  # told right before the command's owner is registered
        request.progress(phase, count)

    with _captured() as stderr:
        try:
            if destination_id is not None:
                if not keep:
                    raise ValueError("release: destination_id names the workspace away copies record; it needs keep")
                destination_id = canonical_uuid(str(destination_id), "destination_id")
            workspace = Workspace(_local_root(_workspace_arguments(workspace_name), context, action="release in it"))
            done = release_holds(workspace, transfer_ids, keep=keep, destination_id=destination_id, progress=progress)
        except _ERRORS as exc:
            if begun:
                raise  # the owner may be registered and a hold released, left for recovery: fail-stop
            raise Refused(str(exc), stderr=stderr.getvalue()) from None
    return {
        "released": [transfer_id for transfer_id, ok in zip(transfer_ids, done, strict=True) if ok],
        "missing": [transfer_id for transfer_id, ok in zip(transfer_ids, done, strict=True) if not ok],
        "stderr": stderr.getvalue(),
    }


#: The ops of format version 1 this far side serves, by name.
OPS: dict[str, Op] = {
    "echo": _echo,
    "bye": _bye,
    "adopt": _adopt,
    "stage": _stage,
    "adopt_staged": _adopt_staged,
    "install": _install,
    "workflows": _workflows,
    "select": _select,
    "hold_page": _hold_page,
    "holds_page": _holds_page,
    "send": _send,
    "release": _release,
}


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
    state: dict[str, Any] = {}  # the connection's: a pull's selection, a push's stages
    last_id = 0
    fatal = False
    try:
        while (frame := reader.read_frame()) is not None:
            if frame.kind != REQUEST:
                raise TransferProtocolError(f"a {frame.kind} frame outside a request")
            if frame.id <= last_id:
                raise TransferProtocolError(f"request id {frame.id} does not follow {last_id}")
            last_id = frame.id
            assert frame.document is not None
            upload = StreamReader(reader.read_frame, frame.id) if frame.document.get("upload") is True else None
            request = Request(frame.id, frame.document, upload, frames, state)
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
                    fatal = True
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
    finally:
        if not fatal:
            _abandon(state)  # stages whose adoption never began; after a fatal op, recovery's
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
