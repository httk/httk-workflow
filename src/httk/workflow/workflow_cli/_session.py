"""The hidden far-side :command:`workflow session` command.

One long-lived process answers a sequence of pinned ``httk`` command vectors
sent as newline-delimited JSON over stdin, so a client reaches a remote
workspace through one connection instead of one login per read. Each response
has the ``returncode``/``stdout``/``stderr`` shape of the adapter ``invoke``
operation.
"""

import argparse
import contextlib
import io
import json
import os
import shlex
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TextIO

from httk.core.cli import CLIContext

from ..adapters import (
    REMOTE_JOB_LIST_COMMAND,
    REMOTE_JOB_LOG_COMMAND,
    REMOTE_JOB_PUBLISH_REQUESTS_COMMAND,
    REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
    REMOTE_JOB_SHOW_COMMAND,
    REMOTE_JOB_WHY_COMMAND,
    REMOTE_MANAGER_COMMAND,
    REMOTE_STATUS_COMMAND,
    SESSION_FORMAT,
    SESSION_FORMAT_VERSION,
)
from ._common import _leaf

__all__ = [
    "SESSION_COMMANDS",
    "build_session_parser",
    "handle_session",
    "serve",
]

#: The pinned remote vectors a session request's ``argv`` may start with; anything else is refused.
SESSION_COMMANDS = (
    REMOTE_JOB_LIST_COMMAND,
    REMOTE_JOB_SHOW_COMMAND,
    REMOTE_JOB_LOG_COMMAND,
    REMOTE_JOB_WHY_COMMAND,
    REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
    REMOTE_JOB_PUBLISH_REQUESTS_COMMAND,
    REMOTE_MANAGER_COMMAND,
    REMOTE_STATUS_COMMAND,
)


def _refused(request_id: object, message: str) -> dict[str, Any]:
    return {"id": request_id, "returncode": 2, "stdout": "", "stderr": f"{message}\n"}


def _handle(request: Mapping[str, Any]) -> dict[str, Any]:
    """Run one decoded request in-process and return its response document."""

    request_id = request.get("id")
    if request_id is not None and (isinstance(request_id, bool) or not isinstance(request_id, int | str)):
        return _refused(None, "session request id must be an integer or a string")
    argv = request.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
        return _refused(request_id, "session request argv must be a nonempty list of nonempty strings")
    cwd = request.get("cwd")
    if cwd is not None and not isinstance(cwd, str):
        return _refused(request_id, "session request cwd must be a string")
    if not any(tuple(argv[: len(pinned)]) == pinned for pinned in SESSION_COMMANDS):
        return _refused(request_id, f"session refuses {shlex.join(argv)}: not a pinned remote command")

    # The real ``httk`` entry point routes these registered names to these handlers (``httk.registry.cli``).
    from . import job_command, workflow_command, workspace_command

    route = {"job": job_command, "workspace": workspace_command, "workflow": workflow_command}[argv[1]]
    context = CLIContext("httk", Path(cwd) if cwd is not None else Path.cwd())
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            returncode = route(argv[2:], context)
        except SystemExit as exc:
            returncode = exc.code if isinstance(exc.code, int) else 1
        except Exception as exc:
            print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
            returncode = 2
    return {"id": request_id, "returncode": returncode, "stdout": stdout.getvalue(), "stderr": stderr.getvalue()}


def serve(requests: TextIO, responses: TextIO) -> int:
    """Answer newline-delimited JSON requests until end of input.

    Writes the hello line, then exactly one response line per nonblank request
    line, flushed before the next request is read. Malformed requests are
    answered with ``returncode`` 2 and do not end the session.

    :param requests: The protocol input stream.
    :param responses: The protocol output stream.
    :return: The exit status, 0 at end of input.
    """

    def send(document: Mapping[str, Any]) -> None:
        responses.write(json.dumps(document, separators=(",", ":")) + "\n")
        responses.flush()

    send({"format": SESSION_FORMAT, "format_version": SESSION_FORMAT_VERSION})
    for line in requests:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
        except ValueError as exc:
            send(_refused(None, f"undecodable session request: {exc}"))
            continue
        send(_handle(request) if isinstance(request, dict) else _refused(None, "session request must be an object"))
    return 0


def _isolate_streams() -> tuple[TextIO, TextIO]:
    """Take fds 0 and 1 private as the protocol pipe and point the standard streams elsewhere.

    The pipe is the protocol: a stray ``print`` outside a request's capture, or a
    child a handler spawns inheriting fd 1, would otherwise forge or corrupt a
    response line. Afterwards fd 0 and ``sys.stdin`` read ``/dev/null``, and fd 1
    and ``sys.stdout`` write to stderr, which the transport relays harmlessly.

    :return: The protocol input and output streams.
    """

    requests = os.fdopen(os.dup(0), "r", encoding="utf-8", newline="\n")
    responses = os.fdopen(os.dup(1), "w", encoding="utf-8", newline="\n")
    null = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null, 0)
    os.close(null)
    os.dup2(2, 1)
    sys.stdin = os.fdopen(0, "r", encoding="utf-8", closefd=False)
    sys.stdout = sys.stderr
    return requests, responses


def handle_session(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Serve pinned remote commands over this process's stdin and stdout until end of input."""

    return serve(*_isolate_streams())


def build_session_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the hidden session command."""

    _leaf(
        subparsers,
        "session",
        summary="serve pinned remote commands over stdin and stdout",
        description="Serve pinned remote httk commands as newline-delimited JSON over stdin and stdout",
        handler=handle_session,
        hidden=True,
    )
