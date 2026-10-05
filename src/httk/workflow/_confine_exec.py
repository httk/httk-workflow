"""Inner exec of a confined launch: the first program inside each rank sandbox.

Run by the rank helper as ``python -I -m httk.workflow._confine_exec --request PATH --request-id ID
--attempt-id A``. It reads the launch request inside the sandbox, where the job directory is the only
writable path, changes to the requested working directory without following a symlink below the job
directory, adds the request's environment to the rank environment Bubblewrap set, and replaces itself with
the requested command. A validation failure exits with status 2, a failed exec with 127.
"""

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from ._launch_protocol import (
    MAX_REQUEST_BYTES,
    LaunchRequest,
    check_attempt_id,
    check_request_id,
    decode_request,
    is_reserved_environment,
    read_bounded,
    request_relative_path,
)

_ANCHOR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
_DIRECTORY_FLAGS = _ANCHOR_FLAGS | os.O_NOFOLLOW


def _walk(anchor: int, parts: Sequence[str]) -> int:
    """Return a new descriptor for *parts* below *anchor*, opening every component without following it."""

    descriptor = os.dup(anchor)
    try:
        for part in parts:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _read_request(request_path: Path, request_id: str, attempt_id: str) -> tuple[int, LaunchRequest]:
    """Return a descriptor of the job directory and the validated request."""

    relative = PurePosixPath(request_relative_path(attempt_id, request_id))
    parts = request_path.parts
    if (
        not request_path.is_absolute()
        or ".." in parts
        or len(parts) <= len(relative.parts) + 1
        or parts[-len(relative.parts) :] != relative.parts
    ):
        raise ValueError(f"--request must be an absolute path <job directory>/{relative}")
    job_path = Path(*parts[: -len(relative.parts)])
    job_fd = os.open(job_path, _ANCHOR_FLAGS)
    try:
        launch_fd = _walk(job_fd, relative.parts[:-1])
        try:
            request = decode_request(read_bounded(launch_fd, relative.name, MAX_REQUEST_BYTES))
        finally:
            os.close(launch_fd)
        if request.request_id != request_id or request.attempt_id != attempt_id:
            raise ValueError("the launch request names another request or attempt")
    except BaseException:
        os.close(job_fd)
        raise
    return job_fd, request


def _run(request_path: Path, request_id: str, attempt_id: str) -> int:
    job_fd, request = _read_request(request_path, check_request_id(request_id), check_attempt_id(attempt_id))
    try:
        cwd_fd = _walk(job_fd, () if request.cwd == "." else PurePosixPath(request.cwd).parts)
    finally:
        os.close(job_fd)
    try:
        os.fchdir(cwd_fd)
    finally:
        os.close(cwd_fd)
    environment = dict(os.environ)
    for name, value in request.environment:
        if is_reserved_environment(name):
            raise ValueError(f"the launch request sets the reserved variable {name}")
        environment[name] = value
    try:
        os.execvpe(request.argv[0], list(request.argv), environment)
    except OSError as exc:
        print(f"httk-workflow launch: cannot execute {request.argv[0]!r}: {exc}", file=sys.stderr, flush=True)
        return 127
    return 127  # pragma: no cover - execvpe does not return


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the launch request and replace this process with its command.

    :param argv: The inner exec arguments, or the process arguments when omitted.
    :return: ``2`` for a refused request and ``127`` for a failed exec; on success the process is replaced.
    """

    parser = argparse.ArgumentParser(prog="python -I -m httk.workflow._confine_exec", description=__doc__)
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    arguments = parser.parse_args(argv)
    try:
        return _run(arguments.request, arguments.request_id, arguments.attempt_id)
    except (OSError, ValueError) as exc:
        print(f"httk-workflow launch: refused: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
