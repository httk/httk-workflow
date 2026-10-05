"""Launch client: the ``HTTK_WORKFLOW_LAUNCH`` prefix of a confined attempt.

Run as ``python -m httk.workflow._launch_client APP ARG...`` inside the attempt sandbox. It asks the
manager to launch ``APP ARG...`` on the ranks of the attempt's binding, reproduces the launch's standard
output and error, and exits with the launch's exit status:

1. create ``launch/ID.lock`` exclusively and hold ``flock(LOCK_EX)`` on it for the client's whole life,
   so the manager can tell a live client from a dead one;
2. publish ``launch/ID.request.json`` by renaming a temporary file into place;
3. copy ``launch/ID.stdout`` and ``launch/ID.stderr`` (created by the manager) to its own standard output
   and error until ``launch/ID.status.json`` exists and both are drained.

SIGTERM, SIGINT and SIGHUP do not end the client: it creates ``launch/ID.stop`` and keeps waiting for the
manager's ``stopped`` status, so its caller returns only after the ranks are reaped. Only SIGKILL ends it
early; the released lock then makes the manager stop the launch.
"""

import errno
import fcntl
import json
import os
import signal
import stat
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from types import FrameType
from typing import Any

from ._launch_protocol import (
    LAUNCH_DIRECTORY,
    MAX_STATUS_BYTES,
    LaunchRequest,
    LaunchStatus,
    check_attempt_id,
    decode_status,
    encode_request,
    is_environment_name,
    is_reserved_environment,
    lock_name,
    new_request_id,
    read_bounded,
    request_name,
    request_temporary_name,
    status_name,
    stderr_name,
    stdout_name,
    stop_name,
)

_POLL_SECONDS = 0.1
_COPY_CHUNK = 64 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_STOP_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_STREAM_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_UNSUPPORTED_LOCK_ERRNOS = frozenset({errno.ENOSYS, errno.ENOLCK, errno.EOPNOTSUPP})
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
_STOPPED_DEFAULT = 128 + signal.SIGTERM


class _Refusal(Exception):
    """The launch cannot be requested; the message is shown to the caller."""


def _attempt_context(environ: Mapping[str, str]) -> tuple[str, str, str]:
    """Return the real control directory, the real job directory and the attempt identifier."""

    values = {name: environ.get(name) for name in ("HTTK_WORKFLOW_CONTROL_DIR", "HTTK_WORKFLOW_JOB_DIR")}
    context_text = environ.get("HTTK_WORKFLOW_CONTEXT")
    missing = [name for name, value in values.items() if not value] + (
        [] if context_text else ["HTTK_WORKFLOW_CONTEXT"]
    )
    if missing:
        raise _Refusal(f"a confined launch runs only inside a workflow attempt; {', '.join(missing)} is not set")
    assert context_text is not None
    try:
        context = json.loads(context_text)
        attempt_id = check_attempt_id(context["attempt_id"] if isinstance(context, dict) else None)
    except (KeyError, ValueError) as exc:
        raise _Refusal("HTTK_WORKFLOW_CONTEXT does not name a valid attempt_id") from exc
    control = os.path.realpath(str(values["HTTK_WORKFLOW_CONTROL_DIR"]))
    job = os.path.realpath(str(values["HTTK_WORKFLOW_JOB_DIR"]))
    if control != os.path.join(job, "attempts", attempt_id):
        raise _Refusal("HTTK_WORKFLOW_CONTROL_DIR is not the attempt's control directory in HTTK_WORKFLOW_JOB_DIR")
    return control, job, attempt_id


def _relative_cwd(job: str) -> str:
    """Return the current directory relative to the real job directory, ``.`` for the job directory."""

    current = os.path.realpath(os.getcwd())
    if current == job:
        return "."
    if os.path.commonpath([current, job]) != job:
        raise _Refusal(f"the working directory {current} is not inside the job directory {job}")
    return os.path.relpath(current, job)


def _request_environment(environ: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
    """Return the environment carried by a request: reserved and unrepresentable names dropped."""

    return tuple(
        sorted(
            (name, value)
            for name, value in environ.items()
            if is_environment_name(name) and not is_reserved_environment(name)
        )
    )


def _build_request(argv: Sequence[str], environ: Mapping[str, str]) -> tuple[str, LaunchRequest]:
    if not argv:
        raise _Refusal("usage: python -m httk.workflow._launch_client APP [ARG...]; no command given")
    control, job, attempt_id = _attempt_context(environ)
    try:
        request = LaunchRequest(
            request_id=new_request_id(),
            attempt_id=attempt_id,
            argv=tuple(argv),
            cwd=_relative_cwd(job),
            environment=_request_environment(environ),
        )
        encode_request(request)
    except ValueError as exc:
        raise _Refusal(f"the launch request is outside the protocol bounds: {exc}") from exc
    return control, request


def _open_launch_directory(control: str) -> int:
    try:
        os.mkdir(os.path.join(control, LAUNCH_DIRECTORY), 0o700)
    except FileExistsError:
        pass
    control_fd = os.open(control, _DIRECTORY_FLAGS)
    try:
        return os.open(LAUNCH_DIRECTORY, _DIRECTORY_FLAGS, dir_fd=control_fd)
    finally:
        os.close(control_fd)


def _unlink(directory_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass


def _lock(directory_fd: int, request_id: str) -> int:
    """Create and exclusively lock ``ID.lock``; the descriptor holds the lock until the client exits."""

    name = lock_name(request_id)
    descriptor = os.open(name, _CREATE_FLAGS, 0o600, dir_fd=directory_fd)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
    except OSError as exc:
        os.close(descriptor)
        _unlink(directory_fd, name)
        if exc.errno in _UNSUPPORTED_LOCK_ERRNOS:
            raise _Refusal("the filesystem does not support flock, which confined launches need") from exc
        raise
    return descriptor


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(descriptor, view) :]


def _publish(directory_fd: int, request: LaunchRequest) -> None:
    temporary = request_temporary_name(request.request_id)
    descriptor = os.open(temporary, _CREATE_FLAGS, 0o600, dir_fd=directory_fd)
    try:
        _write_all(descriptor, encode_request(request))
    except BaseException:
        os.close(descriptor)
        _unlink(directory_fd, temporary)
        raise
    os.close(descriptor)
    os.rename(temporary, request_name(request.request_id), src_dir_fd=directory_fd, dst_dir_fd=directory_fd)


class _Stream:
    """One manager-written output file copied to one of the client's own descriptors."""

    def __init__(self, name: str, target: int) -> None:
        self.name = name
        self.target = target
        self.descriptor: int | None = None
        self.done = False

    def pump(self, directory_fd: int) -> None:
        """Open the file once it exists and copy every byte available now to the target."""

        if self.done:
            return
        if self.descriptor is None:
            try:
                descriptor = os.open(self.name, _STREAM_FLAGS, dir_fd=directory_fd)
            except FileNotFoundError:
                return
            except OSError:
                self.done = True
                return
            if not _is_regular(descriptor):
                os.close(descriptor)
                self.done = True
                return
            self.descriptor = descriptor
        while chunk := os.read(self.descriptor, _COPY_CHUNK):
            try:
                _write_all(self.target, chunk)
            except OSError:
                # The caller closed this stream; keep draining so the other stream and the status still arrive.
                self.target = -1
                continue

    def close(self) -> None:
        """Close the file descriptor."""

        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None


def _is_regular(descriptor: int) -> bool:
    return stat.S_ISREG(os.fstat(descriptor).st_mode)


def _exit_code(status: LaunchStatus) -> int:
    if status.state == "exited":
        assert status.exit_code is not None
        return status.exit_code
    if status.state == "stopped":
        return _STOPPED_DEFAULT if status.exit_code is None else status.exit_code
    return 2


def _read_status(directory_fd: int, request_id: str) -> LaunchStatus | None:
    try:
        data = read_bounded(directory_fd, status_name(request_id), MAX_STATUS_BYTES)
    except FileNotFoundError:
        return None
    status = decode_status(data)
    if status.request_id != request_id:
        raise ValueError("the launch status names another request")
    return status


def _wait(directory_fd: int, request_id: str, stop_requested: list[int]) -> int:
    streams = (_Stream(stdout_name(request_id), 1), _Stream(stderr_name(request_id), 2))
    stop_created = False
    try:
        while True:
            if stop_requested and not stop_created:
                try:
                    os.close(os.open(stop_name(request_id), _STOP_FLAGS, 0o600, dir_fd=directory_fd))
                    stop_created = True
                except OSError:
                    pass
            try:
                status = _read_status(directory_fd, request_id)
            except (OSError, ValueError) as exc:
                for stream in streams:
                    stream.pump(directory_fd)
                print(f"httk-workflow launch: unreadable launch status: {exc}", file=sys.stderr, flush=True)
                return 2
            for stream in streams:
                stream.pump(directory_fd)
            if status is not None:
                if status.error is not None:
                    print(f"httk-workflow launch: {status.state}: {status.error}", file=sys.stderr, flush=True)
                return _exit_code(status)
            time.sleep(_POLL_SECONDS)
    finally:
        for stream in streams:
            stream.close()


def _request_stop(stop_requested: list[int], signum: int, _frame: FrameType | None) -> None:
    stop_requested.append(signum)


def main(argv: Sequence[str] | None = None) -> int:
    """Request one confined launch, reproduce its output and return its exit status.

    :param argv: The command to launch, or the process arguments when omitted.
    :return: The launch's exit status; ``2`` when the request is refused or cannot be made, and ``143``
        for a stopped launch without an exit status.
    """

    command = list(sys.argv[1:] if argv is None else argv)
    try:
        control, request = _build_request(command, os.environ)
        directory_fd = _open_launch_directory(control)
    except _Refusal as exc:
        print(f"httk-workflow launch: {exc}", file=sys.stderr, flush=True)
        return 2
    except OSError as exc:
        print(f"httk-workflow launch: cannot prepare the launch directory: {exc}", file=sys.stderr, flush=True)
        return 2
    lock_fd: int | None = None
    previous: dict[signal.Signals, Callable[[int, FrameType | None], Any] | int | None] = {}
    try:
        try:
            lock_fd = _lock(directory_fd, request.request_id)
        except _Refusal as exc:
            print(f"httk-workflow launch: {exc}", file=sys.stderr, flush=True)
            return 2
        stop_requested: list[int] = []
        for signum in _STOP_SIGNALS:
            previous[signum] = signal.signal(
                signum, lambda received, frame: _request_stop(stop_requested, received, frame)
            )
        _publish(directory_fd, request)
        return _wait(directory_fd, request.request_id, stop_requested)
    except OSError as exc:
        print(f"httk-workflow launch: cannot request the launch: {exc}", file=sys.stderr, flush=True)
        return 2
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        if lock_fd is not None:
            os.close(lock_fd)
        os.close(directory_fd)


if __name__ == "__main__":
    raise SystemExit(main())
