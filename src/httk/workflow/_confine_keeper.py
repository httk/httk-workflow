"""Node namespace keeper of a confined launch: one set of rank namespaces per launch and node.

The ranks of one launch on one node share user, PID, IPC, UTS and mount namespaces. Their runtimes then see
distinct process IDs (Intel and LLVM OpenMP register themselves in ``/dev/shm`` under their process ID), and
they can attach each other's System V shared memory (as VASP's ``getshmem.c`` does). Other launches and
jobs never share them.

The first rank helper of a launch on a node leads a keeper *generation*: it starts a small supervisor, which
starts the keeper, a Bubblewrap sandbox with the rank mounts that runs an idle program, and records it. Every
rank helper, the leader included, then runs its rank in the keeper's namespaces through :mod:`httk.workflow._confine_enter`
(:meth:`Membership.command`). Everything in the keeper's namespaces ends with it: the kernel kills every
process left in its PID namespace and frees the System V objects of its IPC namespace.

Helpers coordinate in the node directory ``<confine.shm_root>/httk-<token>/ns/`` of the launch, which no
rank sees, with renames and a FIFO but no file locks:

* ``<g>/`` is generation ``g``. The leader publishes it by renaming a prepared directory onto it, which
  fails once ``<g>`` exists, so exactly one helper leads each generation.
* ``<g>/members`` is a FIFO whose read end only the supervisor holds. Every member holds a write end until its
  rank is gone. A write end that cannot be opened (``ENXIO``) means the supervisor is gone, and the read end
  reports a hang-up once every member left.
* ``<g>/keeper.json`` names the keeper process and its namespaces; ``<g>/failed`` says why the keeper
  could not start and refuses every rank of the generation.
* ``<g>/closing/`` marks a generation whose members all left. A helper opens its write end before it looks
  for the marker, and the supervisor looks for write ends again after creating it, so a helper either stays a
  member the supervisor waits for, or moves on to generation ``g + 1``. A later generation only starts once no
  rank of the earlier one runs, so ranks that run at the same time always share their namespaces.

The supervisor waits until every member left before it kills the keeper's Bubblewrap and exits. Its PID 1 then
dies by ``--die-with-parent``, and every rank dies with its own helper.
"""

import argparse
import errno
import fcntl
import json
import os
import secrets
import select
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from . import _death
from ._launch_protocol import read_bounded
from ._sandbox import PreparedSandbox

#: The stdlib-only program that enters the keeper's namespaces and runs a rank, run by path.
ENTER_PROGRAM = Path(__file__).with_name("_confine_enter.py")
#: The keeper's idle program: report readiness, then wait for its monitor to end it.
KEEPER_PROGRAM = "import os, signal\nos.write(1, b'r')\nos.close(1)\nwhile True: signal.pause()\n"
#: How long a helper waits for its generation's keeper, in seconds.
KEEPER_WAIT_SECONDS = 120.0
#: The most generations one launch may start on one node.
MAX_GENERATIONS = 1024
MEMBERS = "members"
KEEPER_FILE = "keeper.json"
FAILED_FILE = "failed"
CLOSING = "closing"
# The namespaces a keeper record names: the keeper's user namespace and the four its owning user namespace owns.
_OWNED = ("mnt", "pid", "ipc", "uts")
_RECORDED = ("user", *_OWNED)
_NS_GET_USERNS = 0xB701  # _IO(0xb7, 0x1) from <linux/nsfs.h>
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
_MAX_RECORD_BYTES = 4096
_POLL_MILLISECONDS = 20
_INFO_WAIT_SECONDS = 10.0
_SANDBOX_ENVIRONMENT = {"HOME": "/tmp/home", "TMPDIR": "/tmp"}


def _close(descriptor: int | None) -> None:
    if descriptor is None:
        return
    try:
        os.close(descriptor)
    except OSError:
        pass


def _exists(name: str, directory_fd: int) -> bool:
    try:
        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _publish(directory_fd: int, name: str, data: bytes) -> None:
    """Write a record next to its final name, then rename it into place."""

    temporary = f".{name}.{secrets.token_hex(8)}"
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd
    )
    try:
        os.write(descriptor, data)
    finally:
        os.close(descriptor)
    os.rename(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)


def _read_until_eof(descriptor: int, deadline: float, what: str) -> bytes:
    data = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError(f"timed out waiting for {what}")
        ready, _writable, _failed = select.select([descriptor], [], [], remaining)
        if not ready:
            continue
        chunk = os.read(descriptor, _MAX_RECORD_BYTES)
        if not chunk:
            return bytes(data)
        data.extend(chunk)
        if len(data) > _MAX_RECORD_BYTES:
            raise ValueError(f"{what} is too large")


@dataclass(frozen=True, slots=True)
class KeeperRecord:
    """The published description of a keeper, ``<g>/keeper.json``.

    :param pid: The host pid of the keeper sandbox's PID 1.
    :param start_ticks: That process's start time in clock ticks, which tells it from a later process of the pid.
    :param namespaces: The inode number of each of its user, mount, PID, IPC and UTS namespaces.
    :param pmix: The ``PMIX_SERVER_TMPDIR`` the keeper binds, which every member's must equal.
    """

    pid: int
    start_ticks: int
    namespaces: dict[str, int]
    pmix: str | None

    def encode(self) -> bytes:
        """Return the record as JSON.

        :return: The encoded record.
        """

        return json.dumps(
            {"pid": self.pid, "start_ticks": self.start_ticks, "namespaces": self.namespaces, "pmix": self.pmix}
        ).encode()

    @classmethod
    def decode(cls, data: bytes) -> Self:
        """Decode and check a record.

        :param data: The encoded record.
        :return: The record.
        :raises ValueError: If the record is not valid.
        """

        try:
            document = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("the keeper record is not JSON") from exc
        if not isinstance(document, dict):
            raise ValueError("the keeper record is invalid")
        pid, start_ticks, namespaces, pmix = (document.get(key) for key in ("pid", "start_ticks", "namespaces", "pmix"))
        if (
            not isinstance(pid, int)
            or not isinstance(start_ticks, int)
            or not isinstance(namespaces, dict)
            or set(namespaces) != set(_RECORDED)
            or any(not isinstance(value, int) for value in namespaces.values())
            or not (pmix is None or isinstance(pmix, str))
        ):
            raise ValueError("the keeper record is invalid")
        return cls(pid, start_ticks, namespaces, pmix)


@dataclass(slots=True)
class Keeper:
    """A started keeper sandbox.

    :param process: The Bubblewrap process, a child of the supervisor.
    :param record: The keeper's record.
    :param pidfd: A descriptor that becomes readable once the keeper's PID 1 exits.
    """

    process: subprocess.Popen[bytes]
    record: KeeperRecord
    pidfd: int

    def stop(self) -> None:
        """Kill the keeper's Bubblewrap and wait for it to exit."""

        try:
            self.process.kill()
            self.process.wait()
        finally:
            if self.pidfd >= 0:
                try:
                    select.select([self.pidfd], [], [])
                finally:
                    _close(self.pidfd)
                    self.pidfd = -1


def namespace_identities(pid: int) -> dict[str, int]:
    """Return the inode numbers of a process's namespaces that a keeper record names.

    :param pid: The process.
    :return: The inode number of each of its user, mount, PID, IPC and UTS namespaces.
    :raises OSError: If the process or one of its namespaces cannot be read.
    """

    return {name: os.stat(f"/proc/{pid}/ns/{name}").st_ino for name in _RECORDED}


def start_keeper(prepared: PreparedSandbox, environment: Mapping[str, str], *, pmix: str | None) -> Keeper:
    """Start a keeper sandbox and wait until its program runs inside the finished sandbox.

    *prepared* is consumed. The keeper's standard error is captured during start-up for failure diagnostics and
    discarded after. It runs in its own process group, so signals for the launch reach the ranks, not the keeper;
    it dies with its supervisor instead.

    :param prepared: The keeper's Bubblewrap argument vector (its program last) and descriptors; ``--info-fd``
        is added here.
    :param environment: The environment Bubblewrap and the keeper program are started with.
    :param pmix: The ``PMIX_SERVER_TMPDIR`` the keeper binds, recorded so that every member must match it.
    :return: The running keeper.
    :raises ValueError: If the keeper does not start or does not become ready in time.
    :raises OSError: If Bubblewrap cannot be started.
    """

    info_read, info_write = os.pipe()
    ready_read, ready_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    process: subprocess.Popen[bytes] | None = None
    pidfd = -1
    diagnostics = bytearray()

    def drain_stderr() -> str:
        while len(diagnostics) < _MAX_RECORD_BYTES:
            ready, _writable, _failed = select.select([stderr_read], [], [], 0)
            if not ready:
                break
            chunk = os.read(stderr_read, _MAX_RECORD_BYTES - len(diagnostics))
            if not chunk:
                break
            diagnostics.extend(chunk)
        return diagnostics.decode(errors="replace")

    def failed(message: str) -> ValueError:
        detail = drain_stderr()
        return ValueError(f"{message}: {detail}" if detail else message)

    try:
        argv = [prepared.argv[0], "--info-fd", str(info_write), *prepared.argv[1:]]
        process = subprocess.Popen(
            argv,
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=ready_write,
            stderr=stderr_write,
            pass_fds=(*prepared.descriptors, info_write),
            # Signals for the launch's or a rank's process group must not end every rank of the node at once.
            process_group=0,
            # The supervisor ignores SIGPIPE after closing its startup diagnostics pipe.
            restore_signals=False,
        )
        for descriptor in (info_write, ready_write, stderr_write):
            os.close(descriptor)
        info_write = ready_write = stderr_write = -1
        prepared.close()
        deadline = time.monotonic() + KEEPER_WAIT_SECONDS
        try:
            information = _read_until_eof(info_read, deadline, "the keeper's Bubblewrap information")
            if not information:
                try:
                    status: int | None = process.wait(timeout=_INFO_WAIT_SECONDS)
                except subprocess.TimeoutExpired:
                    status = process.poll()
                raise failed(
                    f"Bubblewrap exited before reporting the keeper's process (exit status {status}); "
                    "its diagnostics follow"
                )
            pid = json.loads(information)["child-pid"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise failed("Bubblewrap did not report the keeper's process") from exc
        if not isinstance(pid, int) or pid <= 0:
            raise failed("Bubblewrap reported an invalid keeper process")
        # Read one byte, not to the end: the sandbox's PID 1 keeps its copy of the keeper's standard output.
        ready, _writable, _failed = select.select([ready_read], [], [], max(0.0, deadline - time.monotonic()))
        if not ready or os.read(ready_read, 1) != b"r":
            raise failed(
                f"the keeper sandbox did not start (Bubblewrap exit status {process.poll()}); diagnostics follow"
            )
        drain_stderr()
        start_ticks = _death.process_identity(pid).start_ticks
        if start_ticks is None:
            raise ValueError("the keeper process ended while starting")
        record = KeeperRecord(pid, start_ticks, namespace_identities(pid), pmix)
        try:
            pidfd = os.pidfd_open(pid)
        except ProcessLookupError:
            raise ValueError("the keeper process ended while starting") from None
        if _death.process_identity(pid).start_ticks != record.start_ticks:
            raise ValueError("the keeper process ended while starting")
        try:
            signal.pidfd_send_signal(pidfd, 0)
        except ProcessLookupError:
            raise ValueError("the keeper process ended while starting") from None
        keeper = Keeper(process, record, pidfd)
        pidfd = -1
        return keeper
    except BaseException:
        if process is not None:
            process.kill()
            process.wait()
        prepared.close()
        raise
    finally:
        for descriptor in (info_read, info_write, ready_read, ready_write, stderr_read, stderr_write, pidfd):
            if descriptor >= 0:
                _close(descriptor)


def open_namespaces(record: KeeperRecord) -> dict[str, int]:
    """Open the namespaces of a recorded keeper, proving that they are its own and differ from this process's.

    :param record: The keeper record.
    :return: Descriptors of its ``user``, ``mnt``, ``pid``, ``ipc`` and ``uts`` namespaces and of ``owner``,
        the user namespace that owns the other four; the caller owns them.
    :raises ValueError: If the keeper is gone, is another process, shares a namespace with this process, or
        its namespaces are not owned by one user namespace.
    :raises OSError: If a namespace cannot be opened.
    """

    pid, recorded = record.pid, record.namespaces
    try:
        pidfd = os.pidfd_open(pid)
    except ProcessLookupError:
        raise ValueError("the launch's namespace keeper on this node is gone") from None
    descriptors: dict[str, int] = {}
    try:
        if _death.process_identity(pid).start_ticks != record.start_ticks:
            raise ValueError("the launch's namespace keeper on this node is gone")
        for name in _RECORDED:
            try:
                descriptors[name] = os.open(f"/proc/{pid}/ns/{name}", os.O_RDONLY | os.O_CLOEXEC)
            except FileNotFoundError:
                raise ValueError("the launch's namespace keeper on this node is gone") from None
            if os.fstat(descriptors[name]).st_ino != recorded[name]:
                raise ValueError(f"the keeper's {name} namespace is not the recorded one")
            if os.stat(f"/proc/self/ns/{name}").st_ino == recorded[name]:
                raise ValueError(f"the keeper shares this helper's {name} namespace; confinement refused")
        try:
            signal.pidfd_send_signal(pidfd, 0)
        except ProcessLookupError:
            raise ValueError("the launch's namespace keeper on this node is gone") from None
        descriptors["owner"] = fcntl.ioctl(descriptors["mnt"], _NS_GET_USERNS)
        owner = os.fstat(descriptors["owner"]).st_ino
        for name in _OWNED[1:]:
            other = fcntl.ioctl(descriptors[name], _NS_GET_USERNS)
            try:
                if os.fstat(other).st_ino != owner:
                    raise ValueError(f"the keeper's {name} namespace has another owner than its mount namespace")
            finally:
                os.close(other)
        return descriptors
    except BaseException:
        for descriptor in descriptors.values():
            _close(descriptor)
        raise
    finally:
        os.close(pidfd)


@dataclass(slots=True)
class Membership:
    """A helper's membership in a keeper generation.

    :param generation_fd: A descriptor of the generation directory.
    :param writer: This member's write end of the generation's ``members`` FIFO.
    :param namespaces: The keeper's namespace descriptors from :func:`open_namespaces`.
    :param supervisor: The supervisor this helper started, retained only to reap it if it already ended.
    """

    generation_fd: int
    writer: int
    namespaces: dict[str, int]
    supervisor: subprocess.Popen[bytes] | None = None

    def command(self, inner: Sequence[str], *, python: Path, cwd: Path, pass_fds: tuple[int, ...]) -> list[str]:
        """Return the command that runs a rank in the keeper's namespaces (:mod:`httk.workflow._confine_enter`).

        Start it with :meth:`take_descriptors` as its passed descriptors, as a child of this process.

        :param inner: The rank's command, run in the keeper's mount namespace.
        :param python: The interpreter that runs the entry program on the host.
        :param cwd: The rank's working directory in the keeper's mount namespace.
        :param pass_fds: Further descriptors the rank inherits.
        :return: The command.
        """

        namespaces = [f"{name}={descriptor}" for name, descriptor in sorted(self.namespaces.items())]
        passed = [option for descriptor in pass_fds for option in ("--pass-fd", str(descriptor))]
        return [
            str(python),
            "-I",
            "-S",
            str(ENTER_PROGRAM),
            "--parent",
            str(os.getpid()),
            "--cwd",
            str(cwd),
            *passed,
            *namespaces,
            "--",
            *inner,
        ]

    def take_descriptors(self) -> tuple[int, ...]:
        """Return and relinquish the namespace descriptors :meth:`command` names.

        :return: The descriptors.
        """

        descriptors = tuple(self.namespaces.values())
        self.namespaces.clear()
        return descriptors

    def leave(self) -> None:
        """Leave the generation without waiting for any other rank."""

        _close(self.writer)
        self.writer = -1
        for descriptor in self.namespaces.values():
            _close(descriptor)
        self.namespaces.clear()
        if self.supervisor is not None:
            self.supervisor.poll()
        _close(self.generation_fd)
        self.generation_fd = -1


def _wait_hangup(poller: select.poll) -> None:
    while not any(events & select.POLLHUP for _descriptor, events in poller.poll()):
        pass


def _await_keeper(
    generation_fd: int,
    writer: int,
    pmix: str | None,
    deadline: float,
    supervisor: subprocess.Popen[bytes] | None = None,
) -> Membership | None:
    """Wait for a published keeper record using an already-open member writer."""

    if not stat.S_ISFIFO(os.fstat(writer).st_mode):
        raise ValueError("the keeper generation's members entry is not a FIFO")
    poller = select.poll()
    poller.register(writer, 0)  # reports only POLLERR, once the read end is gone
    while not _exists(KEEPER_FILE, generation_fd):
        _raise_failed(generation_fd)
        if poller.poll(_POLL_MILLISECONDS):
            _close(writer)
            _close(generation_fd)
            return None
        if time.monotonic() > deadline:
            raise ValueError("timed out waiting for the launch's namespace keeper on this node")
    _raise_failed(generation_fd)
    if _exists(CLOSING, generation_fd):
        _close(writer)
        _close(generation_fd)
        return None
    record = KeeperRecord.decode(read_bounded(generation_fd, KEEPER_FILE, _MAX_RECORD_BYTES))
    if record.pmix != pmix:
        raise ValueError("PMIX_SERVER_TMPDIR differs from the one of the launch's namespace keeper on this node")
    return Membership(generation_fd, writer, open_namespaces(record), supervisor)


def _start_supervisor(
    generation_fd: int, reader: int, prepared: PreparedSandbox, *, pmix: str | None, python: Path
) -> subprocess.Popen[bytes]:
    """Start the detached keeper owner with the descriptors its Bubblewrap command names."""

    command = [
        str(python),
        "-I",
        "-m",
        "httk.workflow._confine_keeper",
        "--generation-fd",
        str(generation_fd),
        "--members-fd",
        str(reader),
    ]
    if pmix is not None:
        command += ["--pmix", pmix]
    command += [item for descriptor in prepared.descriptors for item in ("--bind-fd", str(descriptor))]
    command += ["--", *prepared.argv]
    return subprocess.Popen(
        command,
        env=_SANDBOX_ENVIRONMENT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        pass_fds=(generation_fd, reader, *prepared.descriptors),
    )


def _lead(
    ns_fd: int,
    generation: str,
    prepare: Callable[[], PreparedSandbox],
    pmix: str | None,
    python: Path,
    deadline: float,
) -> Membership | None:
    """Try to lead a generation; ``None`` when another helper published it first."""

    prepared = f".{secrets.token_hex(8)}"
    os.mkdir(prepared, 0o700, dir_fd=ns_fd)
    reader = writer = generation_fd = -1
    try:
        generation_fd = os.open(prepared, _DIRECTORY_FLAGS, dir_fd=ns_fd)
        os.mkfifo(MEMBERS, 0o600, dir_fd=generation_fd)
        reader = os.open(MEMBERS, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=generation_fd)
        writer = os.open(MEMBERS, os.O_WRONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=generation_fd)
        try:
            # The prepared directory holds the FIFO, so this never replaces another generation's directory.
            os.rename(prepared, generation, src_dir_fd=ns_fd, dst_dir_fd=ns_fd)
        except OSError as exc:
            if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise
            os.unlink(MEMBERS, dir_fd=generation_fd)
            os.rmdir(prepared, dir_fd=ns_fd)
            for descriptor in (reader, writer, generation_fd):
                _close(descriptor)
            return None
    except BaseException:
        for descriptor in (reader, writer, generation_fd):
            if descriptor >= 0:
                _close(descriptor)
        raise
    prepared_sandbox: PreparedSandbox | None = None
    try:
        prepared_sandbox = prepare()
        supervisor = _start_supervisor(generation_fd, reader, prepared_sandbox, pmix=pmix, python=python)
    except BaseException as exc:
        try:
            _publish(generation_fd, FAILED_FILE, str(exc).encode()[:_MAX_RECORD_BYTES])
        except OSError:
            pass
        if prepared_sandbox is not None:
            prepared_sandbox.close()
        for descriptor in (reader, writer, generation_fd):
            _close(descriptor)
        raise
    prepared_sandbox.close()
    _close(reader)
    try:
        return _await_keeper(generation_fd, writer, pmix, deadline, supervisor)
    except BaseException:
        _close(writer)
        _close(generation_fd)
        supervisor.poll()
        raise


def _raise_failed(generation_fd: int) -> None:
    if _exists(FAILED_FILE, generation_fd):
        reason = read_bounded(generation_fd, FAILED_FILE, _MAX_RECORD_BYTES).decode(errors="replace")
        raise ValueError(f"the launch's namespace keeper on this node failed: {reason}")


def _follow(ns_fd: int, generation: str, pmix: str | None, deadline: float) -> Membership | None:
    """Join a published generation; ``None`` when it is over and the helper must try the next one."""

    try:
        generation_fd = os.open(generation, _DIRECTORY_FLAGS, dir_fd=ns_fd)
    except FileNotFoundError:
        return None
    writer = -1
    try:
        _raise_failed(generation_fd)
        try:
            writer = os.open(MEMBERS, os.O_WRONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=generation_fd)
        except OSError as exc:
            if exc.errno == errno.ENXIO:  # no read end: the leader is gone
                os.close(generation_fd)
                return None
            raise
        return _await_keeper(generation_fd, writer, pmix, deadline)
    except BaseException:
        _close(writer)
        _close(generation_fd)
        raise


def join(ns_fd: int, *, prepare: Callable[[], PreparedSandbox], pmix: str | None, python: Path) -> Membership:
    """Join the current keeper generation of the launch on this node, leading a new one when there is none.

    :param ns_fd: A descriptor of the launch's ``ns/`` node directory.
    :param prepare: Builds the keeper sandbox for a generation this helper leads.
    :param pmix: This rank's ``PMIX_SERVER_TMPDIR``, which must be the keeper's.
    :param python: The interpreter that starts the supervisor.
    :return: The membership; :meth:`Membership.leave` it once the rank is gone.
    :raises ValueError: If the keeper failed, is gone, cannot be verified, or does not start in time.
    :raises OSError: If the node directory cannot be used.
    """

    deadline = time.monotonic() + KEEPER_WAIT_SECONDS
    for number in range(MAX_GENERATIONS):
        generation = str(number)
        if not _exists(generation, ns_fd):
            membership = _lead(ns_fd, generation, prepare, pmix, python, deadline)
            if membership is not None:
                return membership
        membership = _follow(ns_fd, generation, pmix, deadline)
        if membership is not None:
            return membership
    raise ValueError(f"the launch started more than {MAX_GENERATIONS} namespace keepers on this node")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the supervisor that owns one keeper generation.

    :param argv: Supervisor options followed by ``--`` and the keeper Bubblewrap command.
    :return: Zero after the last member leaves, or 2 when the keeper cannot start.
    """

    arguments = list(sys.argv[1:] if argv is None else argv)
    separator = arguments.index("--") if "--" in arguments else len(arguments)
    parser = argparse.ArgumentParser(prog="python -I -m httk.workflow._confine_keeper")
    parser.add_argument("--generation-fd", required=True, type=int)
    parser.add_argument("--members-fd", required=True, type=int)
    parser.add_argument("--pmix")
    parser.add_argument("--bind-fd", action="append", type=int, default=[])
    parsed = parser.parse_args(arguments[:separator])
    command = arguments[separator + 1 :]
    if not command:
        parser.error("expected a keeper Bubblewrap command after --")
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, signal.SIG_IGN)
    keeper: Keeper | None = None
    try:
        try:
            keeper = start_keeper(PreparedSandbox(command, tuple(parsed.bind_fd)), os.environ, pmix=parsed.pmix)
            for name, identity in keeper.record.namespaces.items():
                if os.stat(f"/proc/self/ns/{name}").st_ino == identity:
                    raise ValueError(f"the keeper shares the supervisor's {name} namespace; confinement refused")
            _publish(parsed.generation_fd, KEEPER_FILE, keeper.record.encode())
        except (OSError, ValueError) as exc:
            try:
                _publish(parsed.generation_fd, FAILED_FILE, str(exc).encode()[:_MAX_RECORD_BYTES])
            except OSError:
                pass
            return 2
        poller = select.poll()
        poller.register(parsed.members_fd, select.POLLIN)
        _wait_hangup(poller)
        try:
            os.mkdir(CLOSING, 0o700, dir_fd=parsed.generation_fd)
        finally:
            _wait_hangup(poller)
        keeper.stop()
        return 0
    finally:
        if keeper is not None and keeper.process.poll() is None:
            keeper.stop()
        for descriptor in (*parsed.bind_fd, parsed.members_fd, parsed.generation_fd):
            _close(descriptor)


if __name__ == "__main__":
    raise SystemExit(main())
