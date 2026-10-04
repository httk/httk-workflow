"""Supervise fixed Slurm steps inside the protected allocation sandbox."""

import argparse
import os
import re
import selectors
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from types import FrameType

from ._daemon_bootstrap import _count
from ._daemon_mpi_protocol import decode_request, encode_terminal, recv_frame, send_frame
from ._daemon_policy import Policy, Profile, load_policy

_SOCKET = Path("/run/httk-mpi/control.sock")
_IO_TIMEOUT = 5.0


def _environment(policy: Policy) -> dict[str, str]:
    environment = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
    if policy.slurm_conf is not None:
        environment["SLURM_CONF"] = str(policy.slurm_conf)
    return environment


def _command(policy: Policy, profile: Profile, arguments: argparse.Namespace, request_id: str | None) -> list[str]:
    assert policy.mpi is not None and profile.mpi is not None
    manager = request_id is None
    command = [
        str(policy.mpi.srun),
        "--mpi=none" if manager else "--mpi=pmix",
        "--overlap",
        f"--jobid={arguments.job_id}",
        f"--nodes={1 if manager else profile.mpi.nodes}",
        f"--ntasks={1 if manager else profile.mpi.ranks}",
        "--export=NONE",
        "--chdir=/",
        "--input=/dev/null",
        "--kill-on-bad-exit=1",
    ]
    if manager or profile.cpus is not None:
        # Without a profile value, srun's default of one CPU per task matches what sbatch allocated.
        command.append(f"--cpus-per-task={1 if manager else profile.cpus}")
    if manager:
        command += [f"--nodelist={arguments.node}", "--output=/dev/null", "--error=/dev/null"]
    elif getattr(profile.mpi, "ntasks_per_node", None) is not None:
        command.append(f"--ntasks-per-node={profile.mpi.ntasks_per_node}")
    command += [
        str(policy.python),
        "-I",
        "-S",
        str(Path(__file__).with_name("_daemon_bootstrap.py")),
        "--policy",
        arguments.policy_source,
        "--mode",
        "payload" if manager else "mpi-rank",
        "--profile",
        profile.name,
        "--handle",
        arguments.handle,
    ]
    if not manager:
        return [*command, "--request-id", str(request_id)]
    command += ["--control-source", arguments.control_source, "--procs", arguments.procs]
    return command if arguments.mem_mb is None else [*command, "--mem-mb", arguments.mem_mb]


def _signal_group(process: subprocess.Popen[bytes], number: int) -> bool:
    try:
        os.killpg(process.pid, number)
        return True
    except ProcessLookupError:
        return False


def _reap(process: subprocess.Popen[bytes], grace: float) -> bool:
    """Reap an owned group; return whether forced cleanup was needed."""

    forced = False
    try:
        _signal_group(process, signal.SIGTERM)
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            forced = True
        # A reaped leader may still have children holding streams or other resources.
        if _signal_group(process, signal.SIGKILL):
            forced = True
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            forced = True
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()
    return forced


def _status(code: int) -> int:
    return min(255, code if code >= 0 else 128 - code)


class _Service:
    def __init__(self, policy: Policy, profile: Profile, arguments: argparse.Namespace) -> None:
        self.policy = policy
        self.profile = profile
        self.arguments = arguments
        self.stop = False
        self.failed = False
        self.used: set[str] = set()
        self.manager: subprocess.Popen[bytes] | None = None
        self.application: subprocess.Popen[bytes] | None = None
        self.client: socket.socket | None = None
        self.exit_deadline: float | None = None
        self.selector = selectors.DefaultSelector()

    def _spawn(self, request_id: str | None) -> subprocess.Popen[bytes]:
        manager = request_id is None
        return subprocess.Popen(
            _command(self.policy, self.profile, self.arguments, request_id),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL if manager else subprocess.PIPE,
            stderr=subprocess.DEVNULL if manager else subprocess.PIPE,
            cwd="/",
            env=_environment(self.policy),
            close_fds=True,
            start_new_session=True,
        )

    @staticmethod
    def _terminal(connection: socket.socket, code: int, error: str | None = None) -> None:
        send_frame(connection, b"X", encode_terminal(code, error))

    def _accept(self, listener: socket.socket) -> None:
        connection, _ = listener.accept()
        connection.settimeout(_IO_TIMEOUT)
        admitted = False
        try:
            kind, data = recv_frame(connection)
            if kind != b"Q":
                raise ValueError("expected one MPI run request")
            request_id = decode_request(data)
            assert self.policy.mpi is not None
            if self.application is not None:
                self._terminal(connection, 2, "an MPI application is already active")
            elif request_id in self.used:
                self._terminal(connection, 2, "MPI request ID has already been used")
            elif len(self.used) >= self.policy.mpi.max_steps:
                self._terminal(connection, 2, "MPI allocation request limit reached")
            elif self.stop or self.manager is None or self.manager.poll() is not None:
                self._terminal(connection, 2, "MPI allocation is stopping")
            else:
                self.used.add(request_id)
                try:
                    self.application = self._spawn(request_id)
                except OSError:
                    self._terminal(connection, 2, "MPI step could not start; request will not be retried")
                    return
                self.client = connection
                admitted = True
                self.selector.register(connection, selectors.EVENT_READ, "client")
                assert self.application.stdout is not None and self.application.stderr is not None
                for stream, label in ((self.application.stdout, "stdout"), (self.application.stderr, "stderr")):
                    os.set_blocking(stream.fileno(), False)
                    self.selector.register(stream, selectors.EVENT_READ, label)
        except (OSError, ValueError, EOFError):
            if admitted:
                self.stop = self.failed = True
        finally:
            if not admitted:
                connection.close()

    def _unregister(self, descriptor: int) -> None:
        try:
            self.selector.unregister(descriptor)
        except (KeyError, ValueError):
            pass

    def _finish(self, code: int) -> None:
        assert self.application is not None and self.client is not None and self.policy.mpi is not None
        process, connection = self.application, self.client
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                self._unregister(stream.fileno())
        self._unregister(connection.fileno())
        forced = _reap(process, self.policy.mpi.termination_grace)
        if forced:
            self.stop = self.failed = True
        try:
            self._terminal(connection, 2 if forced else _status(code), "MPI cleanup is uncertain" if forced else None)
        except (OSError, ValueError):
            self.stop = self.failed = True
        finally:
            connection.close()
            self.application = None
            self.client = None
            self.exit_deadline = None

    def _poll_completion(self) -> None:
        if self.application is None:
            return
        code = self.application.poll()
        if code is None:
            return
        streams = any(key.data in ("stdout", "stderr") for key in self.selector.get_map().values())
        if not streams:
            self._finish(code)
        elif self.exit_deadline is None:
            self.exit_deadline = time.monotonic() + _IO_TIMEOUT
        elif time.monotonic() >= self.exit_deadline:
            self.stop = self.failed = True

    def _event(self, key: selectors.SelectorKey, listener: socket.socket) -> None:
        if key.data == "listener":
            self._accept(listener)
        elif key.data == "client":
            # EOF cancels the step; extra bytes violate the one-request protocol.
            self.stop = self.failed = True
        else:
            try:
                chunk = os.read(key.fd, 65_536)
            except BlockingIOError:
                return
            if not chunk:
                self._unregister(key.fd)
                return
            assert self.client is not None
            try:
                send_frame(self.client, b"O" if key.data == "stdout" else b"E", chunk)
            except (OSError, ValueError):
                self.stop = self.failed = True

    def run(self) -> int:
        assert self.policy.mpi is not None
        bound = False
        result = 2
        cleanup_uncertain = False
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(_SOCKET))
            bound = True
            os.chmod(_SOCKET, 0o600)
            listener.listen(8)
            listener.setblocking(False)
            self.selector.register(listener, selectors.EVENT_READ, "listener")
            self.manager = self._spawn(None)
            while not self.stop:
                if self.manager.poll() is not None:
                    self.failed = self.failed or self.application is not None
                    break
                for key, _ in self.selector.select(0.1):
                    self._event(key, listener)
                    if self.stop:
                        break
                self._poll_completion()
            result = 2 if self.failed else _status(self.manager.returncode or 0)
        finally:
            self.stop = True
            if self.client is not None:
                try:
                    self.client.close()
                except OSError:
                    cleanup_uncertain = True
            for process in (self.application, self.manager):
                if process is None:
                    continue
                try:
                    cleanup_uncertain = _reap(process, self.policy.mpi.termination_grace) or cleanup_uncertain
                except (OSError, subprocess.SubprocessError):
                    cleanup_uncertain = True
            try:
                self.selector.close()
            except OSError:
                cleanup_uncertain = True
            try:
                listener.close()
            except OSError:
                cleanup_uncertain = True
            if bound:
                try:
                    _SOCKET.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    cleanup_uncertain = True
        return 2 if self.failed or cleanup_uncertain else result


def main(argv: Sequence[str] | None = None) -> int:
    """Run the protected allocation service after Bubblewrap confinement.

    :param argv: Fixed bootstrap arguments, or process arguments when omitted.
    :return: Allocation manager status, or two after a refused or uncertain run.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("policy-source", "profile", "handle", "job-id", "node", "control-source"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--procs", required=True)
    parser.add_argument("--mem-mb")
    arguments = parser.parse_args(argv)
    try:
        policy = load_policy(Path("/daemon-policy.json"))
        profile = policy.profile(arguments.profile)
        if policy.mpi is None or profile.mpi is None:
            raise ValueError("MPI service requires a protected MPI profile")
        if re.fullmatch(r"[0-9a-f]{32}", arguments.handle) is None:
            raise ValueError("invalid MPI manager handle")
        if re.fullmatch(r"[1-9][0-9]{0,19}", arguments.job_id) is None:
            raise ValueError("invalid allocation job ID")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", arguments.node) is None:
            raise ValueError("invalid allocation node")
        _count(arguments.procs, "--procs", positive=True)
        if arguments.mem_mb is not None:
            _count(arguments.mem_mb, "--mem-mb", positive=True)
        service = _Service(policy, profile, arguments)

        def stop(_number: int, _frame: FrameType | None) -> None:
            service.stop = True
            service.failed = True

        previous = {number: signal.signal(number, stop) for number in (signal.SIGINT, signal.SIGTERM)}
        try:
            return service.run()
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"MPI allocation: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
