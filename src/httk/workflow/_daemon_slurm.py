"""Fixed Slurm operations for the confined workspace broker."""

import os
import re
import selectors
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from ._daemon_policy import Policy, Profile

_VERSION = re.compile(r"slurm (\d+)\.(\d+)\.(\d+)(?:[.-][A-Za-z0-9.-]+)?\s*\Z")
_JOB = re.compile(r"[1-9][0-9]{0,19}\Z")
_STATE = re.compile(r"[A-Z_]{1,64}\Z")


class SchedulerError(RuntimeError):
    """A scheduler operation did not produce a confirmed bounded result."""


class UncertainSubmission(SchedulerError):
    """Submission may have succeeded and must not be retried automatically."""


@dataclass(frozen=True, slots=True)
class Submission:
    """A scheduler identity retained only in protected broker state.

    :param job_id: Numeric Slurm job identifier.
    :param cluster: The configured Slurm cluster.
    """

    job_id: str
    cluster: str


def _run(argv: list[str], policy: Policy, *, data: bytes = b"") -> tuple[int, bytes]:
    """Run one trusted client with bounded input, output and lifetime."""

    environment = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
    if policy.slurm_conf is not None:
        environment["SLURM_CONF"] = str(policy.slurm_conf)
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE if data else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/",
            env=environment,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as exc:
        raise SchedulerError("scheduler client could not start") from exc

    deadline = time.monotonic() + policy.command_timeout
    output = bytearray()
    total = 0
    written = 0
    try:
        assert process.stdout is not None and process.stderr is not None
        with selectors.DefaultSelector() as selector:
            for stream, label in ((process.stdout, "stdout"), (process.stderr, "stderr")):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, label)
            if process.stdin is not None:
                os.set_blocking(process.stdin.fileno(), False)
                selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SchedulerError("scheduler client timed out")
                for key, _ in selector.select(remaining):
                    if key.data == "stdin":
                        try:
                            written += os.write(key.fd, data[written : written + 4096])
                        except BrokenPipeError:
                            written = len(data)
                        if written == len(data):
                            selector.unregister(key.fileobj)
                            assert process.stdin is not None
                            process.stdin.close()
                    else:
                        chunk = os.read(key.fd, 4096)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        total += len(chunk)
                        if total > policy.max_output_bytes:
                            raise SchedulerError("scheduler output exceeded its limit")
                        if key.data == "stdout":
                            output.extend(chunk)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SchedulerError("scheduler client timed out")
            return process.wait(timeout=remaining), bytes(output)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SchedulerError("scheduler client failed") from exc
    finally:
        # Clients may leave a helper holding a pipe after their leader exits.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        for closing_stream in (process.stdin, process.stdout, process.stderr):
            if closing_stream is not None:
                closing_stream.close()


class SlurmGateway:
    """Execute only the broker's fixed scheduler operations.

    :param policy: Validated operator policy.
    :param policy_path: Protected policy path visible on compute nodes.
    """

    def __init__(self, policy: Policy, policy_path: Path) -> None:
        self.policy = policy
        self.policy_path = policy_path
        self._uid = str(os.getuid())

    def check(self) -> None:
        """Require clients supporting controller-side cancellation filters.

        :raises SchedulerError: If a client fails or its version is unsupported.
        """

        for executable in (self.policy.sbatch, self.policy.squeue, self.policy.scancel):
            code, output = _run([str(executable), "--version"], self.policy)
            try:
                match = _VERSION.fullmatch(output.decode("ascii"))
            except UnicodeDecodeError:
                match = None
            if code != 0 or match is None or tuple(int(part) for part in match.groups()) < (23, 11, 6):
                raise SchedulerError("Slurm 23.11.6 or newer clients are required")

    def _script(self, profile: Profile, handle: str) -> bytes:
        bootstrap = Path(__file__).with_name("_daemon_bootstrap.py")
        command = [
            str(self.policy.python),
            "-I",
            "-S",
            str(bootstrap),
            "--policy",
            str(self.policy_path),
            "--mode",
            "payload",
            "--profile",
            profile.name,
            "--handle",
            handle,
        ]
        return ("#!/bin/sh\nexec " + shlex.join(command) + "\n").encode("utf-8")

    def submit(self, profile: Profile, handle: str) -> Submission:
        """Submit one fixed manager bootstrap from trusted script bytes.

        :param profile: An approved resource profile.
        :param handle: The broker's newly reserved opaque handle.
        :return: The confirmed scheduler job identity.
        :raises UncertainSubmission: If acceptance cannot be confirmed.
        """

        argv = [
            str(self.policy.sbatch),
            "--parsable",
            "--export=NIL",
            f"--clusters={self.policy.cluster}",
            f"--job-name=httk-{handle}",
            "--nodes=1",
            "--ntasks=1",
            f"--cpus-per-task={profile.cpus}",
            f"--mem={profile.memory_mb}M",
            f"--time={profile.time_minutes}",
            "--chdir=/",
            "--input=/dev/null",
            "--output=/dev/null",
            "--error=/dev/null",
        ]
        if profile.partition is not None:
            argv.append(f"--partition={profile.partition}")
        if profile.account is not None:
            argv.append(f"--account={profile.account}")
        try:
            code, output = _run(argv, self.policy, data=self._script(profile, handle))
            fields = output.decode("ascii").strip().split(";")
            if (
                code != 0
                or len(fields) not in (1, 2)
                or _JOB.fullmatch(fields[0]) is None
                or (len(fields) == 2 and fields[1] != self.policy.cluster)
            ):
                raise SchedulerError("submission output did not confirm the job")
            return Submission(fields[0], self.policy.cluster)
        except (SchedulerError, UnicodeDecodeError) as exc:
            raise UncertainSubmission("submission acceptance is unknown") from exc

    def status(self, job_id: str, cluster: str, handle: str) -> str:
        """Read active status only when the recorded job identity matches.

        :param job_id: Protected numeric job identifier.
        :param cluster: Protected scheduler cluster.
        :param handle: Protected correlation handle.
        :return: An uppercase scheduler state, or ``UNKNOWN``.
        :raises SchedulerError: If the client fails or its output is malformed.
        """

        if cluster != self.policy.cluster:
            raise SchedulerError("recorded cluster is outside the current policy")
        code, output = _run(
            [
                str(self.policy.squeue),
                "--noheader",
                f"--clusters={cluster}",
                f"--jobs={job_id}",
                f"--name=httk-{handle}",
                f"--user={self._uid}",
                "--format=%i|%j|%U|%T",
            ],
            self.policy,
        )
        if code != 0:
            raise SchedulerError("scheduler status is unavailable")
        try:
            lines = [line.strip() for line in output.decode("ascii").splitlines() if line.strip()]
        except UnicodeDecodeError as exc:
            raise SchedulerError("scheduler status is malformed") from exc
        # Explicit cluster queries may include a cluster heading.
        if lines[:1] == [f"CLUSTER: {cluster}"]:
            lines = lines[1:]
        rows = [line.split("|") for line in lines]
        if not rows:
            return "UNKNOWN"
        if len(rows) != 1:
            raise SchedulerError("scheduler status is ambiguous")
        row = [field.strip() for field in rows[0]]
        if len(row) != 4 or row[:3] != [job_id, f"httk-{handle}", self._uid]:
            return "UNKNOWN"
        if _STATE.fullmatch(row[3]) is None:
            raise SchedulerError("scheduler status is malformed")
        return row[3]

    def cancel(self, job_id: str, cluster: str, handle: str) -> None:
        """Request cancellation with identity filters applied by Slurm itself.

        :param job_id: Protected numeric job identifier.
        :param cluster: Protected scheduler cluster.
        :param handle: Protected correlation handle.
        :raises SchedulerError: If the cancellation request fails.
        """

        if cluster != self.policy.cluster:
            raise SchedulerError("recorded cluster is outside the current policy")
        code, _ = _run(
            [
                str(self.policy.scancel),
                "--ctld",
                f"--clusters={cluster}",
                f"--name=httk-{handle}",
                f"--user={self._uid}",
                job_id,
            ],
            self.policy,
        )
        if code != 0:
            raise SchedulerError("scheduler cancellation request failed")
