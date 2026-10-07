"""Fixed Slurm operations for the workspace daemon broker."""

import os
import re
import selectors
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from ._daemon_bootstrap import _operator_environment
from ._daemon_policy import ApprovedLauncher, Policy
from .launch_runtime import SubmissionIdentity, slurm_submission

_VERSION = re.compile(r"slurm (\d+)\.(\d+)\.(\d+)(?:[.-][A-Za-z0-9.-]+)?\s*\Z")
_JOB = re.compile(r"[1-9][0-9]{0,19}\Z")
_STATE = re.compile(r"[A-Z_]{1,64}\Z")
_EXIT_CODE = re.compile(r"[0-9]{1,3}:[0-9]{1,3}\Z")
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\Z")


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


@dataclass(frozen=True, slots=True)
class Observation:
    """One manager job's Slurm accounting record.

    :param scheduler_state: Uppercase scheduler state, without a ``by <uid>`` suffix.
    :param exit_code: Slurm ``exit:signal`` code.
    :param started_at: Slurm start time as printed, or ``None`` before the job started.
    :param ended_at: Slurm end time as printed, or ``None`` before the job ended.
    """

    scheduler_state: str
    exit_code: str
    started_at: str | None
    ended_at: str | None


def excerpt(text: str, limit: int = 500) -> str:
    """Return printable single-line ASCII from untrusted client text.

    :param text: Decoded client output or an error message.
    :param limit: Largest returned length in characters.
    :return: The text with non-ASCII as ``?``, controls as spaces, whitespace collapsed and cut to ``limit``.
    """

    ascii_text = text.encode("ascii", "replace").decode("ascii")
    return " ".join(re.sub(r"[\x00-\x1f\x7f]", " ", ascii_text).split())[:limit]


def _failure(argv: list[str], what: str, detail: bytes | str) -> SchedulerError:
    """Describe one failed client call by basename, failure and output excerpt."""

    text = excerpt(detail.decode("utf-8", "replace") if isinstance(detail, bytes) else detail)
    return SchedulerError(f"{Path(argv[0]).name} {what}" + (f": {text}" if text else ""))


def _run(argv: list[str], policy: Policy, *, data: bytes = b"") -> tuple[int, bytes, bytes]:
    """Run one trusted client with bounded input, output and lifetime; return code, stdout and stderr."""

    # Inside the broker os.environ is the filtered operator environment with a private HOME; the C locale
    # keeps the parsed client output stable.
    environment = {"PATH": "/usr/bin:/bin", **_operator_environment(os.environ), "LANG": "C", "LC_ALL": "C"}
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
        raise _failure(argv, "could not start", str(exc)) from exc

    deadline = time.monotonic() + policy.command_timeout
    output = {"stdout": bytearray(), "stderr": bytearray()}
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
                    raise _failure(argv, "timed out", bytes(output["stderr"]))
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
                            raise _failure(argv, "output exceeded its limit", bytes(output["stderr"]))
                        output[key.data].extend(chunk)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _failure(argv, "timed out", bytes(output["stderr"]))
            return process.wait(timeout=remaining), bytes(output["stdout"]), bytes(output["stderr"])
    except subprocess.TimeoutExpired as exc:
        raise _failure(argv, "timed out", bytes(output["stderr"])) from exc
    except OSError as exc:
        raise _failure(argv, "failed", str(exc)) from exc
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


def _checked(argv: list[str], policy: Policy, *, data: bytes = b"") -> bytes:
    """Run one client, refuse a nonzero exit with its stderr excerpt, and return stdout."""

    code, stdout, stderr = _run(argv, policy, data=data)
    if code != 0:
        raise _failure(argv, f"exited {code}", stderr)
    return stdout


def manager_argv(policy: Policy) -> list[str]:
    """Return the manager command of every daemon submission, before the launcher's own additions.

    :param policy: Validated runtime policy.
    :return: The approved interpreter running a serving manager on the real workspace path; the workspace
        has the exchange extension, so every unrestricted manager serves its exchange.
    """

    return [
        str(policy.python),
        "-I",
        "-m",
        "httk.core.cli",
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        str(policy.workspace),
        "--idle",
    ]


def submission(policy: Policy, launcher: ApprovedLauncher, handle: str) -> tuple[list[str], bytes]:
    """Build the ``sbatch`` command and script of one manager from the frozen launcher content.

    The packaged ``slurm`` launcher logic runs in this process; the bundle itself is never executed.

    :param policy: Validated runtime policy.
    :param launcher: The approved launcher, whose frozen settings alone shape the script.
    :param handle: The broker's opaque manager handle, which names the job.
    :return: The ``sbatch`` argument vector and the script bytes for its standard input.
    :raises ValueError: If a frozen setting cannot be expressed in the script.
    """

    argv, script = slurm_submission(
        settings=dict(launcher.settings),
        argv=manager_argv(policy),
        workspace=str(policy.workspace),
        identity=SubmissionIdentity(
            job_name=f"httk-{handle}",
            sbatch=policy.sbatch,
            cluster=policy.cluster,
            output=str(policy.jobs / "httk-%j.out"),
        ),
    )
    return argv, script.encode("utf-8")


class SlurmGateway:
    """Execute only the broker's fixed scheduler operations.

    :param policy: Validated runtime policy snapshot.
    """

    def __init__(self, policy: Policy) -> None:
        self.policy = policy
        self._uid = str(os.getuid())

    def check(self) -> None:
        """Require clients supporting controller-side cancellation filters.

        :raises SchedulerError: If a client fails or its version is unsupported.
        """

        for executable in (self.policy.sbatch, self.policy.squeue, self.policy.scancel):
            code, output, errors = _run([str(executable), "--version"], self.policy)
            try:
                match = _VERSION.fullmatch(output.decode("ascii"))
            except UnicodeDecodeError:
                match = None
            if code != 0 or match is None or tuple(int(part) for part in match.groups()) < (23, 11, 6):
                # Wrappers and loaders report failures on stderr.
                printed = (output + errors).decode("utf-8", "replace").strip()[:200]
                raise SchedulerError(
                    f"Slurm 23.11.6 or newer clients are required: {executable} --version "
                    f"exited {code} and printed {printed!r}"
                )

    def submit(self, launcher: ApprovedLauncher, handle: str) -> Submission:
        """Submit one manager through the frozen launcher content, under the handle's job name.

        :param launcher: An approved launcher.
        :param handle: The broker's newly reserved opaque handle.
        :return: The confirmed scheduler job identity.
        :raises UncertainSubmission: If acceptance cannot be confirmed.
        """

        try:
            argv, script = submission(self.policy, launcher, handle)
        except ValueError as exc:
            raise SchedulerError(f"cannot build the submission: {exc}") from exc
        try:
            output = _checked(argv, self.policy, data=script)
            fields = output.decode("ascii", "replace").strip().split(";")
            if (
                len(fields) not in (1, 2)
                or _JOB.fullmatch(fields[0]) is None
                or (len(fields) == 2 and fields[1] != self.policy.cluster)
            ):
                raise _failure(argv, "exited 0 without a confirmed job", output)
            return Submission(fields[0], self.policy.cluster)
        except SchedulerError as exc:
            raise UncertainSubmission(str(exc)) from exc

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
        output = _checked(
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

    def accounting(self, job_id: str, cluster: str, handle: str) -> Observation | None:
        """Read the accounting record of one recorded manager job when its identity matches.

        :param job_id: Protected numeric job identifier.
        :param cluster: Protected scheduler cluster.
        :param handle: Protected correlation handle.
        :return: The matching record, or ``None`` without one or without a configured ``sacct``.
        :raises SchedulerError: If the client fails or the matching record is malformed or ambiguous.
        """

        if self.policy.sacct is None:
            return None
        if cluster != self.policy.cluster:
            raise SchedulerError("recorded cluster is outside the current policy")
        output = _checked(
            [
                str(self.policy.sacct),
                f"--clusters={cluster}",
                "-j",
                job_id,
                "-X",
                "-n",
                "-P",
                "--format=JobID,JobName,UID,State,ExitCode,Start,End",
            ],
            self.policy,
        )
        rows = [
            fields
            for fields in (line.split("|") for line in output.decode("ascii", "replace").splitlines())
            if fields[:3] == [job_id, f"httk-{handle}", self._uid] and len(fields) == 7
        ]
        if not rows:
            return None
        if len(rows) != 1:
            raise SchedulerError("scheduler accounting is ambiguous")
        state, exit_code, started, ended = rows[0][3:]
        state = state.split(" ", 1)[0]  # "CANCELLED by 123"
        if _STATE.fullmatch(state) is None or _EXIT_CODE.fullmatch(exit_code) is None:
            raise SchedulerError("scheduler accounting is malformed")
        # Slurm prints "Unknown" or "None" for times it does not have.
        return Observation(
            state,
            exit_code,
            started if _TIME.fullmatch(started) else None,
            ended if _TIME.fullmatch(ended) else None,
        )

    def cancel(self, job_id: str, cluster: str, handle: str) -> None:
        """Request cancellation with identity filters applied by Slurm itself.

        :param job_id: Protected numeric job identifier.
        :param cluster: Protected scheduler cluster.
        :param handle: Protected correlation handle.
        :raises SchedulerError: If the cancellation request fails.
        """

        if cluster != self.policy.cluster:
            raise SchedulerError("recorded cluster is outside the current policy")
        _checked(
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
