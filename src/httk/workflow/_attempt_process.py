"""The attempt process: its gated start, its stdio chronicle markers and its process-group signals."""

import logging
import os
import signal
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from ._jobdir import JobDirectory
from ._sandbox import PreparedSandbox
from ._util import utc_now
from .models import LOGS_DIRECTORY

#: Records keep the manager's logger name: operators and tests filter on it.
_LOGGER = logging.getLogger("httk.workflow.manager")
_LAUNCHER = Path(__file__).with_name("_launcher.py")


def write_marker(descriptor: int, line: str, job_key: str) -> None:
    """Write one evidence marker, retaining launch progress on ordinary errors.

    :param descriptor: The open stdio chronicle.
    :param line: The marker line.
    :param job_key: The job key, for the warning.
    """

    try:
        os.write(descriptor, ("\n" + line).encode("utf-8", errors="backslashreplace"))
    except Exception as exc:
        _LOGGER.warning("cannot append an evidence marker for %s: %s", job_key, exc)


def append_log_line(job_dir: JobDirectory, line: str, *, job_key: str) -> None:
    """Append one complete evidence line to the job's stdio chronicle.

    The chronicle is opened through the job directory without following a
    symlink or blocking on a FIFO the job may have planted; such a chronicle is
    skipped with a warning, never written through.

    :param job_dir: The pinned job directory.
    :param line: The evidence line.
    :param job_key: The job key, for warnings.
    """

    descriptor = -1
    try:
        with job_dir.directory(LOGS_DIRECTORY, create=True) as logs:
            descriptor = logs.open_append("stdio.out")
        write_marker(descriptor, line, job_key)
    except Exception as exc:
        _LOGGER.warning("cannot append the stdio chronicle for %s: %s", job_key, exc)
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except Exception as exc:
                _LOGGER.warning("cannot close the stdio chronicle for %s: %s", job_key, exc)


def write_attempt_end(job_dir: JobDirectory, attempt_id: str, return_code: int, action: str, *, job_key: str) -> None:
    """Append the end marker of a reaped attempt to the job's stdio chronicle.

    :param job_dir: The pinned job directory.
    :param attempt_id: The attempt identifier.
    :param return_code: The attempt process's exit status.
    :param action: The attempt's published outcome action, or ``none``.
    :param job_key: The job key, for warnings.
    """

    append_log_line(
        job_dir,
        f"=== httk attempt {attempt_id} ended {utc_now()} exit {return_code} outcome {action}\n",
        job_key=job_key,
    )


def start_gated(
    command: Sequence[str],
    *,
    gate_read: int,
    cwd: Path,
    environment: Mapping[str, str],
    stdio_fd: int,
    confined: bool,
    sandbox: PreparedSandbox | None,
    runner_fd: int | None,
) -> subprocess.Popen[bytes]:
    """Start the launcher of one attempt, blocked on its gate, in a new session.

    The launcher execs *command* (inside *sandbox* when given) only after
    :func:`release_gate`; closing the gate instead makes it exit.

    :param command: The runner command.
    :param gate_read: The read end of the gate pipe.
    :param cwd: The attempt's working directory.
    :param environment: The runner environment.
    :param stdio_fd: The stdio chronicle, which receives stdout and stderr.
    :param confined: Whether the attempt is confined; a confined attempt gets no terminal input.
    :param sandbox: The prepared sandbox, or ``None`` for an unconfined attempt.
    :param runner_fd: The verified runner's descriptor to pass through, or ``None``.
    :return: The launcher process, whose pid is also its process group.
    """

    return subprocess.Popen(
        [
            sys.executable,
            str(_LAUNCHER),
            str(gate_read),
            "--",
            # The gate stays outside the sandbox: the launcher execs
            # Bubblewrap, which keeps the launcher's process group.
            *(sandbox.argv if sandbox is not None else ()),
            *command,
        ],
        cwd=cwd,
        env=environment,
        # A confined attempt gets no terminal input to inject into.
        stdin=subprocess.DEVNULL if confined else None,
        stdout=stdio_fd,
        stderr=stdio_fd,
        start_new_session=True,
        pass_fds=(
            gate_read,
            *([runner_fd] if runner_fd is not None else []),
            *(sandbox.descriptors if sandbox is not None else ()),
        ),
    )


def release_gate(gate_write: int) -> None:
    """Let a launcher started by :func:`start_gated` exec its command.

    :param gate_write: The write end of the gate pipe.
    """

    os.write(gate_write, b"R")


def process_group_alive(process_group: int) -> bool:
    """Report whether one process group still exists on this host.

    :param process_group: The process group identifier.
    :return: Whether the group exists.
    """

    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # It exists and belongs to somebody else, which is still existence.
        return True
    except OSError:
        return True
    return True


def terminate_process(process_group: int, signal_number: int = signal.SIGTERM) -> None:
    """Signal one process group, ignoring a group that is already gone.

    :param process_group: The process group identifier.
    :param signal_number: The signal to send.
    """

    # killpg is load-bearing: a confined attempt is the launcher exec'ing
    # Bubblewrap, whose namespace init and command share this process
    # group. Signalling the outer pid alone (Popen.terminate/kill) would
    # leave them running, so every stop goes through the process group.
    try:
        os.killpg(process_group, signal_number)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        _LOGGER.warning("cannot signal process group %d: %s", process_group, exc)
