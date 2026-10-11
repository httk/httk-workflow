"""Enter a launch's keeper namespaces and run one rank there (see :mod:`httk.workflow._confine_keeper`).

The rank helper runs this file by path as ``python -I -S _confine_enter.py ...``, so it uses the standard
library only. Entering a user namespace needs a single-threaded process, and the rank helper is not one once
it imported the package's numerical dependencies.

It enters the keeper's owning user namespace, then the mount, PID, IPC and UTS namespaces, then the keeper's
own user namespace, and blocks SIGTERM, SIGINT and SIGHUP, forwarding any that arrived before the rank
started, while the helper forwards later signals to the rank's process group. It starts the rank as its child
in the keeper's PID namespace with its own environment and the passed descriptors. The rank dies with this process, which dies
with the helper; it can never gain privileges, starts with an empty capability bounding set, and execs as
the keeper's unprivileged user, which clears its capabilities. This process exits with the rank's status,
``128+N`` for signal ``N``.
"""

import argparse
import ctypes
import errno
import os
import signal
import subprocess
import sys

#: The namespaces in the order they are entered: the owning user namespace, the namespaces it owns, then the
#: keeper's own user namespace (the same one, or a child of it).
NAMESPACES = (
    ("owner", os.CLONE_NEWUSER),
    ("mnt", os.CLONE_NEWNS),
    ("pid", os.CLONE_NEWPID),
    ("ipc", os.CLONE_NEWIPC),
    ("uts", os.CLONE_NEWUTS),
    ("user", os.CLONE_NEWUSER),
)
_IGNORED_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
_PR_SET_PDEATHSIG = 1
_PR_CAPBSET_DROP = 24
_PR_SET_NO_NEW_PRIVS = 38


def _prctl(libc: ctypes.CDLL, option: int, value: int) -> None:
    if libc.prctl(option, ctypes.c_ulong(value), ctypes.c_ulong(0), ctypes.c_ulong(0), ctypes.c_ulong(0)):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def enter(descriptors: dict[str, int]) -> None:
    """Enter the keeper's namespaces and close their descriptors.

    :param descriptors: A descriptor for each name of :data:`NAMESPACES`.
    :raises ValueError: If this process would be root in the keeper's user namespace.
    :raises OSError: If a namespace cannot be entered, for example because this process has several threads.
    """

    own = os.fstat(descriptors["user"]).st_ino == os.fstat(descriptors["owner"]).st_ino
    for name, flag in NAMESPACES:
        if not (name == "user" and own):
            os.setns(descriptors[name], flag)
        os.close(descriptors[name])
    if 0 in os.getresuid() or 0 in os.getresgid():
        raise ValueError("the keeper's user namespace maps this user to root; confinement refused")


def rank_preexec(libc: ctypes.CDLL, last_cap: int) -> None:
    """Prepare the rank between fork and exec.

    :param libc: The C library, loaded before the fork.
    :param last_cap: The highest capability number of the kernel.
    :raises OSError: If this process is already gone or a setting fails.
    """

    signal.pthread_sigmask(signal.SIG_UNBLOCK, _IGNORED_SIGNALS)
    _prctl(libc, _PR_SET_PDEATHSIG, signal.SIGKILL)
    # The parent is outside the keeper's PID namespace, so a live parent reads as 0; a dead one as the PID 1 of
    # the keeper, which adopted the rank.
    if os.getppid() != 0:
        raise OSError(errno.ESRCH, "the rank's parent is gone")
    _prctl(libc, _PR_SET_NO_NEW_PRIVS, 1)
    for capability in range(last_cap + 1):
        _prctl(libc, _PR_CAPBSET_DROP, capability)


def main(argv: list[str]) -> int:
    """Enter the namespaces and run the rank.

    :param argv: ``--parent PID --cwd DIR [--pass-fd N] NAME=FD... -- COMMAND...``.
    :return: The rank's status, ``128+N`` for signal ``N``, or 2 when refused.
    """

    parser = argparse.ArgumentParser(prog="_confine_enter.py")
    parser.add_argument("--parent", required=True, type=int)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--pass-fd", type=int, action="append", default=[])
    parser.add_argument("namespaces", nargs="+")
    separator = argv.index("--") if "--" in argv else len(argv)
    arguments = parser.parse_args(argv[:separator])
    command = argv[separator + 1 :]
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        _prctl(libc, _PR_SET_PDEATHSIG, signal.SIGKILL)
        if os.getppid() != arguments.parent:
            return 2  # the helper is gone already
        descriptors = dict(item.split("=", 1) for item in arguments.namespaces)
        if set(descriptors) != {name for name, _flag in NAMESPACES} or not command:
            raise ValueError("expected the keeper's namespace descriptors and a command")
        with open("/proc/sys/kernel/cap_last_cap", encoding="ascii") as handle:
            last_cap = int(handle.read())
        signal.pthread_sigmask(signal.SIG_BLOCK, _IGNORED_SIGNALS)
        enter({name: int(value) for name, value in descriptors.items()})
        rank = subprocess.Popen(
            command,
            cwd=arguments.cwd,
            pass_fds=tuple(arguments.pass_fd),
            preexec_fn=lambda: rank_preexec(libc, last_cap),  # noqa: PLW1509 - this process is single-threaded
        )
        pending = signal.sigpending()
        for signum in _IGNORED_SIGNALS:
            if signum in pending:
                os.kill(rank.pid, signum)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"httk-workflow rank: refused: cannot enter the launch's namespaces: {exc}", file=sys.stderr, flush=True)
        return 2
    for descriptor in arguments.pass_fd:
        os.close(descriptor)
    status = rank.wait()
    return 128 - status if status < 0 else status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
