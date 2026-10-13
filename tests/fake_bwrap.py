#!/usr/bin/env python3
"""A Bubblewrap stand-in for plumbing tests on hosts where real Bubblewrap cannot build a sandbox.

It provides NO confinement whatsoever: it parses Bubblewrap's options with their arities, applies only
``--clearenv``, ``--setenv``, ``--unsetenv`` and ``--chdir``, and executes the command after ``--`` in the
host namespaces with every inherited descriptor still open. Mount, namespace and capability options are
accepted and ignored. An unknown option exits with status 2, so a change in the argument vectors the code
builds stays visible.

Use it as ``confine.bwrap`` (or as the ``bwrap`` of a trusted launch) through a small wrapper that runs it
with the test interpreter, e.g. ``#!/bin/sh`` + ``exec <python> <this file> "$@"``.

A keeper sandbox (``--info-fd``) needs namespaces of its own, which the rank helpers then enter. Given
``--fake-namespaces BWRAP`` before the options, the stand-in runs the command through that real Bubblewrap
with fresh user, PID, IPC and UTS namespaces over the host's whole filesystem (``--dev-bind / /``), still
without any mount confinement. Without it, it writes its own pid to the info descriptor and runs the command
in the host namespaces, which the rank helpers refuse.
"""

import functools
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Options and the number of values each takes.
_ARITIES = {
    "--unshare-user": 0,
    "--unshare-pid": 0,
    "--unshare-ipc": 0,
    "--unshare-uts": 0,
    "--unshare-net": 0,
    "--unshare-cgroup": 0,
    "--unshare-all": 0,
    "--disable-userns": 0,
    "--assert-userns-disabled": 0,
    "--die-with-parent": 0,
    "--as-pid-1": 0,
    "--new-session": 0,
    "--clearenv": 0,
    "--cap-drop": 1,
    "--unsetenv": 1,
    "--tmpfs": 1,
    "--remount-ro": 1,
    "--dir": 1,
    "--proc": 1,
    "--dev": 1,
    "--chdir": 1,
    "--info-fd": 1,
    "--setenv": 2,
    "--symlink": 2,
    "--ro-bind-fd": 2,
    "--bind-fd": 2,
    "--ro-bind-data": 2,
    "--ro-bind": 2,
    "--bind": 2,
}


@functools.cache
def namespace_bwrap() -> str | None:
    """Return a real Bubblewrap that can create the keeper's namespaces on this host, or ``None``.

    :return: The Bubblewrap path.
    """

    bwrap = shutil.which("bwrap")
    if bwrap is None:
        return None
    options = ["--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts", "--dev-bind", "/", "/"]
    try:
        result = subprocess.run([bwrap, *options, "--", "true"], capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return bwrap if result.returncode == 0 else None


def write_wrapper(path: Path, *, namespaces: bool = True) -> Path:
    """Write an executable ``bwrap`` wrapper that runs this stand-in, with keeper namespaces when available.

    :param path: The wrapper to write.
    :param namespaces: Whether to use :func:`namespace_bwrap` for keeper sandboxes when it is available.
    :return: *path*.
    """

    real = namespace_bwrap() if namespaces else None
    option = "" if real is None else f' --fake-namespaces "{real}"'
    path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{__file__}"{option} "$@"\n', encoding="utf-8")
    path.chmod(0o755)
    return path


def require_namespaces() -> None:
    """Skip the calling test when rank helpers cannot get keeper namespaces here (fail where they are required)."""

    import pytest

    if namespace_bwrap() is None:
        reason = "rank helpers need a Bubblewrap that can create user, PID, IPC and UTS namespaces"
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail(reason)
        pytest.skip(reason)


def main(argv: list[str]) -> int:
    """Apply the environment and directory options and execute the command.

    :param argv: The Bubblewrap arguments.
    :return: 2 for an unknown option or a missing command; otherwise the process is replaced.
    """

    if argv == ["--help"]:
        print(" ".join(_ARITIES))
        return 0
    namespaces: str | None = None
    if argv[:1] == ["--fake-namespaces"]:
        namespaces, argv = argv[1], argv[2:]
    environment = dict(os.environ)
    chdir: str | None = None
    info_fd: int | None = None
    index = 0
    while index < len(argv) and argv[index] != "--":
        option = argv[index]
        arity = _ARITIES.get(option)
        if arity is None or index + arity >= len(argv):
            print(f"fake_bwrap: unknown or incomplete option {option!r}", file=sys.stderr)
            return 2
        values = argv[index + 1 : index + 1 + arity]
        if option == "--clearenv":
            environment.clear()
        elif option == "--setenv":
            environment[values[0]] = values[1]
        elif option == "--unsetenv":
            environment.pop(values[0], None)
        elif option == "--chdir":
            chdir = values[0]
        elif option == "--info-fd":
            info_fd = int(values[0])
        index += 1 + arity
    command = argv[index + 1 :]
    if not command:
        print("fake_bwrap: no command after --", file=sys.stderr)
        return 2
    if chdir is not None:
        os.chdir(chdir)
    if info_fd is not None and namespaces is not None:
        real = [namespaces, "--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts", "--die-with-parent"]
        real += ["--dev-bind", "/", "/", "--info-fd", str(info_fd), "--chdir", os.getcwd(), "--", *command]
        os.execve(namespaces, real, environment)
    if info_fd is not None:
        os.write(info_fd, json.dumps({"child-pid": os.getpid()}).encode())
        os.close(info_fd)
    os.execvpe(command[0], command, environment)
    return 127  # pragma: no cover - execvpe does not return


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
