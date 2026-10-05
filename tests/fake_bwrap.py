#!/usr/bin/env python3
"""A Bubblewrap stand-in for plumbing tests on hosts where real Bubblewrap cannot build a sandbox.

It provides NO confinement whatsoever: it parses Bubblewrap's options with their arities, applies only
``--clearenv``, ``--setenv``, ``--unsetenv`` and ``--chdir``, and executes the command after ``--`` in the
host namespaces with every inherited descriptor still open. Mount, namespace and capability options are
accepted and ignored. An unknown option exits with status 2, so a change in the argument vectors the code
builds stays visible.

Use it as ``confine.bwrap`` (or as the ``bwrap`` of a trusted launch) through a small wrapper that runs it
with the test interpreter, e.g. ``#!/bin/sh`` + ``exec <python> <this file> "$@"``.
"""

import os
import sys

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
    "--new-session": 0,
    "--clearenv": 0,
    "--cap-drop": 1,
    "--unsetenv": 1,
    "--tmpfs": 1,
    "--dir": 1,
    "--proc": 1,
    "--dev": 1,
    "--chdir": 1,
    "--setenv": 2,
    "--symlink": 2,
    "--ro-bind-fd": 2,
    "--bind-fd": 2,
    "--ro-bind-data": 2,
    "--ro-bind": 2,
    "--bind": 2,
}


def main(argv: list[str]) -> int:
    """Apply the environment and directory options and execute the command.

    :param argv: The Bubblewrap arguments.
    :return: 2 for an unknown option or a missing command; otherwise the process is replaced.
    """

    environment = dict(os.environ)
    chdir: str | None = None
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
        index += 1 + arity
    command = argv[index + 1 :]
    if not command:
        print("fake_bwrap: no command after --", file=sys.stderr)
        return 2
    if chdir is not None:
        os.chdir(chdir)
    os.execvpe(command[0], command, environment)
    return 127  # pragma: no cover - execvpe does not return


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
