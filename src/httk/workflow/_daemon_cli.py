"""Enter the workspace daemon before ordinary workflow parser discovery."""

import argparse
import errno
import os
import sys
from collections.abc import Sequence
from pathlib import Path


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the explicit local daemon arguments.

    :param parser: The daemon command parser.
    """

    parser.add_argument("workspace", metavar="WORKSPACE", type=Path, help="absolute local workspace data directory")
    parser.add_argument("--policy", required=True, type=Path, help="absolute protected operator policy file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="check the real sandbox and scheduler requirements")
    mode.add_argument("--initialize", action="store_true", help="initialize a new protected enrollment ledger and exit")
    mode.add_argument("--once", action="store_true", help="process one bounded request scan and exit")


def _close_inherited() -> None:
    for name in os.listdir("/proc/self/fd"):
        descriptor = int(name)
        if descriptor > 2:
            try:
                os.close(descriptor)
            except OSError as exc:
                if exc.errno != errno.EBADF:
                    raise


def launch(arguments: argparse.Namespace) -> int:
    """Replace the CLI process with the isolated installed bootstrap.

    :param arguments: Parsed local daemon arguments.
    :return: A failure exit status if the bootstrap cannot start.
    """

    for path in (arguments.workspace, arguments.policy):
        if not path.is_absolute() or ".." in path.parts or "\0" in str(path):
            print("httk workspace daemon: explicit absolute local paths are required", file=sys.stderr)
            return 2
    bootstrap = Path(__file__).with_name("_daemon_bootstrap.py").resolve()
    executable = sys.executable
    argv = [
        executable,
        "-I",
        "-S",
        str(bootstrap),
        "--mode",
        "broker",
        "--workspace",
        str(arguments.workspace),
        "--policy",
        str(arguments.policy),
    ]
    for flag in ("check", "initialize", "once"):
        if getattr(arguments, flag):
            argv.append("--" + flag)
    try:
        os.chdir("/")
        descriptor = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.dup2(descriptor, 0, inheritable=True)
            os.set_inheritable(0, True)
        finally:
            if descriptor != 0:
                os.close(descriptor)
        _close_inherited()
        os.execve(executable, argv, {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"})
    except OSError:
        print("httk workspace daemon: isolated bootstrap could not start", file=sys.stderr)
        return 2


def command(argv: Sequence[str], *, program: str) -> int:
    """Parse and launch the daemon without constructing the workflow tree.

    :param argv: Arguments following the daemon subcommand.
    :param program: Display name for help and usage errors.
    :return: An exit status for help or a startup failure.
    """

    parser = argparse.ArgumentParser(prog=program, description="Run the confined workspace command daemon")
    add_arguments(parser)
    try:
        arguments = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    return launch(arguments)
