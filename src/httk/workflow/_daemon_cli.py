"""Enter the workspace daemon before ordinary workflow parser discovery."""

import argparse
import errno
import json
import os
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the explicit local daemon arguments.

    :param parser: The daemon command parser.
    """

    parser.add_argument("workspace", metavar="WORKSPACE", type=Path, help="local workspace data directory")
    parser.add_argument("--policy", required=True, type=Path, help="protected operator policy file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="check the real sandbox and scheduler requirements")
    mode.add_argument("--initialize", action="store_true", help="approve and initialize a new protected enrollment")
    mode.add_argument("--reload", action="store_true", help="approve and activate updated local configurations")
    mode.add_argument("--export-endpoint", action="store_true", help="print the saved public endpoint and catalog")
    mode.add_argument("--once", action="store_true", help="process one bounded request scan and exit")


def _anchored(path: Path, name: str) -> Path:
    # Anchor lexically instead of resolving, so symlinks stay visible to the ancestry checks. getcwd() is
    # symlink-free, which makes leading '..' exact; a later '..' could climb out of a symlink target.
    if not path.is_absolute():
        base, parts = Path(os.getcwd()), path.parts
        while parts and parts[0] == "..":
            base, parts = base.parent, parts[1:]
        path = base.joinpath(*parts)
    if ".." in path.parts or "\0" in str(path):
        raise ValueError(
            f"{name} path may use '..' only as a leading relative component and must not contain NUL: {path}"
        )
    return path


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

    try:
        workspace = _anchored(arguments.workspace, "workspace")
        policy = _anchored(arguments.policy, "policy")
    except (OSError, ValueError) as exc:
        print(f"httk workspace daemon: {exc}", file=sys.stderr)
        return 2
    try:
        from . import _daemon_setup

        if arguments.initialize:
            _daemon_setup.initialize(workspace, policy)
            return 0
        if arguments.reload:
            _daemon_setup.reload(workspace, policy)
            return 0
        if arguments.export_endpoint:
            print(
                json.dumps(
                    _daemon_setup.export_endpoint(workspace, policy),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
            )
            return 0
        runtime_policy = _daemon_setup.active_policy_path(workspace, policy)
        runtime_workspace = workspace.resolve(strict=True)
    except (OSError, RuntimeError, ValueError, sqlite3.DatabaseError) as exc:
        print(f"httk workspace daemon: {exc}", file=sys.stderr)
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
        str(runtime_workspace),
        "--policy",
        str(runtime_policy),
    ]
    for flag in ("check", "once"):
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
