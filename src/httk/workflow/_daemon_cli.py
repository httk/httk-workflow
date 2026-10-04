"""Enter the workspace daemon before ordinary workflow parser discovery."""

import argparse
import errno
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
    parser.add_argument("--exchange", type=Path, help="client exchange directory, a sibling of the workspace")
    parser.add_argument(
        "--launcher", action="append", metavar="NAME", help="approve a global daemon launcher (repeatable)"
    )
    parser.add_argument("--authorize", action="append", metavar="KEY", help="authorize an Ed25519 key (repeatable)")
    parser.add_argument("--state", type=Path, help="broker state directory when not the default")
    parser.add_argument("--snapshots", type=Path, help="runtime snapshot directory when not <state>.snapshots")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="check the real sandbox and scheduler requirements")
    mode.add_argument("--initialize", action="store_true", help="approve and initialize a new enrollment")
    mode.add_argument("--reload", action="store_true", help="approve and activate updated launchers or keys")
    mode.add_argument("--once", action="store_true", help="process one bounded request scan and exit")
    parser.add_argument(
        "--force",
        action="store_true",
        help="approve CPU, memory and time requests above the built-in sanity limits",
    )


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


def _usage_refusal(arguments: argparse.Namespace) -> str | None:
    setup = arguments.initialize or arguments.reload
    if arguments.force and not setup:
        return "--force applies only to --initialize and --reload"
    if arguments.initialize and (arguments.exchange is None or not arguments.launcher or not arguments.authorize):
        return "--initialize requires --exchange, at least one --launcher and at least one --authorize"
    if arguments.reload and arguments.exchange is not None:
        return "--reload cannot change --exchange; a new enrollment is required"
    if not setup and (arguments.exchange is not None or arguments.launcher or arguments.authorize):
        return "--exchange, --launcher and --authorize apply only to --initialize and --reload"
    return None


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

    refusal = _usage_refusal(arguments)
    if refusal is not None:
        print(f"httk workspace daemon: {refusal}", file=sys.stderr)
        return 2
    try:
        workspace = _anchored(arguments.workspace, "workspace")
        paths = {
            name: None if getattr(arguments, name) is None else _anchored(getattr(arguments, name), name)
            for name in ("exchange", "state", "snapshots")
        }
    except (OSError, ValueError) as exc:
        print(f"httk workspace daemon: {exc}", file=sys.stderr)
        return 2
    try:
        from . import _daemon_setup
        from ._daemon_policy import load_policy

        if arguments.initialize or arguments.reload:
            if arguments.initialize:
                assert paths["exchange"] is not None
                snapshot = _daemon_setup.initialize(
                    workspace,
                    exchange=paths["exchange"],
                    launchers=arguments.launcher,
                    authorized_keys=arguments.authorize,
                    state=paths["state"],
                    snapshots=paths["snapshots"],
                    force=arguments.force,
                )
            else:
                snapshot = _daemon_setup.reload(
                    workspace,
                    launchers=arguments.launcher,
                    authorized_keys=arguments.authorize,
                    state=paths["state"],
                    snapshots=paths["snapshots"],
                    force=arguments.force,
                )
            approved = load_policy(snapshot)
            for profile in approved.profiles:
                print(f"launcher {profile.name}")
            for key in approved.authorized_keys:
                print(f"authorized {key}")
            return 0
        runtime_policy = _daemon_setup.active_policy_path(workspace, state=paths["state"], snapshots=paths["snapshots"])
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
