"""Enter the workspace daemon before ordinary workflow parser discovery."""

import argparse
import errno
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from ._daemon_bootstrap import _operator_environment

_MODES = {
    "init": "approve global slurm launchers and initialize a new enrollment",
    "configure": "change the daemon configuration, activated when the daemon next starts",
    "show": "describe the enrollment and its daemon configuration",
    "check": "activate the configuration and check the real sandbox and scheduler requirements",
    "run": "activate the configuration and serve the exchange",
}
_KEYS_HELP = (
    "keys: launchers and authorized_keys (lists; --set takes a comma-separated value), bwrap, python, sbatch, "
    "squeue, scancel (executables), sacct and slurm_conf (optional paths; an empty value unsets), "
    "max_submissions, force (true or false: approve CPU, memory and time above the sanity limits)"
)


def _change(operation: str) -> Callable[[str], tuple[str, str]]:
    return lambda item: (operation, item)


def add_subcommands(modes: "argparse._SubParsersAction[argparse.ArgumentParser]", **defaults: object) -> None:
    """Declare the daemon subcommands ``init``, ``configure``, ``show``, ``check`` and ``run``.

    :param modes: The subcommand action of the daemon command parser.
    :param **defaults: Extra parser defaults set on every subcommand, such as a handler.
    """

    for mode, summary in _MODES.items():
        parser = modes.add_parser(mode, help=summary, description=summary[0].upper() + summary[1:])
        parser.set_defaults(daemon_mode=mode, **defaults)
        parser.add_argument("workspace", metavar="WORKSPACE", type=Path, help="local workspace data directory")
        if mode == "init":
            parser.add_argument(
                "--exchange", type=Path, required=True, help="client exchange directory, a sibling of the workspace"
            )
        if mode in ("init", "configure"):
            extra = "; init also takes cluster and scontrol" if mode == "init" else ""
            parser.add_argument(
                "--set",
                dest="changes",
                action="append",
                type=_change("set"),
                metavar="KEY=VALUE",
                help=f"set one configuration value (repeatable); {_KEYS_HELP}{extra}",
            )
            parser.add_argument(
                "--add",
                dest="changes",
                action="append",
                type=_change("add"),
                metavar="KEY=VALUE",
                help="add one launcher name or authorized Ed25519 key (repeatable)",
            )
        if mode == "configure":
            parser.add_argument(
                "--remove",
                dest="changes",
                action="append",
                type=_change("remove"),
                metavar="KEY=VALUE",
                help="remove one launcher name or authorized key (repeatable)",
            )
        if mode == "show":
            parser.add_argument("--json", action="store_true", help="print the description as one JSON document")
        if mode == "run":
            parser.add_argument("--once", action="store_true", help="process one bounded request scan and exit")
        parser.add_argument("--state", type=Path, help="broker state directory when not the default")
        snapshots = "when not <state>.snapshots" if mode == "init" else "of the enrollment, checked when given"
        parser.add_argument("--snapshots", type=Path, help=f"runtime snapshot directory {snapshots}")


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


_CLEAN_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}


def _launch_environment() -> dict[str, str]:
    """Return the filtered trusted operator environment, with the clean defaults as fallbacks."""

    return _CLEAN_ENV | _operator_environment(os.environ)


def _bootstrap_argv(workspace: Path, policy: Path, flag: str | None) -> list[str]:
    """Build the isolated bootstrap command line for the active policy.

    :param workspace: The anchored workspace path.
    :param policy: The active runtime snapshot.
    :param flag: An extra run-mode flag such as ``--check``, or None.
    :return: The argv that starts the broker bootstrap.
    """

    argv = [
        sys.executable,
        "-I",
        "-S",
        str(Path(__file__).with_name("_daemon_bootstrap.py").resolve()),
        "--workspace",
        str(workspace.resolve(strict=True)),
        "--policy",
        str(policy),
    ]
    return argv if flag is None else [*argv, flag]


def _check_after_setup(workspace: Path, policy: Path) -> int:
    # No skip option by design: a misconfiguration must surface at setup time. Setup is not rolled back.
    try:
        argv = _bootstrap_argv(workspace, policy, "--check")
        code = subprocess.run(
            argv, stdin=subprocess.DEVNULL, cwd="/", env=_launch_environment(), close_fds=True, check=False
        ).returncode
    except (OSError, RuntimeError, ValueError, sqlite3.DatabaseError) as exc:
        print(f"httk workspace daemon: {exc}", file=sys.stderr)
        code = 1
    if code == 0:
        print("sandbox check passed")
        return 0
    print(
        "httk workspace daemon: the enrollment was saved, but the sandbox check failed (see above); fix the "
        "launchers or the daemon configuration ('httk workspace daemon configure') and run "
        "'httk workspace daemon check'",
        file=sys.stderr,
    )
    return 2


def _render(description: dict[str, object]) -> str:
    """Render a daemon description: the fixed enrollment, then the configuration as ``KEY=VALUE`` lines."""

    configuration = description["configuration"]
    assert isinstance(configuration, dict)
    lines = [f"{key}: {value}" for key, value in description.items() if key != "configuration"]
    for key, value in configuration.items():
        if isinstance(value, list):
            value = ",".join(str(item) for item in value)
        elif isinstance(value, bool):
            value = "true" if value else "false"
        lines.append(f"{key}={'' if value is None else value}")
    return "\n".join(lines)


def _local(
    arguments: argparse.Namespace, workspace: Path, state: Path | None, snapshots: Path | None
) -> list[str] | int:
    """Run a local mode, returning an exit status, or the bootstrap argv for ``check`` and ``run``."""

    from . import _daemon_setup

    mode = arguments.daemon_mode
    if mode == "init":
        snapshot = _daemon_setup.initialize(
            workspace,
            exchange=_anchored(arguments.exchange, "exchange"),
            changes=arguments.changes or (),
            state=state,
            snapshots=snapshots,
            report=print,
        )
        print(_render(_daemon_setup.describe(workspace, state=state, snapshots=snapshots)))
        return _check_after_setup(workspace, snapshot)
    if mode == "configure":
        print(_render(_daemon_setup.configure(workspace, arguments.changes or (), state=state, snapshots=snapshots)))
        print(f"the configuration takes effect when the daemon next starts: httk workspace daemon run {workspace}")
        return 0
    if mode == "show":
        description = _daemon_setup.describe(workspace, state=state, snapshots=snapshots)
        print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render(description))
        return 0
    snapshot, changed = _daemon_setup.activate(workspace, state=state, snapshots=snapshots)
    if changed is not None:
        print(f"activated the changed daemon configuration; changed launchers: {', '.join(changed) or 'none'}")
        sys.stdout.flush()
    flag = "--check" if mode == "check" else "--once" if arguments.once else None
    return _bootstrap_argv(workspace, snapshot, flag)


def launch(arguments: argparse.Namespace) -> int:
    """Run one daemon subcommand; ``check`` and ``run`` replace the CLI process with the isolated bootstrap.

    :param arguments: Parsed daemon subcommand arguments.
    :return: The exit status of a local subcommand, or a failure status if the bootstrap cannot start.
    """

    try:
        workspace = _anchored(arguments.workspace, "workspace")
        state = None if arguments.state is None else _anchored(arguments.state, "state")
        snapshots = None if arguments.snapshots is None else _anchored(arguments.snapshots, "snapshots")
        result = _local(arguments, workspace, state, snapshots)
    except (OSError, RuntimeError, ValueError, sqlite3.DatabaseError) as exc:
        print(f"httk workspace daemon: {exc}", file=sys.stderr)
        return 2
    if isinstance(result, int):
        return result
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
        os.execve(sys.executable, result, _launch_environment())
    except OSError:
        print("httk workspace daemon: isolated bootstrap could not start", file=sys.stderr)
        return 2


def command(argv: Sequence[str], *, program: str) -> int:
    """Parse and run one daemon subcommand without constructing the workflow tree.

    :param argv: Arguments following the daemon command.
    :param program: Display name for help and usage errors.
    :return: An exit status for help, a local subcommand or a startup failure.
    """

    parser = argparse.ArgumentParser(prog=program, description="Set up and run the confined workspace command daemon")
    add_subcommands(parser.add_subparsers(metavar="COMMAND"))
    try:
        arguments = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    if getattr(arguments, "daemon_mode", None) is None:
        parser.print_help()
        return 0
    return launch(arguments)
