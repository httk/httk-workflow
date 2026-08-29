"""The interactive :command:`workflow monitor` command."""

import argparse
import math
import sys

from httk.core.cli import CLIContext

from ..monitor.data import WorkspaceView
from ..registry import list_workspaces, resolve_workspace
from ._common import _add_adapter_timeout, _leaf


def handle_monitor(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Open the curses monitor for selected local and remote workspaces."""

    if not math.isfinite(arguments.refresh) or not 0.5 <= arguments.refresh <= 3600:
        raise ValueError("--refresh must be finite and between 0.5 and 3600 seconds")
    if arguments.non_interactive or not sys.stdin.isatty() or not sys.stdout.isatty():
        raise ValueError("workflow monitor requires an interactive TTY (use job list for non-interactive output)")
    try:
        import curses
    except ImportError as exc:
        raise ValueError("workflow monitor requires the stdlib curses module on this platform") from exc

    names = arguments.workspace
    bindings = (
        [resolve_workspace(name, project=context.cwd) for name in names]
        if names
        else list_workspaces(project=context.cwd)
    )
    if not bindings:
        raise ValueError("no workspaces are registered; pass --workspace NAME")
    views = [
        WorkspaceView(binding, context, refresh_interval=arguments.refresh, adapter_timeout=arguments.adapter_timeout)
        for binding in bindings
    ]
    from ..monitor.ui import run_curses

    try:
        return int(curses.wrapper(run_curses, views, arguments.refresh) or 0)
    except curses.error as exc:
        raise ValueError(f"could not initialize the curses terminal: {exc}") from exc


def build_monitor_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the interactive monitor command."""

    parser = _leaf(
        subparsers,
        "monitor",
        summary="inspect and control workspace jobs interactively",
        description="Open a curses monitor over local and remote workflow workspaces",
        handler=handle_monitor,
    )
    parser.add_argument(
        "--workspace",
        action="append",
        metavar="NAME|REMOTE:NAME",
        help="workspace to show (repeatable; default: all registered local workspaces)",
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="refresh interval for visible data and counts (default: 5)",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="refuse the command explicitly when no curses terminal is available",
    )
    _add_adapter_timeout(parser)


__all__ = ["build_monitor_parser", "handle_monitor"]
