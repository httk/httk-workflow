"""Command-line interface installed as :command:`httk-v1-taskmanager`.

This executable is a thin alias for the ``v1`` group of the canonical
:command:`httk workflow` tree. It declares no arguments and implements no
behaviour of its own: every subcommand is built by the very function that
builds the canonical one, and dispatched to the very handler the canonical one
dispatches to.

===============================  =================================
``httk-v1-taskmanager``          canonical spelling
===============================  =================================
``prepare``                      ``httk workflow v1 prepare``
``submit``                       ``httk workflow v1 submit``
``run``                          ``httk workflow v1 run``
===============================  =================================
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from httk.core.cli import CLIContext

from httk.workflow.workflow_cli import (
    V1_TASKMANAGER_EPILOG,
    HelpFormatter,
    add_v1_prepare_arguments,
    add_v1_run_arguments,
    add_v1_submit_arguments,
    dispatch,
    handle_v1_prepare,
    handle_v1_run,
    handle_v1_submit,
)

#: Every alias, with the arguments it declares and the handler it runs.
_ALIASES = (
    (
        "prepare",
        "turn an instantiated v1 task directory into a v2 payload",
        add_v1_prepare_arguments,
        handle_v1_prepare,
    ),
    ("submit", "prepare and submit an instantiated v1 task", add_v1_submit_arguments, handle_v1_submit),
    ("run", "run only httk-v1 jobs in a v2 workspace", add_v1_run_arguments, handle_v1_run),
)


def _parser(program: str = "httk-v1-taskmanager") -> argparse.ArgumentParser:
    """Build the alias parser out of the canonical tree's own declarations."""

    parser = argparse.ArgumentParser(
        prog=program,
        description="Prepare and execute httk v1 task templates through the v2 workflow engine",
        epilog=V1_TASKMANAGER_EPILOG,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--durable",
        action="store_true",
        help="fsync protocol publications (the default; accepted for compatibility)",
    )
    parser.add_argument(
        "--no-durable",
        action="store_true",
        help="do not fsync protocol publications; a crashed node may then strand markers",
    )
    parser.set_defaults(handler=None, help_parser=parser)
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    for name, summary, declare, handler in _ALIASES:
        alias = subparsers.add_parser(
            name,
            help=summary,
            description=summary.capitalize(),
            formatter_class=HelpFormatter,
        )
        alias.set_defaults(handler=handler)
        declare(alias)
    return parser


def main(argv: Sequence[str] | None = None, *, program: str = "httk-v1-taskmanager") -> int:
    """Run the httk v1 compatibility command."""

    arguments = sys.argv[1:] if argv is None else argv
    return dispatch(_parser(program), arguments, CLIContext("httk", Path.cwd()))


if __name__ == "__main__":
    raise SystemExit(main())
