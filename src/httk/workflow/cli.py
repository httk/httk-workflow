"""Run the command-line interface installed as :command:`httk-taskmanager`.

This executable is a thin alias for five leaves of the canonical
:command:`httk workflow` tree. It declares no arguments and implements no
behaviour of its own: every subcommand is built by the very function that
builds the canonical one, and dispatched to the very handler the canonical one
dispatches to, so the two spellings cannot drift apart.

============================  ==================================
``httk-taskmanager``          canonical spelling
============================  ==================================
``init``                      ``httk workflow workspace init``
``submit``                    ``httk workflow job submit``
``run``                       ``httk workflow manager run``
``status``                    ``httk workflow workspace status``
``request``                   ``httk workflow job request``
============================  ==================================
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from httk.core.cli import CLIContext

from .workflow_cli import (
    TASKMANAGER_EPILOG,
    HelpFormatter,
    add_job_request_arguments,
    add_job_submit_arguments,
    add_manager_run_arguments,
    add_workspace_init_arguments,
    add_workspace_status_arguments,
    dispatch,
    handle_job_request,
    handle_job_submit,
    handle_manager_run,
    handle_workspace_init,
    handle_workspace_status,
)

#: Every alias, with the arguments it declares and the handler it runs.
_ALIASES = (
    ("init", "initialize a workflow workspace", add_workspace_init_arguments, handle_workspace_init),
    ("submit", "submit a complete payload directory", add_job_submit_arguments, handle_job_submit),
    ("run", "run the task manager", add_manager_run_arguments, handle_manager_run),
    ("status", "summarize authoritative markers", add_workspace_status_arguments, handle_workspace_status),
    ("request", "publish an operator request", add_job_request_arguments, handle_job_request),
)


def _parser(program: str = "httk-taskmanager") -> argparse.ArgumentParser:
    """Build the alias parser out of the canonical tree's own declarations."""

    parser = argparse.ArgumentParser(
        prog=program,
        description="Manage httk filesystem workflows",
        epilog=TASKMANAGER_EPILOG,
        formatter_class=HelpFormatter,
    )
    # Accepted before the subcommand, as this executable has always accepted
    # them; the canonical tree carries the same switches on the leaves, where
    # they suppress their defaults so that a value given here survives.
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


def main(argv: Sequence[str] | None = None, *, program: str = "httk-taskmanager") -> int:
    """Run the command-line interface.

    :param argv: Command-line arguments, or the process arguments when absent.
    :param program: Program name shown in parser output.
    :return: Process exit status.
    """

    arguments = sys.argv[1:] if argv is None else argv
    return dispatch(_parser(program), arguments, CLIContext("httk", Path.cwd()))


if __name__ == "__main__":
    raise SystemExit(main())
