"""Launch an application through the confined allocation's MPI service."""

import argparse

from httk.core.cli import CLIContext

from ._common import _leaf


def handle_mpi_run(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run one application using the allocation's fixed MPI profile."""

    from .._daemon_mpi_client import run

    argv = list(arguments.application)
    if argv[:1] == ["--"]:
        argv = argv[1:]
    if not argv:
        raise ValueError("mpi run requires an application after --")
    try:
        return run(argv)
    except EOFError as exc:
        raise RuntimeError("MPI connection ended without a confirmed result; the request was not retried") from exc


def build_mpi_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the MPI application wrapper command."""

    parser = subparsers.add_parser("mpi", help="run applications in a confined MPI allocation")
    parser.set_defaults(handler=None, help_parser=parser)
    commands = parser.add_subparsers(metavar="COMMAND")
    run_parser = _leaf(
        commands,
        "run",
        summary="run an application with the fixed MPI profile",
        description="Stream one MPI application through the current daemon allocation",
        handler=handle_mpi_run,
    )
    run_parser.add_argument(
        "application",
        nargs=argparse.REMAINDER,
        metavar="-- APPLICATION ARG...",
        help="application executable and arguments passed to every MPI rank",
    )
