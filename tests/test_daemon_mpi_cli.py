"""Verify the explicit MPI wrapper CLI and distributed manager capacity."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from httk.core.cli import CLIContext

from httk.workflow._daemon_payload import _manager_command
from httk.workflow._daemon_policy import MPIProfile, Profile
from httk.workflow.workflow_cli import build_parser


def test_mpi_cli_preserves_application_argv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import _daemon_mpi_client

    observed: list[list[str]] = []

    def run(argv: list[str]) -> int:
        observed.append(argv)
        return 17

    monkeypatch.setattr(_daemon_mpi_client, "run", run)
    context = CLIContext("httk", tmp_path)
    parser = build_parser("httk workflow", context)
    arguments = parser.parse_args(["mpi", "run", "--", "/opt/app with spaces", "--flag", "$(touch outside)"])
    assert arguments.handler(arguments, context) == 17
    assert observed == [["/opt/app with spaces", "--flag", "$(touch outside)"]]


@pytest.mark.parametrize("tail", [[], ["--"]])
def test_mpi_cli_requires_application(tmp_path: Path, tail: list[str]) -> None:
    context = CLIContext("httk", tmp_path)
    arguments = build_parser("httk workflow", context).parse_args(["mpi", "run", *tail])
    with pytest.raises(ValueError, match="requires an application"):
        arguments.handler(arguments, context)


def test_mpi_manager_adds_fixed_geometry() -> None:
    policy: Any = SimpleNamespace(python=Path("/trusted/python"))
    profile = Profile("mpi", cpus=2, memory_mb=512, time_minutes=10, mpi=MPIProfile(nodes=2, ranks=8))
    command = _manager_command(policy, profile, 16, 1024)
    assert command[command.index("--workers") + 1] == "1"
    resources = {
        command[index + 1]: command[index + 2] for index, value in enumerate(command) if value == "--worker-resource"
    }
    assert (resources["nodes"], resources["mpi_ranks"]) == ("2", "8")


def test_mpi_cli_reports_uncertain_eof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import _daemon_mpi_client

    def run(_argv: list[str]) -> int:
        raise EOFError("truncated frame")

    monkeypatch.setattr(_daemon_mpi_client, "run", run)
    context = CLIContext("httk", tmp_path)
    arguments = build_parser("httk workflow", context).parse_args(["mpi", "run", "--", "/bin/true"])
    with pytest.raises(RuntimeError, match="without a confirmed result"):
        arguments.handler(arguments, context)
