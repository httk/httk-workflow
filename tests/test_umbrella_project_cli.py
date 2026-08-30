"""The core-owned ``httk project`` command is not extended by workflow."""

from httk.core.cli import main
from httk.core.register import known_cli_commands

import httk.workflow.workflow_cli  # noqa: F401  (importing registers the workflow CLI commands)


def test_workflow_cli_registration_is_discovered() -> None:
    assert {"job", "workflow", "workspace"} <= set(known_cli_commands())


def test_workflow_project_commands_are_not_umbrella_extensions(capsys) -> None:
    assert main(["project", "--help"]) == 0
    output = capsys.readouterr().out
    assert "doctor" not in output and "manifest" not in output
