"""The core-owned ``httk project`` command is extended by httk-workflow."""

from httk.core.cli import main
from httk.core.register import known_cli_commands

import httk.workflow.workflow_cli  # noqa: F401  (importing registers the workflow CLI commands)


def test_workflow_cli_registration_is_discovered() -> None:
    assert {"job", "workflow", "workspace"} <= set(known_cli_commands())


def test_workflow_project_verbs_extend_the_umbrella(capsys) -> None:
    assert main(["project", "--help"]) == 0
    output = capsys.readouterr().out
    for verb in ("doctor", "manifest", "seal", "unseal"):
        assert verb in output
