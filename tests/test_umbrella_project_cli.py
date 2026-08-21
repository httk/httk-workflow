"""The core-owned ``httk project`` command is not extended by workflow."""

import json
from pathlib import Path

from httk.core.cli import CLIContext, main
from httk.core.register import known_cli_commands

from httk.workflow.workflow_cli import command


def test_workflow_cli_registration_is_discovered() -> None:
    assert "workflow" in known_cli_commands()


def test_workflow_project_commands_are_not_umbrella_extensions(capsys) -> None:
    assert main(["project", "--help"]) == 0
    output = capsys.readouterr().out
    assert "doctor" not in output and "manifest" not in output


def test_workflow_project_init_does_not_create_a_workspace(tmp_path: Path, capsys) -> None:
    root = tmp_path / "project"
    assert command(["project", "init", "--name", "detached", str(root)], CLIContext("httk", tmp_path)) == 0
    capsys.readouterr()
    assert not (root / ".httk-workflow" / "format.json").exists()


def test_workflow_project_init_batch_is_one_json_array(tmp_path: Path, capsys) -> None:
    roots = [tmp_path / "one", tmp_path / "two"]
    assert command(["project", "init", *(str(root) for root in roots)], CLIContext("httk", tmp_path)) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
