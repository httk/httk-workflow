"""Register the workflow capability with the extensible httk CLI.

Two things register here during core plugin discovery: the top-level
``httk workflow`` command, and the workflow extensions of the umbrella
``httk project`` command that *httk-core* owns. Both use lazy
``"module:callable"`` references, so nothing heavy is imported until the
relevant command actually runs; ``httk project manifest`` and ``httk workflow
project manifest`` therefore drive the very same handlers.
"""

from httk.core import register_cli_command
from httk.core.project.cli import (
    register_project_show_section,
    register_project_subcommand,
)

register_cli_command(
    "workflow",
    "httk.workflow.workflow_cli:command",
    "manage filesystem-native workflows and projects",
)

register_project_subcommand(
    "manifest",
    "httk.workflow.workflow_cli:build_umbrella_manifest_parser",
    summary="create and verify the signed project manifest",
    description="Create and verify the deterministic signed manifest of one project",
)
register_project_subcommand(
    "doctor",
    "httk.workflow.workflow_cli:build_umbrella_doctor_parser",
    "httk.workflow.workflow_cli:handle_project_doctor",
    summary="check, and optionally repair, this project",
    description="Check one project for the conditions that quietly break it later",
)
register_project_show_section("workflow", "httk.workflow.hygiene:workflow_show_section")
