"""Register the workflow capability with the extensible httk CLI."""

from httk.core import register_cli_command

register_cli_command(
    "workflow",
    "httk.workflow.workflow_cli:command",
    "manage filesystem-native workflows and projects",
)
