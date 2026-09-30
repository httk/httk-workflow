"""Register the top-level collect command."""

from httk.core.register import register_cli_command

register_cli_command(
    "collect",
    "httk.workflow.workflow_cli:collect_command",
    "collect workspaces and recognized calculations into records",
)
