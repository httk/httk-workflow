"""Register the top-level workspace command."""

from httk.core.register import register_cli_command

register_cli_command(
    "workspace",
    "httk.workflow.workflow_cli:workspace_command",
    "manage workflow workspaces",
)
