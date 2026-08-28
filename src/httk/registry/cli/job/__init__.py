"""Register the top-level job command."""

from httk.core.register import register_cli_command

register_cli_command(
    "job",
    "httk.workflow.workflow_cli:job_command",
    "create and inspect workflow jobs",
)
