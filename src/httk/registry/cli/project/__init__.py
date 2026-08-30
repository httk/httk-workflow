"""Mount httk-workflow's project verbs onto the core ``httk project`` command."""

from httk.core.register import register_cli_extension

register_cli_extension(
    "project",
    "httk.workflow.workflow_cli:project_extension",
)
