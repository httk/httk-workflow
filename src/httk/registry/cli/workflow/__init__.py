"""Register CLI commands implemented by :mod:`httk.workflow`."""

from httk.core.register import register_cli_command

register_cli_command(
    "workflow",
    "httk.workflow.workflow_cli:workflow_command",
    "manage filesystem-native workflows and projects",
)
