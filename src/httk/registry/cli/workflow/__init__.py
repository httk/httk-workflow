"""Register CLI commands implemented by :mod:`httk.workflow`."""

from httk.core.register import register_cli_command

register_cli_command(
    "workflow",
    "httk.workflow.workflow_cli:workflow_command",
    "install, inspect, build and run workflows",
)

register_cli_command(
    "manager",
    "httk.workflow.workflow_cli:manager_command",
    "run workflow managers",
)

register_cli_command(
    "remote",
    "httk.workflow.workflow_cli:remote_command",
    "manage remote connections",
)

register_cli_command(
    "launcher",
    "httk.workflow.workflow_cli:launcher_command",
    "manage manager launchers",
)

register_cli_command(
    "config",
    "httk.workflow.workflow_cli:config_command",
    "show and change per-user configuration",
)

register_cli_command(
    "campaign",
    "httk.workflow.workflow_cli:campaign_command",
    "configure and run partitioned campaigns",
)

register_cli_command(
    "seal",
    "httk.workflow.workflow_cli:seal_command",
    "verify sealed projects, workspaces and jobs",
)

register_cli_command(
    "v1",
    "httk.workflow.workflow_cli:v1_command",
    "collect legacy workflow results",
)

register_cli_command(
    "transfer",
    "httk.workflow.workflow_cli:transfer_command",
    "inspect and resolve workspace transfers (unavailable in this version)",
)
