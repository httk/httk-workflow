"""Register CLI commands implemented by :mod:`httk.workflow`."""

from httk.core.register import register_cli_command

register_cli_command(
    "workflow",
    "httk.workflow.workflow_cli:workflow_command",
    "install, inspect and build workflows",
)

register_cli_command(
    "run",
    "httk.workflow.workflow_cli:run_command",
    "run task managers against a workspace",
)

register_cli_command(
    "monitor",
    "httk.workflow.workflow_cli:monitor_command",
    "inspect and control workspace jobs interactively",
)

register_cli_command(
    "postprocess",
    "httk.workflow.workflow_cli:postprocess_command",
    "run a curated workflow postprocess script",
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
    "push",
    "httk.workflow.workflow_cli:push_command",
    "move the ready jobs of a workspace to another",
)

register_cli_command(
    "pull",
    "httk.workflow.workflow_cli:pull_command",
    "move the succeeded jobs of another workspace here",
)

register_cli_command(
    "v1",
    "httk.workflow.workflow_cli:v1_command",
    "collect legacy workflow results",
)
