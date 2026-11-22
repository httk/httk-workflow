"""Register CLI commands implemented by :mod:`httk.workflow`."""

from httk.core.register import register_cli_command

register_cli_command(
    "workflow",
    "httk.workflow.workflow_cli:workflow_command",
    "install, inspect and build workflows",
    section="Run",
)

register_cli_command(
    "run",
    "httk.workflow.workflow_cli:run_command",
    "run task managers against a workspace or a campaign's partitions",
    section="Run",
)

register_cli_command(
    "manager",
    "httk.workflow.workflow_cli:manager_command",
    "inspect, read the log of, and cancel daemon-started managers",
    section="Run",
)

register_cli_command(
    "daemon",
    "httk.workflow.workflow_cli:daemon_command",
    "set up and run the confined workspace daemon",
    section="Organize",
)

register_cli_command(
    "monitor",
    "httk.workflow.workflow_cli:monitor_command",
    "inspect and control workspace jobs interactively",
    section="Run",
)

register_cli_command(
    "postprocess",
    "httk.workflow.workflow_cli:postprocess_command",
    "run a curated workflow postprocess script",
    section="Results",
)

register_cli_command(
    "remote",
    "httk.workflow.workflow_cli:remote_command",
    "manage remote connections",
    section="Organize",
)

register_cli_command(
    "launcher",
    "httk.workflow.workflow_cli:launcher_command",
    "manage manager launchers",
    section="Organize",
)

register_cli_command(
    "config",
    "httk.workflow.workflow_cli:config_command",
    "show and change per-user configuration",
    section="Setup",
)

register_cli_command(
    "campaign",
    "httk.workflow.workflow_cli:campaign_command",
    "partition a project's jobs across many workspaces",
    section="Organize",
)

register_cli_command(
    "push",
    "httk.workflow.workflow_cli:push_command",
    "move the ready jobs of a workspace to another",
    section="Run",
)

register_cli_command(
    "pull",
    "httk.workflow.workflow_cli:pull_command",
    "move the succeeded jobs of another workspace here",
    section="Run",
)

register_cli_command(
    "v1",
    "httk.workflow.workflow_cli:v1_command",
    "collect legacy workflow results",
    section="Legacy",
)
