"""Assemble the top-level workflow module commands.

This package assembles the existing command-group handlers into one parser and
keeps the supported ``httk.workflow.workflow_cli`` import surface intact. The
group implementations live in private modules; consumers continue to resolve
the command and its handlers from this module.
"""

import argparse
from collections.abc import Sequence

from httk.core.cli import CLIContext

from ..adapters import (
    REMOTE_MANAGER_COMMAND,
    REMOTE_STATUS_COMMAND,
    REMOTE_WORKSPACE_CHECK_COMMAND,
    REMOTE_WORKSPACE_DELETE_COMMAND,
    REMOTE_WORKSPACE_GC_COMMAND,
    REMOTE_WORKSPACE_INIT_COMMAND,
    REMOTE_WORKSPACE_LIST_COMMAND,
    REMOTE_WORKSPACE_MOVE_COMMAND,
    REMOTE_WORKSPACE_REPAIR_COMMAND,
    REMOTE_WORKSPACE_SET_COMMAND,
    REMOTE_WORKSPACE_SHOW_COMMAND,
    REMOTE_WORKSPACE_UNSET_COMMAND,
)
from ._build import build_build_parser, handle_build
from ._campaign import (
    build_campaign_parser,
    handle_campaign_collect,
    handle_campaign_init,
    handle_campaign_show,
    handle_campaign_start_managers,
    handle_campaign_submit,
)
from ._common import remote_workspace_output
from ._compat import add_v1_collect_arguments, build_v1_parser, handle_v1_collect
from ._describe import (
    build_install_parser,
    build_list_parser,
    build_show_parser,
    handle_workflow_fetch,
    handle_workflow_forget,
    handle_workflow_install,
    handle_workflow_list,
    handle_workflow_show,
    handle_workflow_uninstall,
)
from ._job import (
    build_job_parser,
    ensure_identity_key,
    handle_job_control,
    handle_job_debug,
    handle_job_delete,
    handle_job_list,
    handle_job_log,
    handle_job_new,
    handle_job_seal,
    handle_job_set,
    handle_job_show,
    handle_job_submit,
    handle_job_unseal,
    handle_job_why,
    publish_job_requests,
    request_remote_job_result,
)
from ._launcher import (
    build_launcher_parser,
    handle_launcher_add,
    handle_launcher_check,
    handle_launcher_list,
    handle_launcher_remove,
    handle_launcher_set,
    handle_launcher_show,
    handle_launcher_unset,
)
from ._manager import (
    add_run_arguments,
    build_run_parser,
    handle_manager_run,
    launch_workspace_managers,
    submit_remote_manager_result,
)
from ._monitor import build_monitor_parser, handle_monitor
from ._postprocess import build_postprocess_parser, handle_postprocess
from ._precheck import handle_precheck
from ._project import (
    build_config_parser,
    handle_config_import_v1,
    handle_config_set,
    handle_config_show,
    handle_config_unset,
)
from ._transfer import (
    build_push_pull_parser,
    build_remote_parser,
    build_transfer_parser,
    handle_remote_add,
    handle_remote_check,
    handle_remote_import_v1,
    handle_remote_list,
    handle_remote_remove,
    handle_remote_set,
    handle_remote_show,
    handle_remote_unset,
    handle_transfer,
)
from ._workspace import (
    add_workspace_init_arguments,
    add_workspace_status_arguments,
    build_workspace_parser,
    handle_workspace_check,
    handle_workspace_delete,
    handle_workspace_forget,
    handle_workspace_gc,
    handle_workspace_init,
    handle_workspace_list,
    handle_workspace_seal,
    handle_workspace_set,
    handle_workspace_show,
    handle_workspace_status,
    handle_workspace_unseal,
    handle_workspace_unset,
)

__all__ = [
    "REMOTE_MANAGER_COMMAND",
    "REMOTE_STATUS_COMMAND",
    "REMOTE_WORKSPACE_CHECK_COMMAND",
    "REMOTE_WORKSPACE_DELETE_COMMAND",
    "REMOTE_WORKSPACE_GC_COMMAND",
    "REMOTE_WORKSPACE_INIT_COMMAND",
    "REMOTE_WORKSPACE_LIST_COMMAND",
    "REMOTE_WORKSPACE_MOVE_COMMAND",
    "REMOTE_WORKSPACE_REPAIR_COMMAND",
    "REMOTE_WORKSPACE_SET_COMMAND",
    "REMOTE_WORKSPACE_SHOW_COMMAND",
    "REMOTE_WORKSPACE_UNSET_COMMAND",
    "add_run_arguments",
    "add_v1_collect_arguments",
    "add_workspace_init_arguments",
    "add_workspace_status_arguments",
    "build_build_parser",
    "build_campaign_parser",
    "build_config_parser",
    "build_install_parser",
    "build_job_parser",
    "build_launcher_parser",
    "build_list_parser",
    "build_monitor_parser",
    "build_parser",
    "build_postprocess_parser",
    "build_push_pull_parser",
    "build_remote_parser",
    "build_run_parser",
    "build_show_parser",
    "build_transfer_parser",
    "build_v1_parser",
    "build_workspace_parser",
    "campaign_command",
    "collect_command",
    "command",
    "config_command",
    "daemon_command",
    "dispatch",
    "ensure_identity_key",
    "handle_build",
    "handle_campaign_collect",
    "handle_campaign_init",
    "handle_campaign_show",
    "handle_campaign_start_managers",
    "handle_campaign_submit",
    "handle_config_import_v1",
    "handle_config_set",
    "handle_config_show",
    "handle_config_unset",
    "handle_job_control",
    "handle_job_debug",
    "handle_job_delete",
    "handle_job_list",
    "handle_job_log",
    "handle_job_new",
    "handle_job_seal",
    "handle_job_set",
    "handle_job_show",
    "handle_job_submit",
    "handle_job_unseal",
    "handle_job_why",
    "handle_launcher_add",
    "handle_launcher_check",
    "handle_launcher_list",
    "handle_launcher_remove",
    "handle_launcher_set",
    "handle_launcher_show",
    "handle_launcher_unset",
    "handle_manager_run",
    "handle_monitor",
    "handle_postprocess",
    "handle_precheck",
    "handle_remote_add",
    "handle_remote_check",
    "handle_remote_import_v1",
    "handle_remote_list",
    "handle_remote_remove",
    "handle_remote_set",
    "handle_remote_show",
    "handle_remote_unset",
    "handle_transfer",
    "handle_v1_collect",
    "handle_workflow_fetch",
    "handle_workflow_forget",
    "handle_workflow_install",
    "handle_workflow_list",
    "handle_workflow_show",
    "handle_workflow_uninstall",
    "handle_workspace_check",
    "handle_workspace_delete",
    "handle_workspace_forget",
    "handle_workspace_gc",
    "handle_workspace_init",
    "handle_workspace_list",
    "handle_workspace_seal",
    "handle_workspace_set",
    "handle_workspace_show",
    "handle_workspace_status",
    "handle_workspace_unseal",
    "handle_workspace_unset",
    "job_command",
    "launch_workspace_managers",
    "launcher_command",
    "manager_command",
    "monitor_command",
    "postprocess_command",
    "publish_job_requests",
    "pull_command",
    "push_command",
    "remote_command",
    "remote_workspace_output",
    "request_remote_job_result",
    "run_command",
    "submit_remote_manager_result",
    "v1_command",
    "workflow_command",
    "workspace_command",
]

def build_parser(
    program: str, context: CLIContext, *, include_workspace_job: bool = True, only: str | None = None
) -> argparse.ArgumentParser:
    """Build the internal dispatcher, or only workflow verbs without standalone groups.

    *only* naming a group builds that group alone, importing only its module; any other value builds them all.
    """

def dispatch(
    parser: argparse.ArgumentParser, argv: Sequence[str], context: CLIContext, *, prog: str | None = None
) -> int:
    """Parse *argv* with *parser* and run the command it names.

    A parser with no command named prints its own help, so every level of the
    tree answers a bare invocation the way an operator exploring it expects.
    Errors are prefixed with *prog*, or the parser's own program name, so a
    standalone top-level command reports under its own name.
    """

def command(argv: Sequence[str], context: CLIContext, *, prog: str | None = None) -> int:
    """Handle the internal super-dispatcher for every workflow command group.

    *prog* names the command errors are reported under (default ``httk``).
    """

def workflow_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered ``workflow`` command, excluding standalone groups."""

def workspace_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``workspace`` command."""

def daemon_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``daemon`` command with its own parser, not the workflow tree."""

def job_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``job`` command."""

def collect_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``collect`` command."""

def run_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``run`` command."""

def manager_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``manager`` command."""

def monitor_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``monitor`` command."""

def postprocess_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``postprocess`` command."""

def remote_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``remote`` command."""

def launcher_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``launcher`` command."""

def config_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``config`` command."""

def campaign_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``campaign`` command."""

def push_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``push`` command."""

def pull_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``pull`` command."""

def v1_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``v1`` command."""
