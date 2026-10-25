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
    REMOTE_WORKSPACE_DELETE_COMMAND,
    REMOTE_WORKSPACE_FSCK_COMMAND,
    REMOTE_WORKSPACE_GC_COMMAND,
    REMOTE_WORKSPACE_INIT_COMMAND,
    REMOTE_WORKSPACE_LIST_COMMAND,
    REMOTE_WORKSPACE_MOVE_COMMAND,
    REMOTE_WORKSPACE_SETTINGS_COMMAND,
    REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND,
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
    build_describe_parser,
    build_install_parser,
    build_list_parser,
    handle_workflow_describe,
    handle_workflow_install,
    handle_workflow_list,
    handle_workflow_uninstall,
)
from ._job import (
    build_job_parser,
    ensure_identity_key,
    handle_job_debug,
    handle_job_delete,
    handle_job_list,
    handle_job_log,
    handle_job_new,
    handle_job_request,
    handle_job_seal,
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
    handle_launcher_configure,
    handle_launcher_list,
    handle_launcher_remove,
    handle_launcher_show,
)
from ._manager import (
    add_manager_run_arguments,
    add_run_arguments,
    build_manager_parser,
    build_run_parser,
    handle_manager_run,
    launch_workspace_managers,
    submit_remote_manager_result,
)
from ._monitor import build_monitor_parser, handle_monitor
from ._postprocess import build_postprocess_parser, handle_postprocess
from ._precheck import build_precheck_parser, handle_precheck
from ._project import (
    build_config_parser,
    handle_config_import_v1,
    handle_config_set,
    handle_config_show,
    handle_config_unset,
)
from ._seal import build_seal_parser, handle_seal_verify
from ._transfer import (
    build_remote_parser,
    build_transfer_operator_parser,
    build_transfer_parser,
    handle_remote_adapter_operation,
    handle_remote_add,
    handle_remote_import_v1,
    handle_remote_list,
    handle_remote_remove,
    handle_remote_show,
    handle_transfer,
)
from ._workspace import (
    add_workspace_init_arguments,
    add_workspace_status_arguments,
    build_workspace_parser,
    handle_workspace_delete,
    handle_workspace_forget,
    handle_workspace_fsck,
    handle_workspace_gc,
    handle_workspace_init,
    handle_workspace_list,
    handle_workspace_policy_set,
    handle_workspace_policy_show,
    handle_workspace_seal,
    handle_workspace_settings_set,
    handle_workspace_settings_show,
    handle_workspace_settings_unset,
    handle_workspace_status,
    handle_workspace_unseal,
    handle_workspace_workflows,
)

__all__ = [
    "REMOTE_MANAGER_COMMAND",
    "REMOTE_STATUS_COMMAND",
    "REMOTE_WORKSPACE_DELETE_COMMAND",
    "REMOTE_WORKSPACE_FSCK_COMMAND",
    "REMOTE_WORKSPACE_GC_COMMAND",
    "REMOTE_WORKSPACE_INIT_COMMAND",
    "REMOTE_WORKSPACE_LIST_COMMAND",
    "REMOTE_WORKSPACE_MOVE_COMMAND",
    "REMOTE_WORKSPACE_SETTINGS_COMMAND",
    "REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND",
    "add_manager_run_arguments",
    "add_run_arguments",
    "add_v1_collect_arguments",
    "add_workspace_init_arguments",
    "add_workspace_status_arguments",
    "build_build_parser",
    "build_campaign_parser",
    "build_config_parser",
    "build_describe_parser",
    "build_install_parser",
    "build_job_parser",
    "build_launcher_parser",
    "build_list_parser",
    "build_manager_parser",
    "build_monitor_parser",
    "build_parser",
    "build_postprocess_parser",
    "build_precheck_parser",
    "build_remote_parser",
    "build_run_parser",
    "build_seal_parser",
    "build_transfer_operator_parser",
    "build_transfer_parser",
    "build_v1_parser",
    "build_workspace_parser",
    "campaign_command",
    "collect_command",
    "command",
    "config_command",
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
    "handle_job_debug",
    "handle_job_delete",
    "handle_job_list",
    "handle_job_log",
    "handle_job_new",
    "handle_job_request",
    "handle_job_seal",
    "handle_job_show",
    "handle_job_submit",
    "handle_job_unseal",
    "handle_job_why",
    "handle_launcher_add",
    "handle_launcher_check",
    "handle_launcher_configure",
    "handle_launcher_list",
    "handle_launcher_remove",
    "handle_launcher_show",
    "handle_manager_run",
    "handle_monitor",
    "handle_postprocess",
    "handle_precheck",
    "handle_remote_adapter_operation",
    "handle_remote_add",
    "handle_remote_import_v1",
    "handle_remote_list",
    "handle_remote_remove",
    "handle_remote_show",
    "handle_seal_verify",
    "handle_transfer",
    "handle_v1_collect",
    "handle_workflow_describe",
    "handle_workflow_install",
    "handle_workflow_list",
    "handle_workflow_uninstall",
    "handle_workspace_delete",
    "handle_workspace_forget",
    "handle_workspace_fsck",
    "handle_workspace_gc",
    "handle_workspace_init",
    "handle_workspace_list",
    "handle_workspace_policy_set",
    "handle_workspace_policy_show",
    "handle_workspace_seal",
    "handle_workspace_settings_set",
    "handle_workspace_settings_show",
    "handle_workspace_settings_unset",
    "handle_workspace_status",
    "handle_workspace_unseal",
    "handle_workspace_workflows",
    "job_command",
    "launch_workspace_managers",
    "launcher_command",
    "manager_command",
    "publish_job_requests",
    "remote_command",
    "remote_workspace_output",
    "request_remote_job_result",
    "seal_command",
    "submit_remote_manager_result",
    "transfer_command",
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

def job_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``job`` command."""

def collect_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``collect`` command."""

def manager_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``manager`` command."""

def remote_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``remote`` command."""

def launcher_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``launcher`` command."""

def config_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``config`` command."""

def campaign_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``campaign`` command."""

def seal_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``seal`` command."""

def v1_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``v1`` command."""

def transfer_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``transfer`` command: inspecting held transfers."""
