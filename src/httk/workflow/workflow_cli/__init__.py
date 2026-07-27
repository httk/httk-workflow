"""The canonical :command:`httk workflow` command tree.

This package assembles the existing command-group handlers into one parser and
keeps the historical ``httk.workflow.workflow_cli`` import surface intact.
The group implementations live in private modules; consumers continue to
resolve the command and its handlers from this module.
"""

# This module intentionally consists largely of compatibility re-exports.
# ruff: noqa: F401,F405

import argparse
import sys
from collections.abc import Sequence

from httk.core import CLIContext

from ._campaign import (
    build_campaign_parser,
    handle_campaign_harvest,
    handle_campaign_init,
    handle_campaign_show,
    handle_campaign_start_managers,
    handle_campaign_submit,
)

# Private names were attributes of the old flat module too. Keep them
# available for code that imported them deliberately, without making them part
# of the package's command assembly.
from ._common import *  # noqa: F401,F403
from ._common import (  # noqa: E402,F401
    _ERRORS,
    _LOGGER,
    _TRANSFER_PROTOCOL,
    _add_by_path_argument,
    _by_path,
    _durable,
    _field,
    _group,
    _json_value,
    _leaf,
    _local_root,
    _pairs,
    _remote_workspace_read,
    _required,
    _resolve_binding,
    _run_adapter,
    _run_remote_workspace,
    _settings,
    _vasp,
)
from ._compat import (  # noqa: E402,F401
    _add_import_arguments,
    _print_imported,
    _v1_pool,
    add_v1_job_arguments,
    add_v1_prepare_arguments,
    add_v1_run_arguments,
    add_v1_submit_arguments,
    build_import_parser,
    build_v1_parser,
    handle_import_cwl,
    handle_import_pwd,
    handle_v1_prepare,
    handle_v1_run,
    handle_v1_submit,
)
from ._harvest import build_harvest_parser, handle_harvest
from ._job import _add_job_selector  # noqa: E402,F401
from ._job import (
    add_job_request_arguments,
    add_job_submit_arguments,
    build_job_parser,
    build_runner_parser,
    handle_job_debug,
    handle_job_list,
    handle_job_log,
    handle_job_new,
    handle_job_request,
    handle_job_show,
    handle_job_submit,
    handle_job_why,
    handle_runner_describe,
    handle_runner_publish,
)
from ._manager import _submit_remote_manager  # noqa: E402,F401
from ._manager import (
    add_manager_run_arguments,
    build_manager_parser,
    handle_manager_run,
)
from ._project import _render_project  # noqa: E402,F401
from ._project import (
    add_project_doctor_arguments,
    add_project_manifest_create_arguments,
    add_project_manifest_verify_arguments,
    build_config_parser,
    build_project_parser,
    build_umbrella_doctor_parser,
    build_umbrella_manifest_parser,
    handle_config_import_v1,
    handle_config_init,
    handle_config_set,
    handle_config_show,
    handle_config_unset,
    handle_project_doctor,
    handle_project_import_v1,
    handle_project_init,
    handle_project_manifest_create,
    handle_project_manifest_verify,
    handle_project_show,
)
from ._transfer import (  # noqa: E402,F401
    _add_adapter_timeout,
    _dispatch_transfer_protocol,
    _fetch_jobs_from_remote,
    _remote_offer,
    _remote_retire,
    _remote_workspace_id,
    _render_remote,
    _report_transfer,
    _run_transfer_verb,
    _send_jobs_to_remote,
    _transfer_local_to_local,
    _transfer_remote_to_remote,
    build_remote_parser,
    build_transfer_parser,
    handle_remote_adapter_operation,
    handle_remote_add,
    handle_remote_import_v1,
    handle_remote_list,
    handle_remote_remove,
    handle_remote_show,
    handle_transfer,
    handle_transfer_offer,
    handle_transfer_receive,
    handle_transfer_retire,
)
from ._workspace import (  # noqa: E402,F401
    _init_settings,
    _policy_value,
    _print_policy,
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
    handle_workspace_settings_set,
    handle_workspace_settings_show,
    handle_workspace_settings_unset,
    handle_workspace_status,
    handle_workspace_unlock,
    handle_workspace_upgrade,
)


def build_parser(program: str, context: CLIContext) -> argparse.ArgumentParser:
    """Build the whole canonical :command:`httk workflow` tree."""

    parser = argparse.ArgumentParser(
        prog=program,
        description="Filesystem-native workflow execution and project management",
        formatter_class=HelpFormatter,
    )
    parser.set_defaults(handler=None, help_parser=parser)
    groups = parser.add_subparsers(metavar="GROUP")
    build_workspace_parser(groups)
    build_runner_parser(groups)
    build_job_parser(groups)
    build_import_parser(groups)
    build_harvest_parser(groups)
    build_manager_parser(groups)
    build_v1_parser(groups)
    build_config_parser(groups)
    build_project_parser(groups, context)
    build_remote_parser(groups)
    build_transfer_parser(groups)
    build_campaign_parser(groups)
    return parser


def dispatch(parser: argparse.ArgumentParser, argv: Sequence[str], context: CLIContext) -> int:
    """Parse *argv* with *parser* and run the command it names.

    A parser with no command named prints its own help, so every level of the
    tree answers a bare invocation the way an operator exploring it expects.
    """

    try:
        arguments = parser.parse_args(list(argv))
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    handler: Handler | None = getattr(arguments, "handler", None)
    if handler is None:
        getattr(arguments, "help_parser", parser).print_help()
        return 0
    try:
        return handler(arguments, context)
    except _ERRORS as exc:
        print(f"{parser.prog}: {exc}", file=sys.stderr)
        return 2


def command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``workflow`` command."""

    return dispatch(build_parser(f"{context.program} workflow", context), argv, context)
