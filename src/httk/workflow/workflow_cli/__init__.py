"""Assemble the canonical :command:`httk workflow` command tree.

This package assembles the existing command-group handlers into one parser and
keeps the historical ``httk.workflow.workflow_cli`` import surface intact.
The group implementations live in private modules; consumers continue to
resolve the command and its handlers from this module.
"""

# This module intentionally consists largely of compatibility re-exports.
# ruff: noqa: F401

import argparse
import sys
from collections.abc import Sequence

from httk.core.cli import CLIContext

from ._build import build_build_parser, handle_build
from ._campaign import (
    build_campaign_parser,
    handle_campaign_collect,
    handle_campaign_init,
    handle_campaign_show,
    handle_campaign_start_managers,
    handle_campaign_submit,
)
from ._collect import build_collect_parser, handle_collect

# Private names were attributes of the old flat module too. Keep them
# available for code that imported them deliberately, without making them part
# of the package's command assembly.
from ._common import *
from ._common import (
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
    remote_workspace_output,
)
from ._compat import (
    add_v1_collect_arguments,
    build_v1_parser,
    handle_v1_collect,
)
from ._describe import build_describe_parser, build_list_parser, handle_workflow_describe, handle_workflow_list
from ._job import (
    _add_job_selector,
    add_job_request_arguments,
    add_job_submit_arguments,
    build_job_parser,
    build_runner_parser,
    ensure_identity_key,
    handle_job_debug,
    handle_job_delete,
    handle_job_list,
    handle_job_log,
    handle_job_new,
    handle_job_request,
    handle_job_show,
    handle_job_submit,
    handle_job_why,
    handle_runner_describe,
    handle_runner_publish,
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
    _submit_remote_manager,
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
    _render_project,
    add_project_doctor_arguments,
    add_project_manifest_create_arguments,
    add_project_manifest_verify_arguments,
    build_config_parser,
    build_project_parser,
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
from ._transfer import (
    _add_adapter_timeout,
    _dispatch_transfer_protocol,
    _fetch_jobs_from_remote,
    _remote_offer,
    _remote_retire,
    _remote_workspace_probe,
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
    run_transfer_verb_result,
)
from ._workspace import (
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
    handle_workspace_workflows,
)


def build_parser(
    program: str,
    context: CLIContext,
    *,
    include_workspace_job: bool = True,
) -> argparse.ArgumentParser:
    """Build the canonical command tree, optionally without standalone groups."""

    parser = argparse.ArgumentParser(
        prog=program,
        description="Filesystem-native workflow execution and project management",
        formatter_class=HelpFormatter,
    )
    parser.set_defaults(handler=None, help_parser=parser)
    groups = parser.add_subparsers(metavar="GROUP")
    if include_workspace_job:
        build_workspace_parser(groups, program=f"{context.program} workspace")
    build_runner_parser(groups)
    if include_workspace_job:
        build_job_parser(groups, program=f"{context.program} job")
    build_describe_parser(groups)
    build_list_parser(groups)
    build_collect_parser(groups)
    build_build_parser(groups)
    build_postprocess_parser(groups)
    build_precheck_parser(groups)
    build_run_parser(groups)
    build_manager_parser(groups)
    build_v1_parser(groups)
    build_config_parser(groups)
    build_project_parser(groups, context)
    build_remote_parser(groups)
    build_launcher_parser(groups)
    build_transfer_parser(groups)
    build_campaign_parser(groups)
    build_monitor_parser(groups)
    return parser


def dispatch(parser: argparse.ArgumentParser, argv: Sequence[str], context: CLIContext) -> int:
    """Parse *argv* with *parser* and run the command it names.

    A parser with no command named prints its own help, so every level of the
    tree answers a bare invocation the way an operator exploring it expects.
    """

    raw_argv = list(argv)
    if raw_argv == ["transfer"]:
        raw_argv.append("--help")
    if len(raw_argv) > 1 and raw_argv[0] == "transfer" and raw_argv[1] in _TRANSFER_PROTOCOL:
        try:
            return _dispatch_transfer_protocol(raw_argv[1:], context)
        except _ERRORS as exc:
            print(f"{parser.prog}: {exc}", file=sys.stderr)
            return 2
    # ``argparse`` does not intermingle an optional workspace positional with
    # the protocol's ``<path> --by-path KEY [VALUE]`` tail. Keep the frozen
    # remote vector and move only this hidden switch for the local parse.
    if raw_argv[:2] in (["workspace", "settings"], ["workspace", "workflow-prelude"]) and "--by-path" in raw_argv:
        raw_argv.remove("--by-path")
        raw_argv.append("--by-path")
    try:
        arguments = parser.parse_args(raw_argv)
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
    """Handle the internal super-dispatcher for every workflow command group."""

    return dispatch(build_parser(f"{context.program} workflow", context), argv, context)


def workflow_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered ``workflow`` command, excluding standalone groups."""

    return dispatch(build_parser(f"{context.program} workflow", context, include_workspace_job=False), argv, context)


def workspace_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``workspace`` command."""

    return command(["workspace", *argv], context)


def job_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``job`` command."""

    return command(["job", *argv], context)
