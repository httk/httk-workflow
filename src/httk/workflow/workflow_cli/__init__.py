"""Assemble the top-level workflow module commands.

This package assembles the existing command-group handlers into one parser and
keeps the supported ``httk.workflow.workflow_cli`` import surface intact. The
group implementations live in private modules; consumers continue to resolve
the command and its handlers from this module.
"""

import argparse
import importlib
import sys
from collections.abc import Sequence
from typing import Any

from httk.core.cli import CLIContext

from ._common import (
    _ERRORS,
    remote_workspace_output,
)
from ._common import (
    Handler as _Handler,
)
from ._common import (
    HelpFormatter as _HelpFormatter,
)

# The group re-exports resolve lazily (PEP 562 ``__getattr__``), and ``build_parser`` builds only the group a
# command names, so a remote ``httk job adopt`` start imports the job group alone, not numpy and the storage
# stack behind ``collect``. Their static view, for type checkers and the API reference, is ``__init__.pyi``.
# pyright: reportUnsupportedDunderAll=false

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


_LAZY_EXPORTS = {
    "REMOTE_MANAGER_COMMAND": "..adapters",
    "REMOTE_STATUS_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_DELETE_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_FSCK_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_GC_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_INIT_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_LIST_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_MOVE_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_SETTINGS_COMMAND": "..adapters",
    "REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND": "..adapters",
    "build_build_parser": "._build",
    "handle_build": "._build",
    "build_campaign_parser": "._campaign",
    "handle_campaign_collect": "._campaign",
    "handle_campaign_init": "._campaign",
    "handle_campaign_show": "._campaign",
    "handle_campaign_start_managers": "._campaign",
    "handle_campaign_submit": "._campaign",
    "add_v1_collect_arguments": "._compat",
    "build_v1_parser": "._compat",
    "handle_v1_collect": "._compat",
    "build_describe_parser": "._describe",
    "build_install_parser": "._describe",
    "build_list_parser": "._describe",
    "handle_workflow_describe": "._describe",
    "handle_workflow_install": "._describe",
    "handle_workflow_list": "._describe",
    "handle_workflow_uninstall": "._describe",
    "build_job_parser": "._job",
    "ensure_identity_key": "._job",
    "handle_job_debug": "._job",
    "handle_job_delete": "._job",
    "handle_job_list": "._job",
    "handle_job_log": "._job",
    "handle_job_new": "._job",
    "handle_job_request": "._job",
    "handle_job_seal": "._job",
    "handle_job_show": "._job",
    "handle_job_submit": "._job",
    "handle_job_unseal": "._job",
    "handle_job_why": "._job",
    "publish_job_requests": "._job",
    "request_remote_job_result": "._job",
    "build_launcher_parser": "._launcher",
    "handle_launcher_add": "._launcher",
    "handle_launcher_check": "._launcher",
    "handle_launcher_configure": "._launcher",
    "handle_launcher_list": "._launcher",
    "handle_launcher_remove": "._launcher",
    "handle_launcher_show": "._launcher",
    "add_manager_run_arguments": "._manager",
    "add_run_arguments": "._manager",
    "build_manager_parser": "._manager",
    "build_run_parser": "._manager",
    "handle_manager_run": "._manager",
    "launch_workspace_managers": "._manager",
    "submit_remote_manager_result": "._manager",
    "build_monitor_parser": "._monitor",
    "handle_monitor": "._monitor",
    "build_postprocess_parser": "._postprocess",
    "handle_postprocess": "._postprocess",
    "build_precheck_parser": "._precheck",
    "handle_precheck": "._precheck",
    "build_config_parser": "._project",
    "handle_config_import_v1": "._project",
    "handle_config_set": "._project",
    "handle_config_show": "._project",
    "handle_config_unset": "._project",
    "build_seal_parser": "._seal",
    "handle_seal_verify": "._seal",
    "build_remote_parser": "._transfer",
    "build_transfer_operator_parser": "._transfer",
    "build_transfer_parser": "._transfer",
    "handle_remote_adapter_operation": "._transfer",
    "handle_remote_add": "._transfer",
    "handle_remote_import_v1": "._transfer",
    "handle_remote_list": "._transfer",
    "handle_remote_remove": "._transfer",
    "handle_remote_show": "._transfer",
    "handle_transfer": "._transfer",
    "add_workspace_init_arguments": "._workspace",
    "add_workspace_status_arguments": "._workspace",
    "build_workspace_parser": "._workspace",
    "handle_workspace_delete": "._workspace",
    "handle_workspace_forget": "._workspace",
    "handle_workspace_fsck": "._workspace",
    "handle_workspace_gc": "._workspace",
    "handle_workspace_init": "._workspace",
    "handle_workspace_list": "._workspace",
    "handle_workspace_policy_set": "._workspace",
    "handle_workspace_policy_show": "._workspace",
    "handle_workspace_seal": "._workspace",
    "handle_workspace_settings_set": "._workspace",
    "handle_workspace_settings_show": "._workspace",
    "handle_workspace_settings_unset": "._workspace",
    "handle_workspace_status": "._workspace",
    "handle_workspace_unseal": "._workspace",
    "handle_workspace_workflows": "._workspace",
}

# Each command group: the names it registers, its module, its parser builder, whether the builder takes
# ``program``, and whether the group belongs only to the full dispatcher (not to ``httk workflow``).
_GROUPS = (
    (("workspace",), "._workspace", "build_workspace_parser", True, True),
    (("job",), "._job", "build_job_parser", True, True),
    (("describe",), "._describe", "build_describe_parser", False, False),
    (("list",), "._describe", "build_list_parser", False, False),
    (("install", "uninstall"), "._describe", "build_install_parser", False, False),
    (("seal",), "._seal", "build_seal_parser", False, True),
    (("collect",), "._collect", "build_collect_parser", True, True),
    (("build",), "._build", "build_build_parser", False, False),
    (("postprocess",), "._postprocess", "build_postprocess_parser", False, False),
    (("precheck",), "._precheck", "build_precheck_parser", False, False),
    (("run",), "._manager", "build_run_parser", False, False),
    (("manager",), "._manager", "build_manager_parser", False, True),
    (("v1",), "._compat", "build_v1_parser", False, True),
    (("config",), "._project", "build_config_parser", False, True),
    (("remote",), "._transfer", "build_remote_parser", False, True),
    (("launcher",), "._launcher", "build_launcher_parser", False, True),
    (("campaign",), "._campaign", "build_campaign_parser", False, True),
    (("monitor",), "._monitor", "build_monitor_parser", False, False),
)


def __getattr__(name: str) -> Any:
    """Import a group re-export from its module on first access.

    :param name: The module attribute to resolve.
    :return: The requested re-export.
    :raises AttributeError: If ``name`` is not a group re-export.
    """
    module = _LAZY_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """List the module attributes, including the not yet resolved re-exports.

    :return: The sorted attribute names.
    """
    return sorted(set(globals()) | set(__all__))


def build_parser(
    program: str,
    context: CLIContext,
    *,
    include_workspace_job: bool = True,
    only: str | None = None,
) -> argparse.ArgumentParser:
    """Build the internal dispatcher, or only workflow verbs without standalone groups.

    *only* naming a group builds that group alone, importing only its module; any other value builds them all.
    """

    parser = argparse.ArgumentParser(
        prog=program,
        description="Filesystem-native workflow execution and project management",
        formatter_class=_HelpFormatter,
    )
    parser.set_defaults(handler=None, help_parser=parser)
    groups = parser.add_subparsers(metavar="GROUP")
    included = [group for group in _GROUPS if include_workspace_job or not group[4]]
    for names, module, builder, takes_program, _ in [group for group in included if only in group[0]] or included:
        build = getattr(importlib.import_module(module, __name__), builder)
        if takes_program:
            build(groups, program=f"{context.program} {names[0]}")
        else:
            build(groups)
    return parser


def dispatch(
    parser: argparse.ArgumentParser, argv: Sequence[str], context: CLIContext, *, prog: str | None = None
) -> int:
    """Parse *argv* with *parser* and run the command it names.

    A parser with no command named prints its own help, so every level of the
    tree answers a bare invocation the way an operator exploring it expects.
    Errors are prefixed with *prog*, or the parser's own program name, so a
    standalone top-level command reports under its own name.
    """

    prog = prog or parser.prog

    raw_argv = list(argv)
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
    handler: _Handler | None = getattr(arguments, "handler", None)
    if handler is None:
        getattr(arguments, "help_parser", parser).print_help()
        return 0
    try:
        return handler(arguments, context)
    except _ERRORS as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return 2


def command(argv: Sequence[str], context: CLIContext, *, prog: str | None = None) -> int:
    """Handle the internal super-dispatcher for every workflow command group.

    *prog* names the command errors are reported under (default ``httk``).
    """

    return dispatch(build_parser(context.program, context, only=argv[0] if argv else None), argv, context, prog=prog)


def workflow_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered ``workflow`` command, excluding standalone groups."""

    # Frozen peer vector: remote launchers still invoke workflow manager run.
    # Keep it accepted but absent from public workflow help.
    if list(argv[:2]) == ["manager", "run"]:
        return manager_command(argv[1:], context)
    parser = build_parser(
        f"{context.program} workflow", context, include_workspace_job=False, only=argv[0] if argv else None
    )
    return dispatch(parser, argv, context)


def workspace_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``workspace`` command."""

    if argv and argv[0] == "daemon":
        from .._daemon_cli import command as daemon_command

        return daemon_command(argv[1:], program=f"{context.program} workspace daemon")
    return command(["workspace", *argv], context, prog=f"{context.program} workspace")


def job_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``job`` command."""

    return command(["job", *argv], context, prog=f"{context.program} job")


def collect_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``collect`` command."""

    return command(["collect", *argv], context, prog=f"{context.program} collect")


def manager_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``manager`` command."""

    return command(["manager", *argv], context, prog=f"{context.program} manager")


def remote_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``remote`` command."""

    return command(["remote", *argv], context, prog=f"{context.program} remote")


def launcher_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``launcher`` command."""

    return command(["launcher", *argv], context, prog=f"{context.program} launcher")


def config_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``config`` command."""

    return command(["config", *argv], context, prog=f"{context.program} config")


def campaign_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``campaign`` command."""

    return command(["campaign", *argv], context, prog=f"{context.program} campaign")


def seal_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``seal`` command."""

    return command(["seal", *argv], context, prog=f"{context.program} seal")


def v1_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``v1`` command."""

    return command(["v1", *argv], context, prog=f"{context.program} v1")


def transfer_command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``transfer`` command: inspecting held transfers."""

    from ._transfer import build_transfer_operator_parser

    parser = argparse.ArgumentParser(prog=context.program)
    parser.set_defaults(handler=None, help_parser=parser)
    build_transfer_operator_parser(parser.add_subparsers())
    return dispatch(parser, ["transfer", *argv], context, prog=f"{context.program} transfer")
