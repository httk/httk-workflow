"""The canonical :command:`httk workflow` command tree.

Every command this package offers is one leaf of a single nested
:mod:`argparse` tree, so the help of a group is argparse's own help, an unknown
action under a known group is argparse's own error naming that group, and the
two installed executables are thin aliases that reuse these very parsers and
handlers rather than a second implementation of them.

The module is laid out group by group. Each group has a ``build_*_parser``
function that declares its subcommands, and each subcommand has a ``handle_*``
function that receives the parsed :class:`argparse.Namespace` and the
:class:`~httk.core.CLIContext` and does nothing but call the library.

Some spellings are protocol rather than user interface: the argument vectors a
transfer runs on the far side of a remote adapter are listed once, near the
top, because the machine that runs them may have an older or newer *httk*
installed than the machine that composed them.
"""

# ruff: noqa: F401

import argparse
import json
import logging
import sys
import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from httk.core import CLIContext

# The packaged domains register their templates as an import side effect, so the
# CLI resolves `job new --template NAME` against a populated registry. The generic
# execution layer never imports a domain; the CLI does, exactly here.
from .. import vasp as _vasp
from .._logging import LOG_LEVELS, add_log_file, configure_logging
from .._util import read_json, sha256_file, utc_now, write_json_atomic
from ..adapters import (
    add_remote,
    import_v1_remote,
    list_remotes,
    metadata_path,
    read_metadata,
    resolve_remote,
    run_adapter,
    split_settings,
    store_credentials,
)
from ..campaigns import (
    ASSIGNMENT_POLICIES,
    campaign_harvest,
    campaign_managers,
    campaign_submit,
    read_campaign,
    write_campaign,
)
from ..compat.cwl import import_cwl
from ..compat.pwd import import_pwd
from ..compat.v1 import V1TaskManager, prepare_v1_payload, submit_v1_task
from ..configuration import (
    import_v1_configuration,
    initialize_config,
    read_config,
    set_config_key,
    sign_document,
    unset_config_key,
)
from ..errors import WorkflowError
from ..gc import iter_report_rows
from ..harvesting import DEFAULT_HARVEST_STATES, HARVESTABLE_KINDS, harvest
from ..hygiene import (
    describe_project,
    describe_remote,
    project_doctor,
    remove_remote,
)
from ..introspection import (
    JOB_HISTORY_FORMAT,
    JOB_LIST_FORMAT,
    debug_job,
    describe_job,
    explain_job,
    job_frames,
    list_jobs,
    render_frames,
    render_job,
    render_rows,
    resolve_job,
)
from ..manager import DEFAULT_TAKEOVER_GRACE_FACTOR, TaskManager
from ..manifests import create_manifest, release_maintenance_lock, verify_manifest
from ..models import CORE_PROFILE, POLICY_KEYS, STATE_KINDS, canonical_uuid
from ..projects import import_v1_project, initialize_project
from ..registry import (
    LOCAL_REMOTE,
    WorkspaceBinding,
    create_workspace,
    delete_workspace,
    forget_workspace,
    list_workspaces,
    remove_local_workspace,
    resolve_workspace,
)
from ..scaffold import (
    DEFAULT_PLACEMENT,
    STRUCTURE_PATTERNS,
    JobItem,
    ScaffoldedJob,
    new_job,
    new_jobs,
    registered_templates,
    structure_files,
    structure_tag,
)
from ..transfers import (
    DEFAULT_OFFER_STATES,
    TRANSFER_OFFER_FORMAT,
    TRANSFER_RETIREMENT_FORMAT,
    discard_staged_bundle,
    offer_transfers,
    retire_transfers,
)
from ..workspace import Workspace

_LOGGER = logging.getLogger(__name__)

#: Everything a handler may raise that is an operator's problem rather than a
#: defect. Anything here is reported as ``PROGRAM: message`` and exits ``2``.
_ERRORS = (WorkflowError, OSError, ValueError, RuntimeError, TimeoutError)

#: The command vectors one machine runs on another over a remote adapter.
#:
#: These are *protocol*, not user interface. The far side may run an older or a
#: newer *httk* than the side that composed the vector, so this is the frozen
#: spelling both ends agree on: the ``transfer`` group commands, plus the two
#: workspace/manager commands the transfer choreography reads. Only add a
#: spelling here once every supported release understands it.
REMOTE_RECEIVE_COMMAND = ("httk", "workflow", "transfer", "receive")
REMOTE_OFFER_COMMAND = ("httk", "workflow", "transfer", "offer")
REMOTE_RETIRE_COMMAND = ("httk", "workflow", "transfer", "retire")
REMOTE_STATUS_COMMAND = ("httk", "workflow", "workspace", "status")
REMOTE_MANAGER_COMMAND = ("httk", "workflow", "manager", "run")

#: The hidden protocol subcommands the ``transfer`` verb dispatches by name: the
#: far-side halves one machine invokes on another. A workspace name can never be
#: one of these, so the verb never mistakes ``transfer offer`` for a move.
_TRANSFER_PROTOCOL = ("receive", "offer", "retire")

#: What the two installed executables say about themselves in ``--help``. The
#: note lives in the epilog rather than on every run, because an alias that
#: printed a warning would corrupt the output of every script that parses it.
ALIAS_EPILOG = (
    "NOTE: `httk workflow ...` is the canonical spelling of every command\n"
    "below, and this executable is a compatibility alias for it. Both drive\n"
    "exactly the same parsers and exactly the same code."
)

TASKMANAGER_EPILOG = (
    f"{ALIAS_EPILOG}\n\n"
    "  init     ->  httk workflow workspace init\n"
    "  submit   ->  httk workflow job submit\n"
    "  run      ->  httk workflow manager run\n"
    "  status   ->  httk workflow workspace status\n"
    "  request  ->  httk workflow job request\n"
)

V1_TASKMANAGER_EPILOG = (
    f"{ALIAS_EPILOG}\n\n"
    "  prepare  ->  httk workflow v1 prepare\n"
    "  submit   ->  httk workflow v1 submit\n"
    "  run      ->  httk workflow v1 run\n"
)

Handler = Callable[[argparse.Namespace, CLIContext], int]


# ---------------------------------------------------------------------------
def _run_adapter(*args: Any, **kwargs: Any) -> Any:
    """Call the package-level adapter, preserving the historical patch point."""

    from . import run_adapter as configured_run_adapter

    return configured_run_adapter(*args, **kwargs)


# Parser construction helpers
# ---------------------------------------------------------------------------


class HelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Leave room for the longest command name, and print epilogs as written.

    argparse measures a subcommand listing without the extra indentation it then
    prints it with, so a command whose name is only just too long has its one
    line of help pushed onto a second one. Measuring it the way it is printed is
    what keeps a group's listing a readable two-column table.
    """

    def __init__(self, prog: str) -> None:
        super().__init__(prog, max_help_position=32)

    def add_argument(self, action: argparse.Action) -> None:
        """Reserve the column width the indented subcommand names really need."""

        super().add_argument(action)
        subactions = getattr(action, "_get_subactions", None)
        if subactions is None:
            return
        longest = max(
            (len(self._format_action_invocation(item)) for item in subactions()),
            default=0,
        )
        self._action_max_length = max(self._action_max_length, longest + self._current_indent + 2)


def _group(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    name: str,
    *,
    description: str,
    summary: str,
) -> tuple[argparse.ArgumentParser, "argparse._SubParsersAction[argparse.ArgumentParser]"]:
    """Add one command group, and return it with its subcommand action.

    A group invoked with no subcommand prints its own help and exits zero, which
    is what an operator exploring the tree means by typing it.
    """

    parser = subparsers.add_parser(
        name,
        help=summary,
        description=description,
        formatter_class=HelpFormatter,
    )
    parser.set_defaults(handler=None, help_parser=parser)
    return parser, parser.add_subparsers(metavar="COMMAND")


def _leaf(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    name: str,
    *,
    description: str,
    summary: str,
    handler: Handler,
    hidden: bool = False,
) -> argparse.ArgumentParser:
    """Add one command, and bind the function that runs it."""

    if hidden:
        parser = subparsers.add_parser(name, description=description, formatter_class=HelpFormatter)
    else:
        parser = subparsers.add_parser(
            name,
            help=summary,
            description=description,
            formatter_class=HelpFormatter,
        )
    parser.set_defaults(handler=handler)
    return parser


def add_durability_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the durability switches to one command that publishes protocol state.

    Both default to ``argparse.SUPPRESS`` so that the value an alias
    executable parsed before its subcommand survives into the handler.
    """

    parser.add_argument(
        "--durable",
        action="store_true",
        default=argparse.SUPPRESS,
        help="fsync protocol publications (the default; accepted for compatibility)",
    )
    parser.add_argument(
        "--no-durable",
        action="store_true",
        default=argparse.SUPPRESS,
        help="do not fsync protocol publications; a crashed node may then strand markers",
    )


def _durable(arguments: argparse.Namespace) -> bool:
    """Report whether this invocation asked for durable publication.

    Durability is the default: a manager that survives a node crash must not be
    left holding markers that reference journal frames the page cache lost.
    """

    return not getattr(arguments, "no_durable", False)


def _required(
    value: str | None,
    label: str,
    *,
    non_interactive: bool,
    default: str | None = None,
) -> str:
    """Return *value*, asking for it on a terminal and refusing without one."""

    if value:
        return value
    if non_interactive or not sys.stdin.isatty():
        raise ValueError(f"missing required value {label!r} in non-interactive operation")
    suffix = f" [{default}]" if default else ""
    entered = input(f"{label}{suffix}: ").strip()
    result = entered or default
    if not result:
        raise ValueError(f"{label} cannot be empty")
    return result


def _json_value(text: str, label: str) -> object:
    """Return the JSON value one command-line argument denotes.

    The spelling is the one the Bash SDK's ``--input`` uses: ``@path`` is the JSON
    content of a file, which is how a value too large or too quoted for a command
    line is passed, and anything else is a JSON value when it parses as one and the
    literal string when it does not, so ``k=42`` is a number and ``k=Si`` a string
    without the author quoting either.
    """

    if text.startswith("@"):
        path = Path(text[1:]).expanduser()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"{label} cannot be read as JSON from {path}: {exc}") from exc
    try:
        return json.loads(text)
    except ValueError:
        return text


def _pairs(values: Sequence[str], label: str) -> list[tuple[str, str]]:
    """Split ``NAME=VALUE`` arguments, keeping the order they were given in."""

    result: list[tuple[str, str]] = []
    for item in values:
        name, separator, text = item.partition("=")
        if not separator or not name:
            raise ValueError(f"{label} must be spelled NAME=VALUE, not {item!r}")
        result.append((name, text))
    return result


def _settings(values: Sequence[str]) -> dict[str, str]:
    """Return the ``KEY=VALUE`` adapter settings one command line carried."""

    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise ValueError(f"adapter setting must use KEY=VALUE: {value!r}")
        result[key] = item
    return result


def _field(name: str, value: object) -> str:
    """Render one name/value line of a human-readable description."""

    return f"{name:<22}{value}"


# ---------------------------------------------------------------------------
# Workspace name resolution
#
# Every user-typed command names a *registered* workspace, never a bare path.
# The helpers below turn that name into either a local path or a remote binding.
# The one exception is ``--by-path``: a hidden switch the transfer choreography
# and remote-workspace creation set when one machine invokes a command on
# another, where there is no registry and the workspace is addressed by its path.
# ---------------------------------------------------------------------------


def _by_path(arguments: argparse.Namespace) -> bool:
    """Report whether this invocation addresses its workspace by literal path."""

    return bool(getattr(arguments, "by_path", False))


def _resolve_binding(arguments: argparse.Namespace, context: CLIContext) -> tuple[WorkspaceBinding | None, Path | None]:
    """Resolve the workspace argument to a binding and, when local, its path.

    ``--by-path`` yields ``(None, path)``: a literal filesystem path with no
    registry lookup, for the protocol spellings one machine runs on another. A
    registered name yields the binding, and its path too when the binding is
    local; a remote binding yields ``(binding, None)`` so the caller either
    dispatches over the adapter or refuses.
    """

    if _by_path(arguments):
        return None, Path(arguments.workspace)
    binding = resolve_workspace(arguments.workspace, project=context.cwd)
    return binding, (Path(binding.path) if binding.remote == LOCAL_REMOTE else None)


def _local_root(arguments: argparse.Namespace, context: CLIContext, *, action: str) -> Path:
    """Return the local path of the named workspace, refusing a remote binding."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        raise ValueError(
            f"workspace {binding.name!r} is bound to the remote {binding.remote!r}, so it cannot "
            f"{action} locally; run this on {binding.remote}, or reach it with "
            f"`httk workflow transfer` and `httk workflow workspace status {binding.name}`"
        )
    return root


def _run_remote_workspace(
    binding: WorkspaceBinding,
    context: CLIContext,
    argv_tail: Sequence[str],
    *,
    timeout: float | None = None,
) -> int:
    """Run one command against a remote binding's workspace and echo its output.

    The workspace on the far side has no registry, so the command it is sent is
    the path-based ``--by-path`` spelling built from the binding's remote path.
    Its standard output and error are relayed verbatim and its exit status is
    returned, so a read command reads exactly as it would locally.
    """

    target = resolve_remote(binding.remote, project=context.cwd)
    result = _run_adapter(
        target.bundle,
        "invoke",
        {"queue": target.queue, "argv": ["httk", "workflow", *argv_tail]},
        timeout=timeout,
    )
    stdout = str(result.get("stdout", ""))
    if stdout:
        sys.stdout.write(stdout if stdout.endswith("\n") else stdout + "\n")
    stderr = str(result.get("stderr", ""))
    if stderr:
        sys.stderr.write(stderr)
    return int(result.get("returncode", 0) or 0)


def _remote_workspace_read(
    binding: WorkspaceBinding,
    context: CLIContext,
    command: Sequence[str],
    arguments: argparse.Namespace,
    *,
    flags: Sequence[str] = (),
) -> int:
    """Dispatch one read-style workspace command to a remote binding.

    *command* is the group and verb, e.g. ``("workspace", "status")``; the remote
    path and ``--by-path`` are appended, then whichever of *flags* the parsed
    arguments set. This is the single path every remote-capable read command runs
    through, so ``status``, ``gc``, ``fsck``, ``harvest``, and the ``job`` reads
    all reach a remote the same way.
    """

    tail = [*command, binding.path, "--by-path"]
    for flag in flags:
        if getattr(arguments, flag.lstrip("-").replace("-", "_"), False):
            tail.append(flag)
    return _run_remote_workspace(binding, context, tail, timeout=getattr(arguments, "adapter_timeout", None))


def _add_by_path_argument(parser: argparse.ArgumentParser) -> None:
    """Add the hidden switch that makes the workspace argument a literal path.

    It is protocol, not user interface: the transfer choreography and remote
    workspace creation set it when one machine runs a command on another, where
    the workspace is addressed by path because the far side keeps no registry.
    """

    parser.add_argument("--by-path", action="store_true", help=argparse.SUPPRESS)


def _add_adapter_timeout(parser: argparse.ArgumentParser) -> None:
    """Add the bound on every adapter call one command makes."""

    parser.add_argument(
        "--adapter-timeout",
        type=float,
        metavar="SECONDS",
        help="bound every adapter operation this command runs (default: the remote's timeout_seconds)",
    )
