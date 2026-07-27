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
from . import vasp as _vasp  # noqa: F401
from ._logging import LOG_LEVELS, add_log_file, configure_logging
from ._util import read_json, sha256_file, utc_now, write_json_atomic
from .adapters import (
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
from .campaigns import (
    ASSIGNMENT_POLICIES,
    campaign_harvest,
    campaign_managers,
    campaign_submit,
    read_campaign,
    write_campaign,
)
from .compat.cwl import import_cwl
from .compat.pwd import import_pwd
from .compat.v1 import V1TaskManager, prepare_v1_payload, submit_v1_task
from .configuration import (
    import_v1_configuration,
    initialize_config,
    read_config,
    set_config_key,
    sign_document,
    unset_config_key,
)
from .errors import WorkflowError
from .gc import iter_report_rows
from .harvesting import DEFAULT_HARVEST_STATES, HARVESTABLE_KINDS, harvest
from .hygiene import (
    describe_project,
    describe_remote,
    project_doctor,
    remove_remote,
)
from .introspection import (
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
from .manager import DEFAULT_TAKEOVER_GRACE_FACTOR, TaskManager
from .manifests import create_manifest, release_maintenance_lock, verify_manifest
from .models import CORE_PROFILE, POLICY_KEYS, STATE_KINDS, canonical_uuid
from .projects import import_v1_project, initialize_project
from .registry import (
    LOCAL_REMOTE,
    WorkspaceBinding,
    create_workspace,
    delete_workspace,
    forget_workspace,
    list_workspaces,
    remove_local_workspace,
    resolve_workspace,
)
from .scaffold import (
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
from .transfers import (
    DEFAULT_OFFER_STATES,
    TRANSFER_OFFER_FORMAT,
    TRANSFER_RETIREMENT_FORMAT,
    discard_staged_bundle,
    offer_transfers,
    retire_transfers,
)
from .workspace import Workspace

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
        longest = max((len(self._format_action_invocation(item)) for item in subactions()), default=0)
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
    result = run_adapter(
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


# ---------------------------------------------------------------------------
# workspace
# ---------------------------------------------------------------------------


def add_workspace_init_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace init`, shared with ``httk-taskmanager init``."""

    parser.add_argument("workspace", metavar="NAME", help="the name to register this workspace under")
    parser.add_argument(
        "--remote",
        metavar="REMOTE",
        help="the remote this workspace lives on, or 'local' for this machine (required)",
    )
    parser.add_argument("--path", metavar="PATH", help="where the workspace lives on that remote (required)")
    parser.add_argument(
        "--scope",
        choices=("global", "project"),
        default="global",
        help="register the name globally (default) or in this project",
    )
    parser.add_argument(
        "--setting",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="seed one application setting at creation, e.g. vasp.command=... (repeatable)",
    )
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        metavar="EXTENSION",
        choices=("transactional-data-v1", "detached-transfer-v1"),
        help="enable this optional workspace extension (repeatable)",
    )
    _add_by_path_argument(parser)
    add_durability_arguments(parser)


def _init_settings(arguments: argparse.Namespace) -> dict[str, object]:
    """Return the application settings ``workspace init --setting`` carried."""

    return {name: _json_value(text, f"setting {name}") for name, text in _pairs(arguments.setting, "a setting")}


def handle_workspace_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create one workflow workspace, register it, and print its name.

    The user form takes a NAME and the ``--remote``/``--path`` the workspace is
    bound to. The protocol form (``--by-path``) initializes a workspace directly
    at a path with no registration: it is what one machine runs on another, where
    the far side keeps no registry.
    """

    settings = _init_settings(arguments)
    if _by_path(arguments):
        workspace = Workspace.initialize(
            arguments.workspace,
            extensions=arguments.extension,
            durable=_durable(arguments),
        )
        for key, value in settings.items():
            workspace.set_setting(key, value)
        print(workspace.root)
        return 0
    if not arguments.remote:
        raise ValueError(
            "workspace init requires --remote; use --remote local for a workspace on this machine, "
            "because being local is never implied"
        )
    if not arguments.path:
        raise ValueError("workspace init requires --path: where the workspace lives on that remote")
    binding = create_workspace(
        arguments.workspace,
        remote=arguments.remote,
        path=arguments.path,
        scope=arguments.scope,
        project=context.cwd,
        extensions=arguments.extension,
        durable=_durable(arguments),
        settings=settings,
    )
    print(binding.name)
    return 0


def add_workspace_status_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace status`, shared with ``httk-taskmanager status``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace to summarize")
    parser.add_argument("--json", action="store_true", help="print the machine-readable status document")
    _add_by_path_argument(parser)
    add_durability_arguments(parser)


def handle_workspace_status(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Summarize the authoritative markers of one workspace.

    A remote binding is summarized over its adapter, so ``workspace status NAME``
    reads a remote workspace exactly as it reads a local one.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(binding, context, ("workspace", "status"), arguments, flags=("--json",))
    workspace = Workspace(root, mutable=False, durable=_durable(arguments))
    counts: dict[str, int] = {}
    rows: list[dict[str, object]] = []
    for marker in workspace.scan_markers(STATE_KINDS):
        counts[marker.kind] = counts.get(marker.kind, 0) + 1
        rows.append(
            {
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "state": marker.kind,
                "placement": marker.placement.as_posix(),
                "priority": marker.priority,
                "generation": marker.generation,
            }
        )
    if arguments.json:
        print(
            json.dumps(
                {
                    "format": "httk-workflow-status",
                    "format_version": 1,
                    "workspace_id": workspace.workspace_id,
                    "workspace_format_version": workspace.format["format_version"],
                    "core_profile": workspace.format["core_profile"],
                    "extensions": sorted(workspace.extensions),
                    "counts": counts,
                    "jobs": rows,
                },
                indent=2,
            )
        )
        return 0
    print(f"workspace {workspace.workspace_id}")
    for kind in sorted(counts):
        print(f"{kind:12s} {counts[kind]}")
    return 0


def _policy_value(key: str, text: str) -> object:
    """Parse one command-line policy value as the JSON it denotes."""

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"policy {key} must be given as JSON: {text!r} ({exc})") from exc


def _print_policy(policy: Any, *, as_json: bool) -> int:
    """Print one workspace policy, as JSON or as tab-separated members."""

    if as_json:
        print(json.dumps(policy.as_mapping(), indent=2, sort_keys=True))
        return 0
    for key, value in sorted(policy.as_mapping().items()):
        print(f"{key}\t{json.dumps(value, sort_keys=True)}")
    return 0


def handle_workspace_policy_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show the tunables one workspace shares with every process attaching it."""

    root = _local_root(arguments, context, action="show its policy")
    return _print_policy(Workspace(root, mutable=False).policy, as_json=arguments.json)


def handle_workspace_policy_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one policy member of a workspace and print the result."""

    root = _local_root(arguments, context, action="change its policy")
    # A retention member is addressed directly so that setting one limit does
    # not require restating the whole object as JSON.
    if arguments.key.startswith("retention."):
        member = arguments.key.split(".", 1)[1]
        workspace = Workspace(root)
        retention = dict(workspace.policy.retention.as_mapping())
        retention[member] = _policy_value(arguments.key, arguments.value)
        policy = workspace.set_policy({"retention": retention})
    else:
        policy = Workspace(root).set_policy({arguments.key: _policy_value(arguments.key, arguments.value)})
    return _print_policy(policy, as_json=arguments.json)


def handle_workspace_fsck(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Check, and optionally repair, the marker-to-journal integrity."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            ("workspace", "fsck"),
            arguments,
            flags=("--repair", "--quarantine-unrepairable", "--json"),
        )
    workspace = Workspace(root, mutable=arguments.repair)
    report = workspace.check(repair=arguments.repair, quarantine_unrepairable=arguments.quarantine_unrepairable)
    if arguments.json:
        print(json.dumps(report.as_mapping(), indent=2, sort_keys=True))
    else:
        for finding in report.findings:
            print(f"{finding.action}\t{finding.problem}\t{finding.job_key or '-'}\t{finding.entry}\t{finding.detail}")
        print(f"checked {report.markers_checked} markers, {len(report.findings)} findings")
    # A clean workspace and a fully repaired one both exit zero; anything an
    # operator still has to deal with exits one, as a check command should.
    return 1 if report.unresolved else 0


def handle_workspace_gc(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Free the disk the workspace retention policy says may be freed."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(binding, context, ("workspace", "gc"), arguments, flags=("--dry-run", "--json"))
    workspace = Workspace(root, mutable=not arguments.dry_run)
    report = workspace.collect_garbage(dry_run=arguments.dry_run)
    if arguments.json:
        print(json.dumps(report.as_mapping(), indent=2, sort_keys=True))
        return 0
    print(f"{'category':<24}{'candidates':>12}{'removed':>9}{'bytes':>14}")
    for name, candidates, removed, reclaimed in iter_report_rows(report):
        print(f"{name:<24}{candidates:>12}{removed:>9}{reclaimed:>14}")
    for skipped in report.skipped:
        print(f"skipped {skipped}")
    if arguments.dry_run:
        print("dry run: nothing was removed")
    return 0


def handle_workspace_upgrade(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Enable one implemented workspace extension in place."""

    workspace = Workspace(_local_root(arguments, context, action="upgrade it"))
    print("\n".join(sorted(workspace.upgrade(arguments.extension))))
    return 0


def handle_workspace_unlock(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Release a workspace maintenance lock."""

    workspace = Workspace(_local_root(arguments, context, action="release its lock"))
    print(release_maintenance_lock(workspace, force=arguments.force))
    return 0


def handle_workspace_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the registered workspaces and where each resolves to."""

    rows: list[dict[str, object]] = []
    for binding in list_workspaces(project=context.cwd):
        reachable: object
        if binding.remote == LOCAL_REMOTE:
            reachable = (Path(binding.path) / ".httk-workflow" / "format.json").is_file()
        else:
            # A remote is not probed here: reachability would need a live adapter
            # call per row, which listing must not require. The binding is shown
            # so `workspace status NAME` can check it deliberately.
            reachable = None
        rows.append(
            {
                "name": binding.name,
                "remote": binding.remote,
                "path": binding.path,
                "scope": binding.scope,
                "reachable": reachable,
            }
        )
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if not rows:
        print("no workspaces are registered; create one with `httk workflow workspace init`")
        return 0
    for row in rows:
        mark = "?" if row["reachable"] is None else ("ok" if row["reachable"] else "missing")
        print(f"{row['name']}\t{row['remote']}\t{row['path']}\t{row['scope']}\t{mark}")
    return 0


def handle_workspace_forget(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Deregister one workspace name, leaving the workspace itself in place."""

    binding = forget_workspace(arguments.workspace, project=context.cwd)
    print(f"forgot {binding.name} ({binding.scope})")
    return 0


def handle_workspace_delete(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Destroy a registered workspace and deregister it.

    Destruction is irreversible, so it is refused without ``--force``.
    """

    if _by_path(arguments):
        # The protocol form one machine runs on another: destroy the workspace at
        # a literal path, with no registry involved.
        if not arguments.force:
            raise ValueError("workspace delete requires --force")
        remove_local_workspace(Path(arguments.workspace))
        print(arguments.workspace)
        return 0
    binding = delete_workspace(arguments.workspace, project=context.cwd, force=arguments.force)
    print(f"deleted {binding.name}")
    return 0


def handle_workspace_settings_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show one workspace's application settings, or one named member."""

    workspace = Workspace(_local_root(arguments, context, action="read its settings"), mutable=False)
    settings = workspace.settings
    if arguments.key is not None:
        if arguments.key not in settings:
            raise ValueError(f"application setting is not set: {arguments.key}")
        print(json.dumps(settings[arguments.key], sort_keys=True))
        return 0
    if arguments.json:
        print(json.dumps(settings, indent=2, sort_keys=True))
        return 0
    for key in sorted(settings):
        print(f"{key}\t{json.dumps(settings[key], sort_keys=True)}")
    return 0


def handle_workspace_settings_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one application setting on a workspace."""

    workspace = Workspace(_local_root(arguments, context, action="set its settings"))
    settings = workspace.set_setting(arguments.key, _json_value(arguments.value, f"setting {arguments.key}"))
    print(json.dumps(settings[arguments.key], sort_keys=True))
    return 0


def handle_workspace_settings_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one application setting from a workspace."""

    workspace = Workspace(_local_root(arguments, context, action="unset its settings"))
    workspace.unset_setting(arguments.key)
    return 0


def build_workspace_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``workspace`` group: the workspace itself, not its jobs."""

    _, group = _group(
        subparsers,
        "workspace",
        summary="create, inspect, tune, check, and collect one workspace",
        description="Manage one filesystem-native workflow workspace",
    )

    add_workspace_init_arguments(
        _leaf(
            group,
            "init",
            summary="initialize a workflow workspace",
            description="Initialize one workflow workspace",
            handler=handle_workspace_init,
        )
    )
    add_workspace_status_arguments(
        _leaf(
            group,
            "status",
            summary="summarize the authoritative markers",
            description="Summarize the authoritative markers of one workspace",
            handler=handle_workspace_status,
        )
    )

    _, policy_actions = _group(
        group,
        "policy",
        summary="show or set the shared workspace policy",
        description="Show or set the tunables one workspace shares with every attacher",
    )
    show = _leaf(
        policy_actions,
        "show",
        summary="print the current policy",
        description="Print the shared policy of one workflow workspace",
        handler=handle_workspace_policy_show,
    )
    show.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace whose policy to print")
    show.add_argument("--json", action="store_true", help="print the policy as one JSON object")
    store = _leaf(
        policy_actions,
        "set",
        summary="store one policy member",
        description="Store one member of the shared policy of a workflow workspace",
        handler=handle_workspace_policy_set,
    )
    store.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace whose policy to change")
    store.add_argument("key", metavar="KEY", help="one of " + ", ".join(sorted(POLICY_KEYS)))
    store.add_argument("value", metavar="VALUE", help="the JSON value to store")
    store.add_argument("--json", action="store_true", help="print the resulting policy as one JSON object")

    listing = _leaf(
        group,
        "list",
        summary="list the registered workspaces",
        description="List the registered workspaces and where each name resolves to",
        handler=handle_workspace_list,
    )
    listing.add_argument("--json", action="store_true", help="print the registry as one JSON document")

    forget = _leaf(
        group,
        "forget",
        summary="deregister a workspace name",
        description="Deregister one workspace name, leaving the workspace itself untouched",
        handler=handle_workspace_forget,
    )
    forget.add_argument("workspace", metavar="NAME", help="the registered workspace name to forget")

    delete = _leaf(
        group,
        "delete",
        summary="destroy a workspace and deregister it",
        description="Destroy a registered workspace and deregister it; refused without --force",
        handler=handle_workspace_delete,
    )
    delete.add_argument("workspace", metavar="NAME", help="the registered workspace to destroy")
    delete.add_argument("--force", action="store_true", help="confirm the irreversible destruction")
    _add_by_path_argument(delete)

    _, settings_actions = _group(
        group,
        "settings",
        summary="show or set a workspace's application settings",
        description="Show or set the application settings a workspace's runners resolve at run time",
    )
    settings_show = _leaf(
        settings_actions,
        "show",
        summary="print the application settings",
        description="Print the application settings of one workspace, or one named setting",
        handler=handle_workspace_settings_show,
    )
    settings_show.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace whose settings to read")
    settings_show.add_argument("key", metavar="KEY", nargs="?", help="print only this setting (default: all of them)")
    settings_show.add_argument("--json", action="store_true", help="print the settings as one JSON object")
    settings_set = _leaf(
        settings_actions,
        "set",
        summary="store one application setting",
        description="Store one application setting on a workspace, e.g. vasp.command",
        handler=handle_workspace_settings_set,
    )
    settings_set.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace to change")
    settings_set.add_argument("key", metavar="KEY", help="the dotted setting name, e.g. vasp.command")
    settings_set.add_argument("value", metavar="VALUE", help="the JSON value, or a bare string, to store")
    settings_unset = _leaf(
        settings_actions,
        "unset",
        summary="remove one application setting",
        description="Remove one application setting from a workspace",
        handler=handle_workspace_settings_unset,
    )
    settings_unset.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace to change")
    settings_unset.add_argument("key", metavar="KEY", help="the dotted setting name to remove")

    fsck = _leaf(
        group,
        "fsck",
        summary="check that every marker resolves to its journal frame",
        description="Check, and optionally repair, the marker-to-journal integrity of a workspace",
        handler=handle_workspace_fsck,
    )
    fsck.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace to check")
    fsck.add_argument("--repair", action="store_true", help="re-point damaged markers at the last readable frame")
    fsck.add_argument(
        "--quarantine-unrepairable",
        action="store_true",
        help="with --repair, move a marker with no readable history into the quarantine",
    )
    fsck.add_argument("--json", action="store_true", help="print the findings as one JSON report")
    _add_by_path_argument(fsck)

    collect = _leaf(
        group,
        "gc",
        summary="collect the garbage the retention policy allows",
        description="Collect the garbage one workflow workspace has accumulated",
        handler=handle_workspace_gc,
    )
    collect.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace to collect")
    collect.add_argument("--dry-run", action="store_true", help="report what would be removed without touching it")
    collect.add_argument("--json", action="store_true", help="print the collection as one JSON report")
    _add_by_path_argument(collect)

    upgrade = _leaf(
        group,
        "upgrade",
        summary="enable an implemented workspace extension",
        description="Enable one implemented workflow workspace extension",
        handler=handle_workspace_upgrade,
    )
    upgrade.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace to upgrade")
    upgrade.add_argument(
        "--extension",
        action="append",
        required=True,
        metavar="EXTENSION",
        help="the extension to enable, for example detached-transfer-v1 (repeatable)",
    )

    unlock = _leaf(
        group,
        "unlock",
        summary="release a maintenance lock",
        description="Release a stale, or with --force a live, workspace maintenance lock",
        handler=handle_workspace_unlock,
    )
    unlock.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace whose lock to release")
    unlock.add_argument("--force", action="store_true", help="also remove a lock whose holder is still alive")


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------


def handle_runner_publish(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish one runner file into a workspace runner store."""

    reference = Workspace(_local_root(arguments, context, action="publish a runner into it")).publish_runner(
        arguments.file,
        name=arguments.name,
        replace=arguments.replace,
    )
    print(json.dumps(reference, indent=2, sort_keys=True))
    return 0


def handle_runner_describe(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Report the runners a workspace has published, with their digests."""

    workspace = Workspace(_local_root(arguments, context, action="read its runners"), mutable=False)
    store = workspace.runners
    if arguments.name is not None:
        target = workspace.runner_store_path(arguments.name)
        if not target.is_file():
            raise ValueError(f"no such workspace runner: {arguments.name}")
        found = [target]
    else:
        found = sorted(path for path in store.rglob("*") if path.is_file()) if store.is_dir() else []
    references = [
        {"source": "workspace", "path": path.relative_to(store).as_posix(), "sha256": sha256_file(path)}
        for path in found
    ]
    if arguments.json:
        print(json.dumps(references, indent=2, sort_keys=True))
        return 0
    for reference in references:
        print(f"{reference['path']}\t{reference['sha256']}")
    return 0


def build_runner_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``runner`` group: the shared runners a workspace publishes."""

    _, group = _group(
        subparsers,
        "runner",
        summary="publish and describe the shared runners of a workspace",
        description="Manage the shared runners one workspace publishes for its jobs",
    )

    publish = _leaf(
        group,
        "publish",
        summary="publish one runner into a workspace runner store",
        description="Publish one runner into a workspace runner store, pinned by digest",
        handler=handle_runner_publish,
    )
    publish.add_argument("file", metavar="FILE", help="the runner file to publish")
    publish.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the workspace to publish into")
    publish.add_argument(
        "--name", metavar="NAME", help="store name, including any subdirectory (default: the file name)"
    )
    publish.add_argument(
        "--replace",
        action="store_true",
        help="overwrite a stored runner of the same name whose content differs",
    )

    describe = _leaf(
        group,
        "describe",
        summary="report the published runners and their digests",
        description="Report the runners a workspace has published, as the references a job pins",
        handler=handle_runner_describe,
    )
    describe.add_argument("name", metavar="NAME", nargs="?", help="one store name (default: every published runner)")
    describe.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the workspace to read")
    describe.add_argument("--json", action="store_true", help="print the references as one JSON array")


# ---------------------------------------------------------------------------
# job
# ---------------------------------------------------------------------------


def handle_job_new(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Scaffold and submit one job per template, structure, or both."""

    workspace = Workspace(_local_root(arguments, context, action="submit into it"))
    inputs = {name: _json_value(text, f"job input {name!r}") for name, text in _pairs(arguments.inputs, "a job input")}
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(arguments.files, "a staged file")}
    shared: dict[str, Any] = {
        "inputs": inputs,
        "placement": arguments.placement,
        "priority": arguments.priority,
        "workdir_mode": arguments.workdir_mode,
        "data_mode": arguments.data_mode,
        "publish": arguments.publish,
        "step": arguments.step,
        "name": arguments.name,
    }
    structures = Path(arguments.structures).expanduser() if arguments.structures else None
    if structures is not None and structures.is_dir():
        found = structure_files(structures)
        if not found:
            raise ValueError(f"no {' or '.join(STRUCTURE_PATTERNS)} file in {structures}")
        items: list[JobItem] = [
            {
                "files": {**files, "POSCAR": path},
                "tag": arguments.tag or structure_tag(path) or f"structure-{index:04d}",
            }
            for index, path in enumerate(found)
        ]
        results: Iterator[ScaffoldedJob] = new_jobs(workspace, arguments.template, items, **shared)
    else:
        tag = arguments.tag
        if structures is not None:
            files["POSCAR"] = structures
            tag = tag or structure_tag(structures)
        results = iter([new_job(workspace, arguments.template, files=files, tag=tag, **shared)])
    if arguments.json:
        # One self-describing report per job, as an array, exactly as `harvest
        # --json` prints one array of records.
        print(json.dumps([job.as_mapping() for job in results], indent=2))
        return 0
    for job in results:
        # One tab-separated line per job, so a shell reads the key of one job with
        # cut and a campaign streams as it is submitted.
        print(f"{job.job_key}\t{job.payload}")
    return 0


def add_job_submit_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`job submit`, shared with ``httk-taskmanager submit``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    parser.add_argument("source", metavar="SOURCE", help="the complete payload directory to submit")
    parser.add_argument("--placement", metavar="PLACEMENT", required=True, help="where the job lands in the tree")
    parser.add_argument("--move", action="store_true", help="rename rather than copy the source")
    add_durability_arguments(parser)


def handle_job_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit one prepared payload directory and print its marker."""

    workspace = Workspace(_local_root(arguments, context, action="submit into it"), durable=_durable(arguments))
    marker = workspace.submit(arguments.source, arguments.placement, move=arguments.move)
    print(marker.path)
    return 0


def add_job_request_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`job request`, shared with ``httk-taskmanager request``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace holding the job")
    parser.add_argument("job_id", metavar="JOB_ID", help="the UUID of the job the request is about")
    parser.add_argument(
        "action",
        metavar="ACTION",
        choices=("continue", "override_step", "cancel", "set_priority", "pause"),
        help="continue, override_step, cancel, set_priority, or pause",
    )
    parser.add_argument("--operator", metavar="NAME", required=True, help="who is asking, recorded in the state frame")
    parser.add_argument("--reason", metavar="TEXT", required=True, help="why, recorded in the state frame")
    parser.add_argument("--priority", type=int, metavar="PRIORITY", help="the new priority, for set_priority")
    parser.add_argument("--step", metavar="STEP", help="the step to resume at, for override_step")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "accept the hazard of reviving a job a decided join already consumed; "
            "the hazard is journalled in the resulting state frame"
        ),
    )
    add_durability_arguments(parser)


def handle_job_request(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish one operator request against a job and print its path."""

    workspace = Workspace(_local_root(arguments, context, action="request against it"), durable=_durable(arguments))
    marker = workspace.find_marker_by_id(arguments.job_id)
    if marker is None:
        raise ValueError(f"job does not exist: {arguments.job_id}")
    request: dict[str, object] = {
        "format": "httk-workflow-request",
        "format_version": 1,
        "request_id": str(uuid.uuid4()),
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "placement": marker.placement.as_posix(),
        "expected_generation": marker.generation,
        "expected_record_ref": marker.record_ref,
        "action": arguments.action,
        "operator": arguments.operator,
        "reason": arguments.reason,
        "created_at": utc_now(),
    }
    if arguments.priority is not None:
        request["priority"] = arguments.priority
    if arguments.step is not None:
        request["step"] = arguments.step
    if arguments.force:
        request["force"] = True
    # Attribution, when this installation has an identity key: the manager
    # verifies a signature that is there and accepts a request that has none.
    print(workspace.publish_request(sign_document(request)))
    return 0


def handle_job_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the jobs of one workspace as a cheap table."""

    workspace = Workspace(_local_root(arguments, context, action="list its jobs"), mutable=False)
    rows = list_jobs(workspace, kinds=arguments.kind, placement=arguments.placement)
    if arguments.json:
        print(json.dumps({"format": JOB_LIST_FORMAT, "format_version": 1, "jobs": rows}, indent=2))
        return 0
    print(render_rows(rows))
    return 0


def handle_job_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one job completely from its authoritative state."""

    workspace = Workspace(_local_root(arguments, context, action="show its jobs"), mutable=False)
    report = describe_job(workspace, resolve_job(workspace, arguments.job))
    print(json.dumps(report, indent=2, sort_keys=True) if arguments.json else render_job(report))
    return 0


def handle_job_log(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print the recorded transition history of one job, oldest first."""

    if arguments.limit is not None and arguments.limit < 1:
        raise ValueError("--limit must be positive")
    workspace = Workspace(_local_root(arguments, context, action="read its job log"), mutable=False)
    frames = job_frames(workspace, resolve_job(workspace, arguments.job), limit=arguments.limit)
    if arguments.json:
        print(json.dumps({"format": JOB_HISTORY_FORMAT, "format_version": 1, "frames": frames}, indent=2))
        return 0
    print(render_frames(frames))
    return 0


def handle_job_why(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Explain why one job is, or is not, making progress."""

    workspace = Workspace(_local_root(arguments, context, action="explain its jobs"), mutable=False)
    diagnosis = explain_job(workspace, resolve_job(workspace, arguments.job))
    print(json.dumps(diagnosis.as_mapping(), indent=2, sort_keys=True) if arguments.json else diagnosis.render())
    return 0


def handle_job_debug(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Drive one job to a terminal state in the foreground."""

    # The transitions of the debugged job are reported by the debug runner
    # itself, so the private manager's own log stays quiet unless asked for.
    configure_logging(level=arguments.log_level)
    workspace = Workspace(_local_root(arguments, context, action="debug in it"))
    outcome = debug_job(
        workspace,
        arguments.job,
        placement=arguments.placement,
        step=arguments.step,
        follow_children=arguments.follow_children,
        timeout=arguments.timeout,
    )
    return outcome.exit_code


def _add_job_selector(parser: argparse.ArgumentParser) -> None:
    """Add the workspace and job selector every inspection command shares."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace holding the job")
    parser.add_argument("job", metavar="JOB", help="job UUID, job key, or any unique prefix of either")


def build_job_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``job`` group: making jobs, and finding out about them."""

    _, group = _group(
        subparsers,
        "job",
        summary="create, submit, inspect, and debug individual jobs",
        description="Create, submit, inspect, and debug the jobs of one workflow workspace",
    )

    new = _leaf(
        group,
        "new",
        summary="scaffold and submit jobs from a runner template",
        description="Scaffold and submit jobs from a runner template",
        handler=handle_job_new,
    )
    new.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    new.add_argument(
        "--template",
        metavar="TEMPLATE",
        required=True,
        help=f"a registered template ({', '.join(registered_templates())}) or the path of a runner file",
    )
    new.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one job input; VALUE is JSON when it parses as JSON and a string otherwise, "
        "and NAME=@FILE reads a JSON file (repeatable)",
    )
    new.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="stage PATH in the payload as NAME; a bare NAME lands in files/ (repeatable)",
    )
    new.add_argument(
        "--from",
        dest="structures",
        metavar="PATH",
        help="a structure file staged as files/POSCAR, or a directory of "
        f"{' / '.join(STRUCTURE_PATTERNS)} files, one job each",
    )
    new.add_argument("--tag", metavar="TAG", help="the readable half of the job key (default: derived from --from)")
    new.add_argument("--name", metavar="NAME", help="the human-readable job name")
    new.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default=DEFAULT_PLACEMENT,
        help=f"placement subtree (default: {DEFAULT_PLACEMENT})",
    )
    new.add_argument("--priority", type=int, metavar="PRIORITY", help="scheduling priority (default: the template's)")
    new.add_argument("--step", metavar="STEP", help="the step the job starts at (default: the template's own)")
    new.add_argument(
        "--data-mode",
        choices=("none", "transactional"),
        help="the job's data mode (default: what the template needs)",
    )
    new.add_argument(
        "--workdir-mode",
        choices=("persistent", "isolated"),
        default="persistent",
        help="the job's working-directory mode (default: persistent)",
    )
    new.add_argument(
        "--publish",
        choices=("workspace", "installed"),
        default="workspace",
        help="publish the runner into the workspace store (default), or reference a packaged one where it is installed",
    )
    new.add_argument("--json", action="store_true", help="print one JSON report per job, as an array")

    add_job_submit_arguments(
        _leaf(
            group,
            "submit",
            summary="submit a complete payload directory",
            description="Submit one complete payload directory into a workspace",
            handler=handle_job_submit,
        )
    )
    add_job_request_arguments(
        _leaf(
            group,
            "request",
            summary="publish an operator request",
            description="Publish one operator request against a job of a workspace",
            handler=handle_job_request,
        )
    )

    listing = _leaf(
        group,
        "list",
        summary="list the jobs of a workspace",
        description="List the jobs of one workflow workspace",
        handler=handle_job_list,
    )
    listing.add_argument("workspace", metavar="WORKSPACE", help="the workspace to list")
    listing.add_argument(
        "--kind",
        action="append",
        metavar="KIND",
        choices=STATE_KINDS,
        help="state kind to list (repeatable, default: every kind)",
    )
    listing.add_argument("--placement", metavar="PLACEMENT", help="list only jobs at or below this placement")
    listing.add_argument("--json", action="store_true", help="print the rows as one JSON document")

    show = _leaf(
        group,
        "show",
        summary="describe one job from its authoritative state",
        description="Describe one job from its authoritative state",
        handler=handle_job_show,
    )
    _add_job_selector(show)
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    log = _leaf(
        group,
        "log",
        summary="print the transition history of one job",
        description="Print the recorded transition history of one job, oldest first",
        handler=handle_job_log,
    )
    _add_job_selector(log)
    log.add_argument("--limit", type=int, metavar="COUNT", help="read at most this many frames, newest first")
    log.add_argument("--json", action="store_true", help="print the frames as one JSON document")

    why = _leaf(
        group,
        "why",
        summary="explain why one job is not running",
        description="Explain why one job is, or is not, making progress",
        handler=handle_job_why,
    )
    _add_job_selector(why)
    why.add_argument("--json", action="store_true", help="print the diagnosis as one JSON document")

    debug = _leaf(
        group,
        "debug",
        summary="drive one job to a terminal state in the foreground",
        description="Drive one job to a terminal state in the foreground, reporting every transition",
        handler=handle_job_debug,
    )
    debug.add_argument("workspace", metavar="WORKSPACE", help="the workspace to debug in")
    debug.add_argument("job", metavar="JOB", help="a payload directory to submit, or a selector of an existing job")
    debug.add_argument("--step", metavar="STEP", help="initial step of a freshly submitted payload")
    debug.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default="debug",
        help="placement of a freshly submitted payload (default: debug)",
    )
    debug.add_argument("--follow-children", action="store_true", help="drive spawned children depth first")
    debug.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        default=3600.0,
        help="give up driving the job after this long (default: 3600)",
    )
    debug.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default="error",
        help="log level of the private manager on the console (default: error)",
    )


# ---------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------


def _print_imported(job: ScaffoldedJob, *, as_json: bool) -> int:
    """Report one imported job the way ``job new`` reports a scaffolded one."""

    if as_json:
        print(json.dumps(job.as_mapping(), indent=2))
    else:
        print(f"{job.job_key}\t{job.payload}")
    return 0


def handle_import_pwd(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import one Python Workflow Definition document as one job."""

    workspace = Workspace(_local_root(arguments, context, action="import into it"))
    overrides = {
        name: _json_value(text, f"workflow input {name!r}")
        for name, text in _pairs(arguments.inputs, "a workflow input")
    }
    job = import_pwd(
        workspace,
        arguments.document,
        placement=arguments.placement,
        tag=arguments.tag,
        name=arguments.name,
        priority=arguments.priority,
        modules=arguments.modules,
        module_path=arguments.module_path,
        workflow_inputs=overrides,
        allowed_modules=arguments.allow_module,
        data_mode=arguments.data_mode,
        maximum_attempts=arguments.attempts,
        allow_unknown_version=arguments.allow_unknown_version,
    )
    return _print_imported(job, as_json=arguments.json)


def handle_import_cwl(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import one CWL workflow or command-line tool as one job."""

    workspace = Workspace(_local_root(arguments, context, action="import into it"))
    job = import_cwl(
        workspace,
        arguments.workflow,
        arguments.inputs,
        placement=arguments.placement,
        tag=arguments.tag,
        name=arguments.name,
        priority=arguments.priority,
        data_mode=arguments.data_mode,
    )
    for warning in job.warnings:
        print(f"warning: {warning}", file=sys.stderr)
    return _print_imported(job.job, as_json=arguments.json)


def build_import_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``import`` group: workflows written in another language."""

    _, group = _group(
        subparsers,
        "import",
        summary="import a workflow written in another language as one job",
        description=(
            "Import a workflow written in another language as one httk job. Importing is one way: "
            "the imported job carries the document it came from, and nothing translates a job back"
        ),
    )

    pwd = _leaf(
        group,
        "pwd",
        summary="import one Python Workflow Definition document",
        description="Import one Python Workflow Definition (PWD) JSON document as one job",
        handler=handle_import_pwd,
    )
    pwd.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    pwd.add_argument("document", metavar="DOCUMENT", help="the PWD JSON document to import")
    pwd.add_argument(
        "--module",
        dest="modules",
        action="append",
        default=[],
        metavar="FILE",
        help="a Python file staged into the payload and put first on the runner's import path (repeatable)",
    )
    pwd.add_argument(
        "--module-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="a further import root, as it will exist on the machine that runs the job (repeatable)",
    )
    pwd.add_argument(
        "--input",
        dest="inputs",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="override one input node by name; @FILE reads a JSON value from a file (repeatable)",
    )
    pwd.add_argument(
        "--allow-module",
        action="append",
        default=[],
        metavar="PREFIX",
        help="restrict the function nodes to modules at or below this prefix (repeatable)",
    )
    pwd.add_argument(
        "--attempts",
        type=int,
        default=3,
        metavar="COUNT",
        help="how many attempts one activation may take before the job fails (default: 3)",
    )
    pwd.add_argument(
        "--allow-unknown-version",
        action="store_true",
        help="import a document declaring a format version this importer was not written against",
    )
    _add_import_arguments(pwd)

    cwl = _leaf(
        group,
        "cwl",
        summary="import one CWL workflow or command-line tool",
        description=(
            "Import one Common Workflow Language document as one job. CWL is supported as a workflow "
            "language: the document is executed on httk's own runner and manager, never by cwltool"
        ),
        handler=handle_import_cwl,
    )
    cwl.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    cwl.add_argument("workflow", metavar="WORKFLOW", help="the .cwl workflow or command-line tool to import")
    cwl.add_argument("inputs", metavar="INPUTS", help="the CWL input object, as YAML or JSON")
    _add_import_arguments(cwl)


def _add_import_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare what every import command shares with every other one."""

    parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default=DEFAULT_PLACEMENT,
        help=f"where the job lands in the tree (default: {DEFAULT_PLACEMENT})",
    )
    parser.add_argument("--tag", metavar="TAG", help="the job tag, which prefixes its job key")
    parser.add_argument("--name", metavar="NAME", help="the human-readable job name")
    parser.add_argument("--priority", type=int, metavar="PRIORITY", help="the job priority (default: 500)")
    parser.add_argument(
        "--data-mode",
        choices=("none", "transactional"),
        default="none",
        help="publish the workflow outputs as transactional data (default: none)",
    )
    parser.add_argument("--json", action="store_true", help="print the submitted job as one JSON report")


# ---------------------------------------------------------------------------
# harvest
# ---------------------------------------------------------------------------


def handle_harvest(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Stream the finished jobs of one workspace as harvest records."""

    workspace = Workspace(_local_root(arguments, context, action="harvest it"), mutable=False)
    records = harvest(
        workspace,
        states=arguments.state or DEFAULT_HARVEST_STATES,
        placement=arguments.placement,
    )
    if arguments.json:
        print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
        return 0
    for record in records:
        print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
    return 0


def build_harvest_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare ``harvest``: the one leaf that is a command rather than a group."""

    parser = _leaf(
        subparsers,
        "harvest",
        summary="stream the finished jobs of a workspace as records",
        description="Harvest the finished jobs of one workflow workspace",
        handler=handle_harvest,
    )
    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace to harvest")
    parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=HARVESTABLE_KINDS,
        help=f"state kind to harvest (repeatable, default: {', '.join(DEFAULT_HARVEST_STATES)})",
    )
    parser.add_argument("--placement", metavar="PLACEMENT", help="harvest only jobs at or below this placement")
    output = parser.add_mutually_exclusive_group()
    output.add_argument(
        "--jsonl",
        action="store_true",
        help="print one record object per line, streaming (the default)",
    )
    output.add_argument(
        "--json",
        action="store_true",
        help="print one JSON array of every record, which materializes the whole harvest",
    )


# ---------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------


def add_manager_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`manager run`, shared with ``httk-taskmanager run``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the registered workspace this manager serves")
    parser.add_argument(
        "--pool",
        action="append",
        default=[],
        metavar="POOL",
        help="claim only jobs of this pool (repeatable, default: default)",
    )
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="run the manager in this process (the local default); refused for a remote workspace",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        metavar="COUNT",
        help="for a remote workspace, how many managers to submit to its scheduler (default: 1)",
    )
    _add_adapter_timeout(parser)
    _add_by_path_argument(parser)
    parser.add_argument(
        "--capability",
        action="append",
        default=[],
        metavar="CAPABILITY",
        help="advertise this capability to the scheduler (repeatable)",
    )
    parser.add_argument(
        "--placement-prefix",
        action="append",
        default=[],
        metavar="PREFIX",
        help=(
            "restrict every scheduling scan to jobs at or below this placement subtree "
            "(repeatable, default: the whole workspace)"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        metavar="COUNT",
        help="attempts to run at once, locally, or workers per submitted remote manager (default: 1)",
    )
    parser.add_argument(
        "--lease-seconds",
        type=float,
        metavar="SECONDS",
        help="lease length for this manager (default: the workspace policy's lease_seconds)",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="how often this manager refreshes its lease (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="how often this manager looks for work (default: 1)",
    )
    parser.add_argument("--until-idle", action="store_true", help="stop once no claimable job is left")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="with --until-idle, give up waiting for work after this long (default: 3600)",
    )
    parser.add_argument(
        "--unsafe-persistent-takeover",
        action="store_true",
        help="take over a persistent workdir on lease expiry alone, without proving the old writer stopped",
    )
    parser.add_argument(
        "--unsafe-isolated-takeover",
        action="store_true",
        help="relaunch an isolated-workdir attempt on lease expiry alone, without waiting out the takeover grace",
    )
    parser.add_argument(
        "--takeover-grace-factor",
        type=float,
        default=DEFAULT_TAKEOVER_GRACE_FACTOR,
        metavar="FACTOR",
        help="multiples of the lease a silent attempt is left alone before it may be taken over (default: 2.0)",
    )
    parser.add_argument(
        "--runner-search-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="ordered root for jobs whose runner.source is installed (repeatable)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="seconds to keep committing outcomes after a stop signal (default: 30)",
    )
    parser.add_argument(
        "--gc-interval",
        type=float,
        metavar="SECONDS",
        help=(
            "also collect garbage from this manager, at most once per SECONDS "
            "(default: no background collection; use 'httk workflow workspace gc' instead)"
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        help="log level for the manager log file, and for the console when given (default: info)",
    )
    parser.add_argument(
        "--log-file",
        metavar="PATH",
        help="manager log file (default: WORKSPACE/.httk-workflow/managers/MANAGER_ID/log)",
    )
    parser.add_argument("--json-logs", action="store_true", help="log one JSON object per line")
    add_durability_arguments(parser)


def _submit_remote_manager(binding: WorkspaceBinding, arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit one or more managers to a remote binding's scheduler."""

    if arguments.count < 1:
        raise ValueError("--count must be a positive integer")
    target = resolve_remote(binding.remote, project=context.cwd)
    manager_argv = [*REMOTE_MANAGER_COMMAND, binding.path, "--by-path"]
    # Left off unless asked for, so a queue configured with workers=N is not
    # permanently shadowed by a command-line default.
    if arguments.workers is not None:
        if arguments.workers < 1:
            raise ValueError("--workers must be a positive integer")
        manager_argv += ["--workers", str(arguments.workers)]
    request: dict[str, object] = {
        "queue": target.queue,
        "argv": manager_argv,
        "workspace": binding.path,
        "count": arguments.count,
    }
    print(json.dumps(run_adapter(target.bundle, "start-manager", request, timeout=arguments.adapter_timeout), indent=2))
    return 0


def handle_manager_run(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run one task manager with its own log file.

    A local binding runs the manager in this process. A remote binding submits
    managers through the remote's scheduler over its adapter; ``--foreground``,
    which asks to run here, is refused for a remote workspace because a manager
    on another machine cannot run in this process.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        if arguments.foreground:
            raise ValueError(
                f"--foreground cannot run a manager on the remote {binding.remote!r}; "
                "a remote workspace's managers are submitted through its scheduler"
            )
        return _submit_remote_manager(binding, arguments, context)
    workspace = Workspace(root, durable=_durable(arguments))
    # Without an explicit level the console stays quiet about normal lifecycle
    # events while the manager log file keeps the complete info-level record.
    configure_logging(level=arguments.log_level or "warning", json_logs=arguments.json_logs)
    with TaskManager(
        workspace,
        pools=arguments.pool or ["default"],
        capabilities=arguments.capability,
        placement_prefixes=arguments.placement_prefix,
        maximum_workers=arguments.workers if arguments.workers is not None else 1,
        lease_seconds=arguments.lease_seconds,
        heartbeat_interval=arguments.heartbeat_interval,
        unsafe_persistent_takeover=arguments.unsafe_persistent_takeover,
        unsafe_isolated_takeover=arguments.unsafe_isolated_takeover,
        takeover_grace_factor=arguments.takeover_grace_factor,
        runner_search_paths=arguments.runner_search_path,
        gc_interval=arguments.gc_interval,
    ) as manager:
        log_file = Path(arguments.log_file) if arguments.log_file else manager.manager_directory / "log"
        add_log_file(log_file, level=arguments.log_level or "info", json_logs=arguments.json_logs)
        _LOGGER.info(
            "manager %s serving workspace %s; logging to %s",
            manager.manager_id,
            workspace.root,
            log_file,
        )
        if arguments.until_idle:
            manager.run_until_idle(timeout=arguments.idle_timeout, poll_interval=arguments.poll_interval)
        else:
            manager.serve(poll_interval=arguments.poll_interval, drain_timeout=arguments.drain_timeout)
    return 0


def build_manager_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``manager`` group: the process that runs the jobs."""

    _, group = _group(
        subparsers,
        "manager",
        summary="run a task manager against a workspace",
        description="Run the task manager that claims and executes the jobs of a workspace",
    )
    add_manager_run_arguments(
        _leaf(
            group,
            "run",
            summary="run the task manager",
            description="Run one task manager against one workflow workspace",
            handler=handle_manager_run,
        )
    )


# ---------------------------------------------------------------------------
# v1
# ---------------------------------------------------------------------------


def _v1_pool(taskset: str) -> str:
    """Return the pool one *httk* v1 task set assigns work to."""

    return "default" if taskset == "any" else taskset


def add_v1_job_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the job description shared by :command:`v1 prepare` and ``v1 submit``."""

    parser.add_argument("--id", dest="job_id", metavar="UUID", help="the job UUID (default: a fresh one)")
    parser.add_argument("--tag", metavar="TAG", default="v1-task", help="the readable half of the job key")
    parser.add_argument("--name", metavar="NAME", default="httk v1 task", help="the human-readable job name")
    parser.add_argument("--step", metavar="STEP", default="start", help="the step the task starts at")
    # prepare and submit *assign* a task set, so their default is the task set
    # an unconfigured v1 installation calls its own; `run` *filters* by one, so
    # its default is the filter that accepts every set. See build_v1_parser.
    parser.add_argument(
        "--taskset",
        "-s",
        dest="taskset",
        metavar="TASKSET",
        default="default",
        help="the httk v1 task set this task belongs to (default: default)",
    )
    parser.add_argument(
        "--priority",
        type=int,
        choices=range(1, 6),
        default=3,
        metavar="PRIORITY",
        help="httk v1 priority, 1 to 5 (default: 3)",
    )
    parser.add_argument(
        "--attempts",
        "-a",
        type=int,
        default=10,
        metavar="COUNT",
        help="how many times the task may be attempted (default: 10)",
    )


def handle_v1_prepare(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Turn one instantiated *httk* v1 task directory into a v2 payload."""

    job = prepare_v1_payload(
        arguments.source,
        arguments.destination,
        job_id=arguments.job_id,
        tag=arguments.tag,
        name=arguments.name,
        initial_step=arguments.step,
        pool=_v1_pool(arguments.taskset),
        priority=arguments.priority,
        attempts=arguments.attempts,
    )
    print(job.job_key)
    return 0


def handle_v1_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Prepare and submit one instantiated *httk* v1 task."""

    workspace = Workspace(_local_root(arguments, context, action="submit into it"), durable=_durable(arguments))
    marker = submit_v1_task(
        workspace,
        arguments.source,
        arguments.placement,
        job_id=arguments.job_id,
        tag=arguments.tag,
        name=arguments.name,
        initial_step=arguments.step,
        pool=_v1_pool(arguments.taskset),
        priority=arguments.priority,
        attempts=arguments.attempts,
    )
    print(marker.path)
    return 0


def handle_v1_run(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run only *httk* v1 jobs of one v2 workspace."""

    workspace = Workspace(_local_root(arguments, context, action="run its managers"), durable=_durable(arguments))
    compression = "zstd" if arguments.zstdlog else "none" if arguments.no_bzip2log else "bzip2"
    with V1TaskManager(
        workspace,
        taskset=arguments.taskset,
        maximum_workers=arguments.workers,
        lease_seconds=arguments.lease_seconds,
        heartbeat_interval=arguments.heartbeat_interval,
        unsafe_persistent_takeover=arguments.unsafe_persistent_takeover,
        runtime_root=arguments.httk_v1_root,
        timeout=arguments.task_timeout,
        wrapper=arguments.wrap,
        log_compression=compression,
        attempts=arguments.attempts,
    ) as manager:
        if arguments.until_idle:
            manager.run_until_idle(timeout=arguments.idle_timeout, poll_interval=arguments.poll_interval)
        else:
            manager.serve(poll_interval=arguments.poll_interval)
    return 0


def add_v1_prepare_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 prepare`, shared with ``httk-v1-taskmanager prepare``."""

    parser.add_argument("source", metavar="SOURCE", help="the instantiated httk v1 task directory")
    parser.add_argument("destination", metavar="DESTINATION", help="the payload directory to write")
    add_v1_job_arguments(parser)
    add_durability_arguments(parser)


def add_v1_submit_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 submit`, shared with ``httk-v1-taskmanager submit``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace to submit into")
    parser.add_argument("source", metavar="SOURCE", help="the instantiated httk v1 task directory")
    parser.add_argument("--placement", metavar="PLACEMENT", required=True, help="where the job lands in the tree")
    add_v1_job_arguments(parser)
    add_durability_arguments(parser)


def add_v1_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`v1 run`, shared with ``httk-v1-taskmanager run``."""

    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace this manager serves")
    # `run` filters rather than assigns, so "any" — accept every task set — is
    # the only default that runs the work a v1 operator already submitted.
    parser.add_argument(
        "--taskset",
        "-s",
        dest="taskset",
        metavar="TASKSET",
        default="any",
        help="run only the jobs of this httk v1 task set (default: any = accept all)",
    )
    parser.add_argument("--wrap", "-w", metavar="COMMAND", help="wrap each task launch in this command")
    parser.add_argument(
        "--task-timeout",
        "-t",
        type=float,
        default=21600.0,
        metavar="SECONDS",
        help="give up on one task after this long (default: 21600)",
    )
    parser.add_argument(
        "--attempts",
        "-a",
        type=int,
        default=10,
        metavar="COUNT",
        help="how many times a task may be attempted (default: 10)",
    )
    parser.add_argument("--workers", type=int, default=1, metavar="COUNT", help="tasks to run at once (default: 1)")
    parser.add_argument(
        "--lease-seconds",
        type=float,
        metavar="SECONDS",
        help="lease length for this manager (default: the workspace policy's lease_seconds)",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="how often this manager refreshes its lease (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="how often this manager looks for work (default: 1)",
    )
    parser.add_argument("--until-idle", action="store_true", help="stop once no claimable task is left")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="with --until-idle, give up waiting for work after this long (default: 3600)",
    )
    parser.add_argument(
        "--unsafe-persistent-takeover",
        action="store_true",
        help="take over a persistent workdir on lease expiry alone, without proving the old writer stopped",
    )
    parser.add_argument("--httk-v1-root", metavar="DIRECTORY", help="the httk v1 runtime to execute tasks with")
    compression = parser.add_mutually_exclusive_group()
    compression.add_argument("--no-bzip2log", "-b", action="store_true", help="leave task logs uncompressed")
    compression.add_argument("--zstdlog", action="store_true", help="compress task logs with zstd rather than bzip2")
    add_durability_arguments(parser)


def build_v1_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``v1`` group: *httk* v1 task templates on the v2 engine.

    ``--taskset`` deliberately defaults differently between siblings, because
    the sibling commands mean different things by it. ``prepare`` and ``submit``
    *assign* a task set to the job they create, so their default is the ordinary
    ``default`` set; ``run`` *filters* the jobs it will claim, so its default is
    ``any``, which accepts every set. Unifying them would either strand every
    submitted job under a manager that filters for one set, or quietly file
    every prepared task under a set named ``any``.
    """

    _, group = _group(
        subparsers,
        "v1",
        summary="prepare, submit, and run httk v1 task templates",
        description="Prepare and execute httk v1 task templates through the v2 workflow engine",
    )
    add_v1_prepare_arguments(
        _leaf(
            group,
            "prepare",
            summary="turn an instantiated v1 task directory into a v2 payload",
            description="Turn one instantiated httk v1 task directory into a v2 payload",
            handler=handle_v1_prepare,
        )
    )
    add_v1_submit_arguments(
        _leaf(
            group,
            "submit",
            summary="prepare and submit an instantiated v1 task",
            description="Prepare and submit one instantiated httk v1 task",
            handler=handle_v1_submit,
        )
    )
    add_v1_run_arguments(
        _leaf(
            group,
            "run",
            summary="run only httk-v1 jobs in a v2 workspace",
            description="Run only the httk v1 jobs of one v2 workflow workspace",
            handler=handle_v1_run,
        )
    )


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def handle_config_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Write the user configuration and ensure an operator identity key."""

    current = read_config()
    name = _required(
        arguments.name,
        "name",
        non_interactive=arguments.non_interactive,
        default=str(current.get("name", "")) or None,
    )
    email = _required(
        arguments.email,
        "email",
        non_interactive=arguments.non_interactive,
        default=str(current.get("email", "")) or None,
    )
    print(json.dumps(initialize_config(name=name, email=email), indent=2, sort_keys=True))
    return 0


def handle_config_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print the whole user configuration, or one member of it."""

    values = read_config()
    if arguments.key:
        if arguments.key not in values:
            raise ValueError(f"configuration key is not set: {arguments.key}")
        value = values[arguments.key]
        print(json.dumps(value) if not isinstance(value, str) else value)
    else:
        print(json.dumps(values, indent=2, sort_keys=True))
    return 0


def handle_config_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one member of the user configuration."""

    print(set_config_key(arguments.key, arguments.value))
    return 0


def handle_config_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one member of the user configuration."""

    print(unset_config_key(arguments.key))
    return 0


def handle_config_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Read a legacy ``~/.httk`` configuration into the XDG one."""

    print(json.dumps(import_v1_configuration(arguments.source), indent=2, sort_keys=True))
    return 0


def build_config_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``config`` group: the per-user configuration and identity."""

    _, group = _group(
        subparsers,
        "config",
        summary="read and write the per-user httk configuration",
        description="Read and write the per-user httk configuration below $XDG_CONFIG_HOME",
    )

    initialize = _leaf(
        group,
        "init",
        summary="write the configuration and the identity key",
        description="Write the per-user configuration and ensure an operator identity key",
        handler=handle_config_init,
    )
    initialize.add_argument("--name", metavar="NAME", help="the operator's name")
    initialize.add_argument("--email", metavar="EMAIL", help="the operator's email address")
    initialize.add_argument("--non-interactive", action="store_true", help="never prompt; refuse a missing value")

    show = _leaf(
        group,
        "show",
        summary="print the configuration, or one member",
        description="Print the whole user configuration, or the value of one key",
        handler=handle_config_show,
    )
    show.add_argument("key", metavar="KEY", nargs="?", help="one configuration key (default: print everything)")

    store = _leaf(
        group,
        "set",
        summary="store one configuration member",
        description="Store one member of the user configuration",
        handler=handle_config_set,
    )
    store.add_argument("key", metavar="KEY", help="the configuration key to write")
    store.add_argument("value", metavar="VALUE", help="the value to store")

    remove = _leaf(
        group,
        "unset",
        summary="remove one configuration member",
        description="Remove one member of the user configuration",
        handler=handle_config_unset,
    )
    remove.add_argument("key", metavar="KEY", help="the configuration key to remove")

    imported = _leaf(
        group,
        "import-v1",
        summary="read a legacy ~/.httk configuration",
        description="Read a legacy httk v1 configuration into the XDG configuration",
        handler=handle_config_import_v1,
    )
    imported.add_argument("source", metavar="SOURCE", nargs="?", help="the legacy directory (default: ~/.httk)")


# ---------------------------------------------------------------------------
# project
# ---------------------------------------------------------------------------


def handle_project_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create one project directory, its key, and its workspace."""

    default_name = Path(arguments.path).resolve().name
    name = _required(
        arguments.name,
        "project name",
        non_interactive=arguments.non_interactive,
        default=default_name,
    )
    result = initialize_project(
        arguments.path,
        name=name,
        description=arguments.description,
        default_queue=arguments.default_queue,
        manifest_exclusions=arguments.exclude,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def handle_project_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Read a legacy ``ht.project`` into an *httk₂* project."""

    print(
        json.dumps(
            import_v1_project(arguments.path, source=arguments.source, name=arguments.name),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _render_project(description: dict[str, Any]) -> str:
    """Render one project description as readable lines."""

    project = description.get("project", {})
    keys = description.get("keys", {})
    workspace = description.get("workspace", {})
    manifest = description.get("manifest", {})
    public = keys.get("public_key") or {}
    lines = [
        _field("root", description.get("root")),
        _field("name", project.get("name") or "-"),
        _field("project_id", project.get("project_id") or "-"),
        _field("default_queue", project.get("default_queue") or "-"),
        _field("key_pinned", "yes" if keys.get("pinned") else "no"),
        _field("key_fingerprint", public.get("fingerprint") or "-"),
        _field("trusted_keys", len(keys.get("trusted_keys", []))),
        _field("workspace", workspace.get("workspace_id") or ("present" if workspace.get("present") else "-")),
        _field("jobs", workspace.get("jobs", 0)),
        _field("manifest", f"{manifest.get('verdict') or 'none'}: {manifest.get('reason') or '-'}"),
        _field("remotes", ", ".join(description.get("remotes", [])) or "-"),
    ]
    lock = workspace.get("maintenance_lock")
    if isinstance(lock, dict):
        lines.append(_field("maintenance_lock", f"{lock.get('holder')} ({'stale' if lock.get('stale') else 'live'})"))
    return "\n".join(lines)


def handle_project_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one project: its metadata, its keys, its workspace, its manifest."""

    description = describe_project(arguments.path or context.cwd, verify=not arguments.no_verify)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_project(description))
    return 0


def handle_project_doctor(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Check one project for the conditions that quietly break it later."""

    report = project_doctor(arguments.path or context.cwd, repair=arguments.repair)
    reported = report["findings"]
    findings: list[dict[str, Any]] = [
        finding for finding in (reported if isinstance(reported, list) else []) if isinstance(finding, dict)
    ]
    if arguments.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for finding in findings:
            repaired = " (repaired)" if finding.get("repaired") else ""
            print(f"{finding['status']}\t{finding['check']}\t{finding['message']}{repaired}")
        print(f"{report['problems']} problem(s), {report['repaired']} repaired")
    # A warning is a thing to know about, not a thing to fail a script on; only
    # a check that is actually broken makes the command itself fail.
    return 1 if any(finding.get("status") == "error" for finding in findings) else 0


def handle_project_manifest_create(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Write the signed manifest of one project."""

    print(create_manifest(arguments.project or context.cwd, output=arguments.manifest))
    return 0


def handle_project_manifest_verify(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Verify one project manifest against the tree and its trust anchors."""

    verification = verify_manifest(
        arguments.project or context.cwd,
        manifest=arguments.manifest,
        trusted_keys=arguments.trusted_key,
    )
    # The first line keeps the shape every existing caller reads; the verdict
    # line is what distinguishes a manifest that is merely self-consistent
    # from one signed by a key this project actually pins.
    print("valid" if verification.valid else "invalid")
    print(f"{verification.verdict}: {verification.reason}")
    return verification.exit_code


def add_project_doctor_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project doctor`, shared with ``httk project doctor``."""

    parser.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        help="the project to check (default: the nearest project of the working directory)",
    )
    parser.add_argument("--repair", action="store_true", help="also fix every finding that can be fixed automatically")
    parser.add_argument("--json", action="store_true", help="print the report as one JSON document")


def add_project_manifest_create_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project manifest create`, shared with the umbrella command."""

    parser.add_argument("project", metavar="PROJECT", nargs="?", help="the project (default: the nearest one)")
    parser.add_argument("--manifest", metavar="PATH", help="write the manifest here rather than in the project")


def add_project_manifest_verify_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project manifest verify`, shared with the umbrella command."""

    parser.add_argument("project", metavar="PROJECT", nargs="?", help="the project (default: the nearest one)")
    parser.add_argument("--manifest", metavar="PATH", help="verify this manifest rather than the project's")
    parser.add_argument(
        "--trusted-key",
        action="append",
        default=[],
        metavar="PATH_OR_VALUE",
        help=(
            "trust this Ed25519 public key as well: an ed25519:BASE64 value or the path of a "
            "*.pub file (repeatable). The project's pinned key is always trusted"
        ),
    )


def build_umbrella_doctor_parser(parser: argparse.ArgumentParser) -> None:
    """Declare ``httk project doctor`` on the umbrella command's own parser.

    Registered into the core ``httk project`` command, so ``httk project doctor``
    and ``httk workflow project doctor`` drive the very same handler.
    """

    add_project_doctor_arguments(parser)


def build_umbrella_manifest_parser(parser: argparse.ArgumentParser) -> None:
    """Declare ``httk project manifest create|verify`` on the umbrella parser."""

    parser.set_defaults(handler=None, help_parser=parser)
    actions = parser.add_subparsers(metavar="COMMAND")
    create = actions.add_parser(
        "create",
        help="write the signed manifest",
        description="Write the deterministic signed manifest of one project",
        formatter_class=HelpFormatter,
    )
    create.set_defaults(handler=handle_project_manifest_create, help_parser=create)
    add_project_manifest_create_arguments(create)
    verify = actions.add_parser(
        "verify",
        help="verify the manifest against the tree",
        description="Verify one project manifest against the tree and this project's trust anchors",
        formatter_class=HelpFormatter,
    )
    verify.set_defaults(handler=handle_project_manifest_verify, help_parser=verify)
    add_project_manifest_verify_arguments(verify)


def build_project_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    context: CLIContext,
) -> None:
    """Declare the ``project`` group: the directory a campaign lives in."""

    _, group = _group(
        subparsers,
        "project",
        summary="create, describe, check, and sign a project directory",
        description="Create, describe, check, and sign one httk project directory",
    )

    initialize = _leaf(
        group,
        "init",
        summary="create a project, its key, and its workspace",
        description="Create one project directory with its identity key and workflow workspace",
        handler=handle_project_init,
    )
    initialize.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        default=str(context.cwd),
        help="the directory to make a project (default: the working directory)",
    )
    initialize.add_argument("--name", metavar="NAME", help="the project name (default: the directory name)")
    initialize.add_argument("--description", metavar="TEXT", default="", help="a one-line description")
    initialize.add_argument("--default-queue", metavar="QUEUE", help="the remote queue commands use by default")
    initialize.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="PATTERN",
        help="exclude paths matching this glob from signed manifests (repeatable)",
    )
    initialize.add_argument("--non-interactive", action="store_true", help="never prompt; refuse a missing value")

    imported = _leaf(
        group,
        "import-v1",
        summary="read a legacy ht.project into a project",
        description="Read a legacy httk v1 ht.project directory into an httk project",
        handler=handle_project_import_v1,
    )
    imported.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        default=str(context.cwd),
        help="the directory to make a project (default: the working directory)",
    )
    imported.add_argument("--source", metavar="SOURCE", help="the legacy ht.project directory to read")
    imported.add_argument("--name", metavar="NAME", help="the project name (default: the legacy one)")

    show = _leaf(
        group,
        "show",
        summary="describe this project, its keys, and its workspace",
        description="Describe one project: its metadata, its keys, its workspace, and its manifest",
        handler=handle_project_show,
    )
    show.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        help="the project to describe (default: the nearest project of the working directory)",
    )
    show.add_argument(
        "--no-verify",
        action="store_true",
        help="do not walk the tree to classify the manifest, which is much cheaper on a large project",
    )
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    add_project_doctor_arguments(
        _leaf(
            group,
            "doctor",
            summary="check, and optionally repair, this project",
            description="Check one project for the conditions that quietly break it later",
            handler=handle_project_doctor,
        )
    )

    _, manifest_actions = _group(
        group,
        "manifest",
        summary="create and verify the signed project manifest",
        description="Create and verify the deterministic signed manifest of one project",
    )
    add_project_manifest_create_arguments(
        _leaf(
            manifest_actions,
            "create",
            summary="write the signed manifest",
            description="Write the deterministic signed manifest of one project",
            handler=handle_project_manifest_create,
        )
    )
    add_project_manifest_verify_arguments(
        _leaf(
            manifest_actions,
            "verify",
            summary="verify the manifest against the tree",
            description="Verify one project manifest against the tree and this project's trust anchors",
            handler=handle_project_manifest_verify,
        )
    )


# ---------------------------------------------------------------------------
# remote
# ---------------------------------------------------------------------------


def handle_remote_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the remotes this project and this user define."""

    print(json.dumps(list_remotes(context.cwd), indent=2, sort_keys=True))
    return 0


def handle_remote_add(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create one remote bundle from a packaged adapter template."""

    template = _required(
        arguments.template,
        "adapter template",
        non_interactive=arguments.non_interactive,
        default="local",
    )
    print(
        add_remote(
            arguments.name,
            template=template,
            global_scope=arguments.global_scope,
            project=context.cwd,
        )
    )
    return 0


def handle_remote_adapter_operation(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run the ``configure`` or ``install`` operation of one remote adapter."""

    operation = arguments.operation
    target = resolve_remote(arguments.remote, project=context.cwd)
    settings = _settings(arguments.set)
    result = run_adapter(
        target.bundle,
        operation,
        {"queue": target.queue, "settings": settings},
        timeout=arguments.adapter_timeout,
    )
    if operation == "configure" and settings:
        persistable, credentials = split_settings(settings)
        if persistable:
            metadata = read_metadata(target.bundle)
            queues = metadata.setdefault("queues", {})
            if not isinstance(queues, dict):
                raise ValueError("adapter queue configuration is not mutable JSON")
            queue = queues.setdefault(target.queue, {})
            if not isinstance(queue, dict):
                raise ValueError("adapter queue configuration is not an object")
            queue.update(persistable)
            # A bundle that still carries the pre-rename file name keeps it, so
            # configuring an old definition rewrites what is there rather than
            # leaving two metadata files behind.
            write_json_atomic(metadata_path(target.bundle), metadata)
        if credentials:
            path = store_credentials(target.bundle, target.queue, credentials)
            names = ", ".join(sorted(credentials))
            print(
                f"stored {names} for queue {target.queue} in {path}; "
                "values there are excluded from signed project manifests",
                file=sys.stderr,
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def handle_remote_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Map one recognized legacy httk v1 computer bundle into a remote."""

    print(
        import_v1_remote(
            arguments.source,
            name=arguments.name,
            global_scope=arguments.global_scope,
            project=context.cwd,
        )
    )
    return 0


def _render_remote(description: dict[str, Any]) -> str:
    """Render one remote description as readable lines."""

    lines = [
        _field("name", description.get("name")),
        _field("scope", description.get("scope")),
        _field("bundle", description.get("bundle")),
        _field("kind", description.get("kind") or "-"),
        _field("adapter_version", description.get("adapter_version") or "-"),
        _field("valid", "yes" if description.get("valid") else f"no: {description.get('problem')}"),
        _field("default_queue", description.get("default_queue")),
        _field("timeout_seconds", description.get("timeout_seconds")),
        _field("required_binaries", ", ".join(description.get("required_binaries", [])) or "-"),
        _field("credentials_file", description.get("credentials_file") or "-"),
    ]
    queues = description.get("queues", {})
    for queue, detail in sorted(queues.items()) if isinstance(queues, dict) else ():
        settings = detail.get("settings", {}) if isinstance(detail, dict) else {}
        credentials = detail.get("credential_keys", []) if isinstance(detail, dict) else []
        lines.append(f"queue {queue}")
        for key, value in sorted(settings.items()):
            lines.append(f"  {key}={value}")
        for key in credentials:
            # The value stays where it is; a description an operator pastes into
            # a bug report must never carry a password.
            lines.append(f"  {key}=<credential>")
    return "\n".join(lines)


def handle_remote_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one remote: where it lives, what it is, how it is configured."""

    description = describe_remote(arguments.name, project=context.cwd)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_remote(description))
    return 0


def handle_remote_remove(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one remote bundle, after asking unless told not to.

    ``--force`` skips the confirmation, and nothing else: a remote a sealed
    transfer still depends on is refused either way, because removing it would
    leave that transfer with no way home.
    """

    if not arguments.force:
        if not sys.stdin.isatty():
            raise ValueError(f"removing the remote {arguments.name!r} without a terminal requires --force")
        answer = input(f"remove the remote {arguments.name!r} and everything configured in it? [y/N]: ")
        if answer.strip().lower() not in {"y", "yes"}:
            print("not removed")
            return 1
    print(json.dumps(remove_remote(arguments.name, project=context.cwd), indent=2, sort_keys=True))
    return 0


def build_remote_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``remote`` group: the adapters that reach other machines."""

    _, group = _group(
        subparsers,
        "remote",
        summary="define, configure, describe, and remove remotes",
        description="Define, configure, describe, and remove the remote adapters of this project",
    )

    _leaf(
        group,
        "list",
        summary="list the remotes this project can reach",
        description="List the remotes this project and this user define",
        handler=handle_remote_list,
    )

    add = _leaf(
        group,
        "add",
        summary="create a remote from a packaged template",
        description="Create one remote bundle from a packaged adapter template",
        handler=handle_remote_add,
    )
    add.add_argument("name", metavar="NAME", help="the name this remote is addressed by")
    add.add_argument("--template", metavar="TEMPLATE", help="local, local-slurm, or ssh-slurm (default: local)")
    add.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the remote for this user rather than for this project",
    )
    add.add_argument("--non-interactive", action="store_true", help="never prompt; refuse a missing value")

    for operation, summary in (
        ("configure", "run the adapter's configure operation"),
        ("install", "run the adapter's install operation"),
    ):
        parser = _leaf(
            group,
            operation,
            summary=summary,
            description=f"Run the {operation} operation of one remote adapter",
            handler=handle_remote_adapter_operation,
        )
        parser.set_defaults(operation=operation)
        parser.add_argument("remote", metavar="REMOTE", help="the remote, as NAME or NAME:QUEUE")
        parser.add_argument(
            "--set",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="one adapter setting; a secret one is stored in credentials.json (repeatable)",
        )
        parser.add_argument(
            "--adapter-timeout",
            type=float,
            metavar="SECONDS",
            help="bound this adapter operation (default: the remote's timeout_seconds)",
        )

    imported = _leaf(
        group,
        "import-v1",
        summary="map a legacy computer bundle",
        description="Map one recognized legacy httk v1 computer bundle into a remote adapter bundle",
        handler=handle_remote_import_v1,
    )
    imported.add_argument("source", metavar="SOURCE", help="the legacy computer directory to read")
    imported.add_argument("--name", metavar="NAME", help="the name to define (default: the legacy one)")
    imported.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the remote for this user rather than for this project",
    )

    show = _leaf(
        group,
        "show",
        summary="describe one remote and its queues",
        description="Describe one remote: where it lives, what it is, and how it is configured",
        handler=handle_remote_show,
    )
    show.add_argument("name", metavar="NAME", help="the remote to describe")
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    remove = _leaf(
        group,
        "remove",
        summary="remove one remote bundle",
        description="Remove one remote bundle, refusing while a sealed transfer still needs it",
        handler=handle_remote_remove,
    )
    remove.add_argument("name", metavar="NAME", help="the remote to remove")
    remove.add_argument(
        "--force",
        action="store_true",
        help="skip the confirmation; a remote an unretired transfer still needs is refused either way",
    )


# ---------------------------------------------------------------------------
# transfer (formerly tasks, then remote)
# ---------------------------------------------------------------------------


def _remote_workspace_id(target: Any, root: str, *, timeout: float | None, noun: str = "destination") -> str:
    """Probe one remote workspace over the adapter and return its UUID.

    The probe is the same for both directions of a transfer: nothing is sealed,
    pushed, or pulled until the far side has answered with a status of the
    profile and extension this protocol needs, so an incompatible or absent
    workspace is reported before any state moves. The status is asked for
    ``--by-path`` because the far side keeps no registry: it addresses its own
    workspace by the path this client resolved the binding to.
    """

    status = run_adapter(
        target.bundle,
        "status",
        {"queue": target.queue, "argv": [*REMOTE_STATUS_COMMAND, root, "--by-path", "--json"]},
        timeout=timeout,
    )
    if status.get("returncode") != 0:
        raise RuntimeError(f"{noun} workspace compatibility check failed: {status.get('stderr', '')}")
    try:
        status_data = json.loads(str(status.get("stdout", "")))
        if (
            status_data.get("format") != "httk-workflow-status"
            or status_data.get("format_version") != 1
            or status_data.get("core_profile") != CORE_PROFILE
            or "detached-transfer-v1" not in status_data.get("extensions", [])
        ):
            raise ValueError
        workspace_id = str(status_data["workspace_id"])
        uuid.UUID(workspace_id)
    except (AttributeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise ValueError(f"{noun} did not return a compatible workflow workspace status") from exc
    return workspace_id


def _send_jobs_to_remote(
    source: Workspace,
    target: Any,
    destination_root: str,
    jobs: Sequence[str],
    *,
    destination_placement: str | None,
    timeout: float | None,
) -> list[dict[str, object]]:
    """Detach the named jobs from *source* and import them on a remote.

    This is the local→remote leg of the ``transfer`` verb: probe the destination
    workspace, then for each job seal a detached bundle, push it, and ask the far
    side to import it. Every step is idempotent, so an interrupted transfer is
    finished by running the same command again.
    """

    destination_workspace_id = _remote_workspace_id(target, destination_root, timeout=timeout)
    acknowledgements: list[dict[str, object]] = []
    for job_id in jobs:
        source.recover_transfers()
        candidates: list[dict[str, object]] = []
        for ledger_path in (source.control / "transfers").glob("*.json"):
            ledger = read_json(ledger_path)
            if (
                ledger.get("job_id") == job_id
                and ledger.get("destination_workspace_id") == destination_workspace_id
                and ledger.get("status") == "sealed"
            ):
                candidates.append(ledger)
        if len(candidates) > 1:
            raise ValueError(f"multiple resumable transfers exist for job: {job_id}")
        transfer_id = str(candidates[0]["transfer_id"]) if candidates else str(uuid.uuid4())
        if candidates and destination_placement:
            requested = str(destination_placement).strip("/")
            if candidates[0].get("destination_placement") != requested:
                raise ValueError("resumed transfer destination placement disagrees with the request")
        bundle = source.detach(
            job_id,
            destination_workspace_id=destination_workspace_id,
            destination_placement=destination_placement,
            transfer_id=transfer_id,
        )
        incoming = f"{destination_root.rstrip('/')}/.httk-workflow/transfers/incoming/{transfer_id}"
        push = run_adapter(
            target.bundle,
            "push",
            {"queue": target.queue, "source": str(bundle), "destination": incoming},
            timeout=timeout,
        )
        remote_bundle = str(push.get("path", incoming))
        invoked = run_adapter(
            target.bundle,
            "invoke",
            {
                "queue": target.queue,
                "argv": [
                    *REMOTE_RECEIVE_COMMAND,
                    "--workspace",
                    destination_root,
                    "--bundle",
                    remote_bundle,
                ],
            },
            timeout=timeout,
        )
        if invoked.get("returncode") != 0:
            raise RuntimeError(f"destination import failed: {invoked.get('stderr', '')}")
        try:
            acknowledgement = json.loads(str(invoked.get("stdout", "")))
        except json.JSONDecodeError as exc:
            raise ValueError("destination import did not return an acknowledgement") from exc
        if not isinstance(acknowledgement, dict):
            raise ValueError("destination acknowledgement is not an object")
        source.acknowledge_transfer(acknowledgement)
        acknowledgements.append(acknowledgement)
    return acknowledgements


def handle_transfer_receive(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import one sealed detached transfer bundle into this workspace."""

    acknowledgement = Workspace(arguments.workspace).import_bundle(arguments.bundle)
    print(json.dumps(acknowledgement, sort_keys=True, separators=(",", ":")))
    return 0


def handle_transfer_offer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal the finished jobs of this workspace for one that will fetch them."""

    workspace = Workspace(arguments.workspace)
    offers = offer_transfers(
        workspace,
        destination_workspace_id=arguments.destination_workspace_id,
        states=arguments.state or DEFAULT_OFFER_STATES,
        placement=arguments.placement,
    )
    if arguments.json:
        document = {
            "format": TRANSFER_OFFER_FORMAT,
            "format_version": 1,
            "workspace_id": workspace.workspace_id,
            "destination_workspace_id": arguments.destination_workspace_id,
            "offers": offers,
        }
        print(json.dumps(document, sort_keys=True, separators=(",", ":")))
        return 0
    for offer in offers:
        print(f"{offer['job_key']}\t{offer['state']}\t{offer['bundle_path']}")
    return 0


def handle_transfer_retire(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Retire the sealed source bundles of jobs another workspace has imported."""

    retired = retire_transfers(
        Workspace(arguments.workspace),
        arguments.jobs,
        destination_workspace_id=arguments.destination_workspace_id,
    )
    if arguments.json:
        document = {"format": TRANSFER_RETIREMENT_FORMAT, "format_version": 1, "retired": retired}
        print(json.dumps(document, sort_keys=True, separators=(",", ":")))
        return 0
    for entry in retired:
        print(f"{entry['job_key']}\t{entry['status']}\t{entry['retired_bundle']}")
    return 0


def _remote_offer(
    target: Any,
    remote_root: str,
    destination_workspace_id: str,
    *,
    states: Sequence[str] | None,
    placement: str | None,
    timeout: float | None,
) -> list[dict[str, object]]:
    """Ask a remote to seal its finished jobs and return the offers it made."""

    argv = [*REMOTE_OFFER_COMMAND, remote_root, "--destination-workspace-id", destination_workspace_id, "--json"]
    for state in states or DEFAULT_OFFER_STATES:
        argv += ["--state", state]
    if placement:
        argv += ["--placement", placement]
    offered = run_adapter(target.bundle, "invoke", {"queue": target.queue, "argv": argv}, timeout=timeout)
    if offered.get("returncode") != 0:
        raise RuntimeError(f"remote offer failed: {offered.get('stderr', '')}")
    try:
        document = json.loads(str(offered.get("stdout", "")))
        if document.get("format") != TRANSFER_OFFER_FORMAT or document.get("format_version") != 1:
            raise ValueError
        offers = document["offers"]
        if not isinstance(offers, list):
            raise ValueError
    except (AttributeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise ValueError("remote offer did not return a transfer offer document") from exc
    return [offer for offer in offers if isinstance(offer, dict)]


def _remote_retire(
    target: Any,
    remote_root: str,
    job_ids: Sequence[str],
    destination_workspace_id: str,
    *,
    timeout: float | None,
) -> list[object]:
    """Tell a remote the sources of imported jobs are no longer needed there."""

    if not job_ids:
        return []
    argv = [
        *REMOTE_RETIRE_COMMAND,
        remote_root,
        *job_ids,
        "--destination-workspace-id",
        destination_workspace_id,
        "--json",
    ]
    response = run_adapter(target.bundle, "invoke", {"queue": target.queue, "argv": argv}, timeout=timeout)
    if response.get("returncode") != 0:
        raise RuntimeError(f"remote retirement failed: {response.get('stderr', '')}")
    try:
        report = json.loads(str(response.get("stdout", "")))
        return list(report["retired"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("remote retirement did not return a retirement report") from exc


def _fetch_jobs_from_remote(
    local: Workspace,
    target: Any,
    remote_root: str,
    *,
    states: Sequence[str] | None,
    placement: str | None,
    timeout: float | None,
) -> tuple[list[dict[str, object]], list[object]]:
    """Bring the jobs that finished on one remote back into *local*.

    Probe the remote workspace, ask it to offer what stopped there, pull each
    offered bundle into local staging, import it, and only then tell the remote
    to retire the sources it still holds. Every step is idempotent, so an
    interrupted fetch is finished by running the same command again.
    """

    _remote_workspace_id(target, remote_root, timeout=timeout, noun="remote")
    offers = _remote_offer(target, remote_root, local.workspace_id, states=states, placement=placement, timeout=timeout)
    staging_root = local.control / "transfers" / "incoming"
    acknowledgements: list[dict[str, object]] = []
    for offer in offers:
        transfer_id = canonical_uuid(offer.get("transfer_id"), "transfer_id")
        staging = staging_root / transfer_id
        pulled = run_adapter(
            target.bundle,
            "pull",
            {"queue": target.queue, "source": str(offer["bundle_path"]), "destination": str(staging)},
            timeout=timeout,
        )
        acknowledgement = local.import_bundle(str(pulled.get("path", staging)))
        # The payload now lives at its placement in this workspace, so the
        # staged copy is dropped through a rename rather than left to be
        # re-imported by the next fetch.
        discard_staged_bundle(local, staging)
        acknowledgements.append(acknowledgement)
    retired = _remote_retire(
        target,
        remote_root,
        [str(acknowledgement["job_id"]) for acknowledgement in acknowledgements],
        local.workspace_id,
        timeout=timeout,
    )
    return acknowledgements, retired


def _transfer_local_to_local(source: Workspace, destination: Workspace, jobs: Sequence[str]) -> list[dict[str, object]]:
    """Move explicit jobs from one local workspace into another, directly."""

    if not jobs:
        raise ValueError("a local-to-local transfer needs at least one --job JOB_ID")
    acknowledgements: list[dict[str, object]] = []
    for job_id in jobs:
        source.recover_transfers()
        bundle = source.detach(
            job_id,
            destination_workspace_id=destination.workspace_id,
            transfer_id=str(uuid.uuid4()),
        )
        acknowledgement = destination.import_bundle(str(bundle))
        source.acknowledge_transfer(acknowledgement)
        acknowledgements.append(acknowledgement)
    return acknowledgements


def _transfer_remote_to_remote(
    source_binding: WorkspaceBinding,
    destination_binding: WorkspaceBinding,
    context: CLIContext,
    *,
    states: Sequence[str] | None,
    placement: str | None,
    timeout: float | None,
) -> tuple[list[dict[str, object]], list[object]]:
    """Relay jobs between two remotes through this client (v1 semantics).

    A direct remote-to-remote copy is deferred: this pulls each offered bundle
    from the source into local staging and pushes it to the destination, then
    asks the destination to import it and the source to retire the sources it
    still holds. Every leg reuses the same offer, pull, push, receive and retire
    the single-hop transfers use.
    """

    source_target = resolve_remote(source_binding.remote, project=context.cwd)
    destination_target = resolve_remote(destination_binding.remote, project=context.cwd)
    destination_workspace_id = _remote_workspace_id(destination_target, destination_binding.path, timeout=timeout)
    _remote_workspace_id(source_target, source_binding.path, timeout=timeout, noun="source")
    offers = _remote_offer(
        source_target,
        source_binding.path,
        destination_workspace_id,
        states=states,
        placement=placement,
        timeout=timeout,
    )
    acknowledgements: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="httk-relay-") as relay:
        for offer in offers:
            transfer_id = canonical_uuid(offer.get("transfer_id"), "transfer_id")
            staging = Path(relay) / transfer_id
            pulled = run_adapter(
                source_target.bundle,
                "pull",
                {"queue": source_target.queue, "source": str(offer["bundle_path"]), "destination": str(staging)},
                timeout=timeout,
            )
            local_bundle = str(pulled.get("path", staging))
            incoming = f"{destination_binding.path.rstrip('/')}/.httk-workflow/transfers/incoming/{transfer_id}"
            pushed = run_adapter(
                destination_target.bundle,
                "push",
                {"queue": destination_target.queue, "source": local_bundle, "destination": incoming},
                timeout=timeout,
            )
            remote_bundle = str(pushed.get("path", incoming))
            imported = run_adapter(
                destination_target.bundle,
                "invoke",
                {
                    "queue": destination_target.queue,
                    "argv": [
                        *REMOTE_RECEIVE_COMMAND,
                        "--workspace",
                        destination_binding.path,
                        "--bundle",
                        remote_bundle,
                    ],
                },
                timeout=timeout,
            )
            if imported.get("returncode") != 0:
                raise RuntimeError(f"destination import failed: {imported.get('stderr', '')}")
            try:
                acknowledgement = json.loads(str(imported.get("stdout", "")))
            except json.JSONDecodeError as exc:
                raise ValueError("destination import did not return an acknowledgement") from exc
            if not isinstance(acknowledgement, dict):
                raise ValueError("destination acknowledgement is not an object")
            acknowledgements.append(acknowledgement)
    retired = _remote_retire(
        source_target,
        source_binding.path,
        [str(acknowledgement["job_id"]) for acknowledgement in acknowledgements],
        destination_workspace_id,
        timeout=timeout,
    )
    return acknowledgements, retired


def handle_transfer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move jobs between two registered workspaces, or run a protocol command.

    ``transfer SRC DST`` is the canonical verb. It resolves both names and moves
    work whichever way they point: local→remote seals and imports on the remote,
    remote→local fetches finished jobs home, local→local imports directly, and
    remote→remote relays through this client. The hidden ``receive``, ``offer``,
    and ``retire`` spellings are the frozen protocol one machine runs on another.
    """

    tokens = list(arguments.args)
    if tokens and tokens[0] in _TRANSFER_PROTOCOL:
        return _dispatch_transfer_protocol(tokens, context)
    return _run_transfer_verb(tokens, context, getattr(arguments, "help_parser", None))


def _run_transfer_verb(tokens: Sequence[str], context: CLIContext, help_parser: argparse.ArgumentParser | None) -> int:
    """Parse and run the ``transfer SRC DST`` verb."""

    if not tokens:
        if help_parser is not None:
            help_parser.print_help()
        return 0
    parser = argparse.ArgumentParser(prog="httk workflow transfer", description="Move jobs between two workspaces")
    parser.add_argument("source", metavar="SRC", help="the registered workspace the jobs leave")
    parser.add_argument("destination", metavar="DST", help="the registered workspace the jobs arrive in")
    parser.add_argument(
        "--job", action="append", default=[], dest="jobs", metavar="JOB_ID", help="a job to move (repeatable)"
    )
    parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=HARVESTABLE_KINDS,
        help=f"state kind to move when fetching (repeatable, default: {', '.join(DEFAULT_OFFER_STATES)})",
    )
    parser.add_argument("--placement", metavar="PLACEMENT", help="move only jobs at or below this placement")
    parser.add_argument(
        "--destination-placement", metavar="PLACEMENT", help="where the jobs land (default: their placement)"
    )
    _add_adapter_timeout(parser)
    parser.add_argument("--json", action="store_true", help="print what moved as one JSON document")
    try:
        arguments = parser.parse_args(list(tokens))
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    source_binding = resolve_workspace(arguments.source, project=context.cwd)
    destination_binding = resolve_workspace(arguments.destination, project=context.cwd)
    source_local = source_binding.remote == LOCAL_REMOTE
    destination_local = destination_binding.remote == LOCAL_REMOTE
    timeout = arguments.adapter_timeout

    if source_local and not destination_local:
        target = resolve_remote(destination_binding.remote, project=context.cwd)
        if not arguments.jobs:
            raise ValueError("a local-to-remote transfer needs at least one --job JOB_ID")
        acknowledgements = _send_jobs_to_remote(
            Workspace(source_binding.path),
            target,
            destination_binding.path,
            arguments.jobs,
            destination_placement=arguments.destination_placement,
            timeout=timeout,
        )
        return _report_transfer(arguments, {"moved": acknowledgements})
    if destination_local and not source_local:
        target = resolve_remote(source_binding.remote, project=context.cwd)
        acknowledgements, retired = _fetch_jobs_from_remote(
            Workspace(destination_binding.path),
            target,
            source_binding.path,
            states=arguments.state,
            placement=arguments.placement,
            timeout=timeout,
        )
        return _report_transfer(arguments, {"moved": acknowledgements, "retired": retired})
    if source_local and destination_local:
        acknowledgements = _transfer_local_to_local(
            Workspace(source_binding.path), Workspace(destination_binding.path), arguments.jobs
        )
        return _report_transfer(arguments, {"moved": acknowledgements})
    acknowledgements, retired = _transfer_remote_to_remote(
        source_binding,
        destination_binding,
        context,
        states=arguments.state,
        placement=arguments.placement,
        timeout=timeout,
    )
    return _report_transfer(arguments, {"moved": acknowledgements, "retired": retired})


def _report_transfer(arguments: argparse.Namespace, report: Mapping[str, object]) -> int:
    """Print the result of one ``transfer`` verb run."""

    if arguments.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    moved = report.get("moved", [])
    assert isinstance(moved, list)
    for acknowledgement in moved:
        assert isinstance(acknowledgement, Mapping)
        print(f"{acknowledgement['job_key']}\t{acknowledgement['state']}\t{acknowledgement['placement']}")
    return 0


def _dispatch_transfer_protocol(tokens: Sequence[str], context: CLIContext) -> int:
    """Parse and run one hidden ``receive``/``offer``/``retire`` protocol command."""

    parser = argparse.ArgumentParser(prog="httk workflow transfer", add_help=True)
    protocol = parser.add_subparsers(dest="_which", required=True)

    receive = protocol.add_parser("receive")
    receive.add_argument("--workspace", metavar="WORKSPACE", required=True)
    receive.add_argument("--bundle", metavar="BUNDLE", required=True)
    receive.set_defaults(handler=handle_transfer_receive)

    offer = protocol.add_parser("offer")
    offer.add_argument("workspace", metavar="WORKSPACE")
    offer.add_argument("--destination-workspace-id", metavar="UUID", required=True)
    offer.add_argument("--state", action="append", metavar="STATE", choices=HARVESTABLE_KINDS)
    offer.add_argument("--placement", metavar="PLACEMENT")
    offer.add_argument("--json", action="store_true")
    offer.set_defaults(handler=handle_transfer_offer)

    retire = protocol.add_parser("retire")
    retire.add_argument("workspace", metavar="WORKSPACE")
    retire.add_argument("jobs", metavar="JOB_ID", nargs="+")
    retire.add_argument("--destination-workspace-id", metavar="UUID")
    retire.add_argument("--json", action="store_true")
    retire.set_defaults(handler=handle_transfer_retire)

    try:
        arguments = parser.parse_args(list(tokens))
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    return arguments.handler(arguments, context)


def _add_adapter_timeout(parser: argparse.ArgumentParser) -> None:
    """Add the bound on every adapter call one command makes."""

    parser.add_argument(
        "--adapter-timeout",
        type=float,
        metavar="SECONDS",
        help="bound every adapter operation this command runs (default: the remote's timeout_seconds)",
    )


def build_transfer_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``transfer`` verb: move jobs between two registered workspaces.

    ``transfer`` takes two workspace names and a few options, so it is one leaf
    with a trailing argument vector its handler parses. That vector also carries
    the hidden ``receive``/``offer``/``retire`` protocol spellings one machine
    runs on another over an adapter, named by the ``REMOTE_*_COMMAND`` vectors
    above; a workspace can therefore never be named after one of them.
    """

    transfer = _leaf(
        subparsers,
        "transfer",
        summary="move jobs between two registered workspaces",
        description=(
            "Move jobs between two registered workspaces: `transfer SRC DST`. It works whichever way the "
            "workspaces point — local to remote, remote to local, local to local, or remote to remote "
            "(relayed through this client). The hidden receive/offer/retire spellings are protocol."
        ),
        handler=handle_transfer,
    )
    transfer.set_defaults(help_parser=transfer)
    transfer.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        metavar="SRC DST [--job JOB_ID] [--state STATE] [--placement P]",
        help="the source and destination workspace names, and how much to move",
    )


# ---------------------------------------------------------------------------
# campaign
# ---------------------------------------------------------------------------


def handle_campaign_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Define this project's campaign partition map and assignment policy."""

    partitions = dict(_pairs(arguments.partition, "a partition"))
    config = write_campaign(partitions, assignment=arguments.assignment, project=context.cwd)
    print(
        json.dumps({"partitions": dict(config.partitions), "assignment": config.assignment}, indent=2, sort_keys=True)
    )
    return 0


def handle_campaign_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show this project's campaign partition map."""

    config = read_campaign(context.cwd)
    document = {"partitions": dict(config.partitions), "assignment": config.assignment}
    if arguments.json:
        print(json.dumps(document, indent=2, sort_keys=True))
        return 0
    print(f"assignment\t{config.assignment}")
    for partition in config.ordered_partitions():
        print(f"{partition}\t{config.partitions[partition]}")
    return 0


def handle_campaign_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Assign one root job to a partition and submit it into that workspace."""

    inputs = {name: _json_value(text, f"job input {name!r}") for name, text in _pairs(arguments.inputs, "a job input")}
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(arguments.files, "a staged file")}
    job = campaign_submit(
        arguments.template,
        key=arguments.key,
        index=arguments.index,
        project=context.cwd,
        inputs=inputs,
        files=files,
        tag=arguments.tag,
        placement=arguments.placement or DEFAULT_PLACEMENT,
        priority=arguments.priority,
        name=arguments.name,
    )
    if arguments.json:
        print(json.dumps(job.as_mapping(), indent=2))
        return 0
    print(f"{job.job_key}\t{job.payload}")
    return 0


def handle_campaign_harvest(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Harvest every partition of this campaign, one workspace after another."""

    records = campaign_harvest(
        states=arguments.state or DEFAULT_HARVEST_STATES,
        placement=arguments.placement,
        partitions=arguments.partition or None,
        project=context.cwd,
    )
    if arguments.json:
        print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
        return 0
    for record in records:
        print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
    return 0


def handle_campaign_start_managers(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Start a manager per selected partition of this campaign."""

    report = campaign_managers(
        partitions=arguments.partition or None,
        workers=arguments.workers,
        count=arguments.count,
        adapter_timeout=arguments.adapter_timeout,
        project=context.cwd,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def build_campaign_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``campaign`` group: a partition map over many workspaces."""

    _, group = _group(
        subparsers,
        "campaign",
        summary="partition a large campaign across many workspaces",
        description="Define and drive a campaign that partitions its jobs across many registered workspaces",
    )

    init = _leaf(
        group,
        "init",
        summary="define the partition map and assignment policy",
        description="Define this project's campaign partitions and how root jobs are assigned to them",
        handler=handle_campaign_init,
    )
    init.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME=WORKSPACE",
        help="map one partition name to a registered workspace (repeatable)",
    )
    init.add_argument(
        "--assignment",
        choices=ASSIGNMENT_POLICIES,
        default="hash",
        help="how root jobs pick a partition (default: hash)",
    )

    show = _leaf(
        group,
        "show",
        summary="show the partition map",
        description="Show this project's campaign partition map and assignment policy",
        handler=handle_campaign_show,
    )
    show.add_argument("--json", action="store_true", help="print the campaign as one JSON object")

    submit = _leaf(
        group,
        "submit",
        summary="submit one root job to its assigned partition",
        description="Assign one root job to a partition by policy and submit it into that partition's workspace",
        handler=handle_campaign_submit,
    )
    submit.add_argument("--template", metavar="TEMPLATE", required=True, help="the runner template to scaffold")
    submit.add_argument("--key", metavar="KEY", required=True, help="the tag or key the assignment policy hashes")
    submit.add_argument(
        "--index", type=int, default=0, metavar="N", help="the batch position, for round-robin (default: 0)"
    )
    submit.add_argument(
        "--input", action="append", default=[], dest="inputs", metavar="NAME=VALUE", help="one job input (repeatable)"
    )
    submit.add_argument(
        "--file", action="append", default=[], dest="files", metavar="NAME=PATH", help="one staged file (repeatable)"
    )
    submit.add_argument("--tag", metavar="TAG", help="the job tag")
    submit.add_argument("--placement", metavar="PLACEMENT", help="where the job lands in its workspace")
    submit.add_argument("--priority", type=int, metavar="PRIORITY", help="the job priority")
    submit.add_argument("--name", metavar="NAME", help="a human name for the job")
    submit.add_argument("--json", action="store_true", help="print the scaffolded job as one JSON object")

    harvest_parser = _leaf(
        group,
        "harvest",
        summary="harvest every partition of the campaign",
        description="Harvest the finished jobs of every campaign partition, one workspace after another",
        handler=handle_campaign_harvest,
    )
    harvest_parser.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME",
        help="harvest only this partition (repeatable, default: all of them)",
    )
    harvest_parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=HARVESTABLE_KINDS,
        help=f"state kind to harvest (repeatable, default: {', '.join(DEFAULT_HARVEST_STATES)})",
    )
    harvest_parser.add_argument("--placement", metavar="PLACEMENT", help="harvest only jobs at or below this placement")
    harvest_parser.add_argument("--json", action="store_true", help="print every record as one JSON array")

    managers = _leaf(
        group,
        "start-managers",
        summary="start a manager per selected partition",
        description="Start a manager for each selected partition: in-process for local ones, via the scheduler for remote ones",
        handler=handle_campaign_start_managers,
    )
    managers.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME",
        help="start a manager only for this partition (repeatable, default: all of them)",
    )
    managers.add_argument("--workers", type=int, metavar="COUNT", help="workers per manager")
    managers.add_argument(
        "--count", type=int, default=1, metavar="COUNT", help="remote managers to submit per partition (default: 1)"
    )
    _add_adapter_timeout(managers)


# ---------------------------------------------------------------------------
# The tree, and the three ways in
# ---------------------------------------------------------------------------


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


if __name__ == "__main__":
    raise SystemExit(command(sys.argv[1:], CLIContext("httk", Path.cwd())))
