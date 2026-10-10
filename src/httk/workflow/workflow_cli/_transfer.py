"""Remote and transfer command groups.

Moving jobs between workspaces is being rebuilt on the filesystem kernel
(phase D): the ``transfer`` verbs and their protocol spellings keep their
command tree and help, and every one of them refuses with exit status 2.
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from copy import copy
from io import StringIO
from pathlib import Path
from typing import Any

from httk.core.cli import CLIContext

from .._util import write_json_atomic
from ..adapters import (
    add_remote,
    import_v1_remote,
    list_remotes,
    metadata_path,
    read_credentials,
    read_metadata,
    resolve_remote,
    run_adapter,
    split_settings,
    store_credentials,
)
from ..collecting import COLLECTABLE_KINDS
from ..errors import ResolutionMiss, WorkflowError
from ..hygiene import describe_remote, remove_remote
from ..models import canonical_uuid
from ..registry import LOCAL_REMOTE, list_workspaces, resolve_workspace
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _field,
    _group,
    _leaf,
    _required,
    _settings,
    confirm,
)

# ---------------------------------------------------------------------------
# remote
# ---------------------------------------------------------------------------


def _remote_batch(
    arguments: argparse.Namespace,
    context: CLIContext,
    handler: Any,
    attribute: str,
    *,
    json_output: bool = False,
) -> int:
    """Run a remote leaf sequentially, preserving the existing one-remote code."""

    targets = getattr(arguments, attribute)
    assert isinstance(targets, list)
    json_output |= getattr(arguments, "json", False)
    results: list[object] = []
    failed = False
    multiple = len(targets) > 1
    for target in targets:
        item = copy(arguments)
        setattr(item, attribute, target)
        try:
            if json_output:
                output = StringIO()
                with redirect_stdout(output):
                    code = handler(item, context)
                results.append(json.loads(output.getvalue()))
            else:
                if multiple:
                    print(f"== remote {target} ==")
                code = handler(item, context)
            failed |= code != 0
        except _ERRORS as exc:
            print(f"remote {target}: {exc}", file=sys.stderr)
            failed = True
    if json_output:
        print(json.dumps(results, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_remote_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the remotes this project and this user define."""

    rows = list_remotes(context.cwd)
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        for row in rows:
            print(f"{row['name']}\t{row['scope']}\t{row['path']}")
    return 0


def handle_remote_add(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create one remote bundle from a packaged adapter template."""

    if isinstance(arguments.name, list):
        return _remote_batch(arguments, context, handle_remote_add, "name")
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
    """Run the ``configure`` or ``check`` verb of one remote adapter.

    ``check`` runs the adapter operation that keeps its historical protocol
    spelling ``install``: it verifies the target has a working httk and never
    installs anything.
    """

    if isinstance(arguments.remote, list):
        return _remote_batch(arguments, context, handle_remote_adapter_operation, "remote", json_output=True)
    operation = arguments.operation
    target = resolve_remote(arguments.remote, project=context.cwd)
    settings = _settings(arguments.set)
    if operation != "configure":
        result = run_adapter(target.bundle, operation, {"settings": settings}, timeout=arguments.adapter_timeout)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1 if result.get("returncode") not in (None, 0) else 0
    unset = arguments.unset
    if any(not key or "=" in key for key in unset):
        raise ValueError("--unset requires a nonempty KEY without '='")
    persistable, credentials = split_settings(settings)
    metadata = read_metadata(target.bundle)
    configured = metadata.setdefault("settings", {})
    if not isinstance(configured, dict):
        raise ValueError("adapter settings are not mutable JSON")
    stored_credentials = read_credentials(target.bundle)
    for key in unset:
        configured.pop(key, None)
    configured.update(persistable)
    result = run_adapter(
        target.bundle,
        operation,
        {"settings": settings},
        timeout=arguments.adapter_timeout,
        unset_settings=unset,
    )
    if result.get("returncode") not in (None, 0):
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1
    if persistable or unset:
        write_json_atomic(metadata_path(target.bundle), metadata)
    if credentials or set(unset) & stored_credentials.keys():
        path = store_credentials(target.bundle, credentials, unset=unset)
        if credentials:
            names = ", ".join(sorted(credentials))
            print(
                f"stored {names} for remote {target.name} in {path}; "
                "values there are excluded from signed project manifests",
                file=sys.stderr,
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    print(
        f"configured; verify httk is available there with: httk remote check {arguments.remote}",
        file=sys.stderr,
    )
    return 0


def handle_remote_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Map one recognized legacy httk v1 computer bundle into a remote."""

    if isinstance(arguments.source, list):
        if arguments.name and len(arguments.source) != 1:
            raise ValueError("remote import-v1 --name requires exactly one SOURCE")
        return _remote_batch(arguments, context, handle_remote_import_v1, "source")
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
        _field(
            "valid",
            "yes" if description.get("valid") else f"no: {description.get('problem')}",
        ),
        _field("timeout_seconds", description.get("timeout_seconds")),
        _field(
            "required_binaries",
            ", ".join(description.get("required_binaries", [])) or "-",
        ),
        _field("credentials_file", description.get("credentials_file") or "-"),
    ]
    settings = description.get("settings", {})
    if isinstance(settings, dict):
        for key, value in sorted(settings.items()):
            lines.append(f"{key}={value}")
    for key in description.get("credential_keys", []):
        lines.append(f"{key}=<credential>")
    return "\n".join(lines)


def handle_remote_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one remote: where it lives, what it is, how it is configured."""

    if isinstance(arguments.name, list):
        return _remote_batch(arguments, context, handle_remote_show, "name")
    description = describe_remote(arguments.name, project=context.cwd)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_remote(description))
    return 0


def handle_remote_remove(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one remote bundle, after asking unless told not to (``--force`` skips the confirmation)."""

    if isinstance(arguments.name, list):
        return _remote_batch(arguments, context, handle_remote_remove, "name", json_output=True)
    if not confirm(f"remove the remote {arguments.name!r} and everything configured in it?", force=arguments.force):
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

    listing = _leaf(
        group,
        "list",
        summary="list the remotes this project can reach",
        description="List the remotes this project and this user define",
        handler=handle_remote_list,
    )
    listing.add_argument("--json", action="store_true", help="print the remotes as one JSON array")

    add = _leaf(
        group,
        "add",
        summary="create a remote from a packaged template",
        description="Create one remote bundle from a packaged adapter template",
        handler=handle_remote_add,
    )
    add.add_argument("name", metavar="NAME", nargs="+", help="the name this remote is addressed by")
    add.add_argument(
        "--template",
        metavar="TEMPLATE",
        help="local, ssh, mount or mount-daemon (default: local)",
    )
    add.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the remote for this user rather than for this project",
    )
    add.add_argument(
        "--non-interactive",
        action="store_true",
        help="never prompt; refuse a missing value",
    )

    # The check verb runs the adapter operation whose frozen protocol spelling
    # is "install"; the maintained adapters only ever verify, never install.
    for verb, operation, summary in (
        ("configure", "configure", "run the adapter's configure operation"),
        ("check", "install", "check that httk answers on the remote"),
    ):
        parser = _leaf(
            group,
            verb,
            summary=summary,
            description=f"Run the {verb} operation of one remote adapter",
            handler=handle_remote_adapter_operation,
        )
        parser.set_defaults(operation=operation)
        parser.add_argument(
            "remote", metavar="NAME", nargs="+", help="the remote name; ':' addresses a workspace on a remote"
        )
        parser.add_argument(
            "--set",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help=(
                "one adapter setting, e.g. host=, username=, port=, httk_command=, or "
                "prelude= (shell run before each remote command, e.g. module loads and a "
                "venv activation); a secret one is stored in credentials.json (repeatable)"
            ),
        )
        parser.add_argument(
            "--adapter-timeout",
            type=float,
            metavar="SECONDS",
            help="bound this adapter operation (default: the remote's timeout_seconds)",
        )
        if operation == "configure":
            parser.add_argument(
                "--unset",
                action="append",
                default=[],
                metavar="KEY",
                help="remove a stored setting or credential before applying --set (repeatable)",
            )

    imported = _leaf(
        group,
        "import-v1",
        summary="map a legacy computer bundle",
        description="Map recognized legacy httk v1 computer bundles into remote adapter bundles",
        handler=handle_remote_import_v1,
    )
    imported.add_argument("source", metavar="SOURCE", nargs="+", help="legacy computer directories to read")
    imported.add_argument("--name", metavar="NAME", help="the name to define for one source (default: the legacy one)")
    imported.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the remote for this user rather than for this project",
    )

    show = _leaf(
        group,
        "show",
        summary="describe one remote",
        description="Describe one remote: where it lives, what it is, and how it is configured",
        handler=handle_remote_show,
    )
    show.add_argument("name", metavar="NAME", nargs="+", help="the remote to describe")
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    remove = _leaf(
        group,
        "remove",
        summary="remove one remote bundle",
        description="Remove one remote bundle",
        handler=handle_remote_remove,
    )
    remove.add_argument("name", metavar="NAME", nargs="+", help="the remote to remove")
    remove.add_argument(
        "--force",
        action="store_true",
        help="skip the confirmation",
    )

    from ._daemon_remote import build_daemon_remote_parser

    build_daemon_remote_parser(group)


# ---------------------------------------------------------------------------
# transfer: unavailable until phase D
# ---------------------------------------------------------------------------

_UNAVAILABLE = (
    "is unavailable in this development version: moving jobs between workspaces is being rebuilt "
    "on the filesystem kernel"
)


def _refuse(verb: str) -> int:
    print(f"{verb} {_UNAVAILABLE}", file=sys.stderr)
    return 2


def _protocol_workspace(value: str, context: CLIContext) -> Workspace:
    """Resolve a protocol workspace name, with narrow legacy path support."""

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = Path(context.cwd) / candidate
    path_like = (
        value in {".", ".."} or ":" in value or os.sep in value or (os.altsep is not None and os.altsep in value)
    )
    # A path-like value never reads the registry through resolve_workspace (it
    # fails name validation first), so read it explicitly: a corrupt registry
    # must surface rather than be masked by the path fallback.
    list_workspaces()
    try:
        binding = resolve_workspace(value, project=context.cwd)
    except ResolutionMiss:
        if not path_like and not candidate.is_dir():
            raise
        return Workspace(candidate)
    if binding.remote != LOCAL_REMOTE or binding.path is None:
        raise ValueError(f"protocol workspace must be local on this machine: {value}")
    return Workspace(binding.path)


def handle_transfer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Refuse ``job transfer SRC DST``: moving jobs is unavailable in this development version.

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 2.
    """

    return _refuse("job transfer")


def run_transfer_verb_result(arguments: argparse.Namespace, context: CLIContext) -> Mapping[str, object]:
    """Refuse a parsed transfer: moving jobs is unavailable in this development version.

    :param arguments: The parsed transfer arguments.
    :param context: The CLI invocation.
    :return: Never returns.
    :raises httk.workflow.errors.WorkflowError: Always.
    """

    raise WorkflowError(f"transfer {_UNAVAILABLE}")


def handle_transfer_receive(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Refuse the ``transfer receive`` protocol command (unavailable in this development version).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 2.
    """

    return _refuse("transfer receive")


def handle_transfer_offer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Refuse the ``transfer offer`` protocol command (unavailable in this development version).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 2.
    """

    return _refuse("transfer offer")


def handle_transfer_retire(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Refuse ``transfer retire`` (unavailable in this development version).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 2.
    """

    return _refuse("transfer retire")


def handle_transfer_reclaim(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Refuse ``transfer reclaim`` (unavailable in this development version).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 2.
    """

    return _refuse("transfer reclaim")


def handle_transfer_status(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Refuse ``transfer status`` (unavailable in this development version).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 2.
    """

    return _refuse("transfer status")


def _dispatch_transfer_protocol(tokens: Sequence[str], context: CLIContext) -> int:
    """Parse and run one ``receive``/``offer``/``retire`` protocol command or ``reclaim``/``status`` operator verb."""

    parser = argparse.ArgumentParser(prog="httk workflow transfer", add_help=True)
    protocol = parser.add_subparsers(dest="_which", required=True)

    receive = protocol.add_parser("receive")
    receive.add_argument("--workspace", metavar="WORKSPACE", required=True)
    receive.add_argument("--bundle", metavar="BUNDLE", required=True, action="append")
    receive.set_defaults(handler=handle_transfer_receive)

    offer = protocol.add_parser("offer")
    offer.add_argument("workspace", metavar="WORKSPACE")
    offer.add_argument("--destination-workspace-id", metavar="UUID", required=True)
    offer.add_argument("--job", action="append", default=[], dest="jobs", metavar="JOB_ID")
    offer.add_argument("--state", action="append", metavar="STATE", choices=COLLECTABLE_KINDS)
    offer.add_argument("--placement", metavar="PLACEMENT")
    offer.add_argument("--json", action="store_true")
    offer.add_argument("--environment-settings", help=argparse.SUPPRESS)
    offer.add_argument("--strict-environment", action="store_true", help=argparse.SUPPRESS)
    offer.set_defaults(handler=handle_transfer_offer)

    # Protocol: `retire WORKSPACE JOB_ID...` (a peer always names the workspace
    # first). Operator: `retire [--workspace WORKSPACE] JOB_ID...`, whose first
    # word is a job UUID, which no workspace name is taken to be.
    retire = protocol.add_parser("retire")
    retire.add_argument("words", metavar="[WORKSPACE] JOB_ID", nargs="+")
    retire.add_argument("--workspace", dest="operator_workspace", metavar="WORKSPACE")
    retire.add_argument("--destination-workspace-id", metavar="UUID")
    retire.add_argument("--json", action="store_true")
    retire.add_argument("--acknowledgements-json")
    retire.set_defaults(handler=handle_transfer_retire)

    reclaim = protocol.add_parser("reclaim")
    reclaim.add_argument("jobs", metavar="JOB_ID", nargs="+")
    reclaim.add_argument("--workspace", dest="operator_workspace", metavar="WORKSPACE")
    reclaim.add_argument("--json", action="store_true")
    reclaim.set_defaults(handler=handle_transfer_reclaim)

    status = protocol.add_parser("status")
    status.add_argument("--workspace", dest="operator_workspace", metavar="WORKSPACE")
    status.add_argument("--json", action="store_true")
    status.set_defaults(handler=handle_transfer_status)

    try:
        arguments = parser.parse_args(list(tokens))
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    return arguments.handler(arguments, context)


def build_transfer_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``transfer`` verb: move jobs between two workspace names or directories."""

    transfer = _leaf(
        subparsers,
        "transfer",
        summary="move jobs between two registered workspace names or workspace directories (unavailable in this version)",
        description=(
            "Unavailable in this version: moving jobs between workspaces is being rebuilt, and the verb "
            "refuses with exit status 2. Move jobs between two workspaces: `transfer [OPTIONS] SRC DST`. "
            "Each of SRC and DST is tried "
            "first as a registered workspace name, and then as a workspace directory (one containing "
            ".httk-workspace/); a registered name always wins over a same-named directory, so `./NAME` "
            "addresses the directory unambiguously. It works whichever way the workspaces point — local to "
            "remote, remote to local, local to local, or remote to remote (relayed through this client). The "
            "hidden receive/offer/retire spellings remain protocol, under `httk workflow transfer`, for "
            "remote peers to invoke by exact name. Operators use `transfer status [--workspace WS] [--json]` "
            "(what waits for an operator; exit 1 when something does), `transfer retire [--workspace WS] "
            "JOB_ID...` and `transfer reclaim [--workspace WS] JOB_ID...`."
        ),
        handler=handle_transfer,
    )
    transfer.add_argument(
        "source", metavar="SRC", help="the workspace the jobs leave: a registered name, or a workspace directory"
    )
    transfer.add_argument(
        "destination",
        metavar="DST",
        help="the workspace the jobs arrive in: a registered name, or a workspace directory",
    )
    transfer.add_argument(
        "--job",
        action="append",
        default=[],
        dest="jobs",
        metavar="JOB_ID",
        help="a local-source job UUID, unique prefix, or path; remote sources require canonical job IDs (repeatable)",
    )
    transfer.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=COLLECTABLE_KINDS,
        help="state kind to move when fetching (repeatable, default: succeeded, failed)",
    )
    transfer.add_argument("--placement", metavar="PLACEMENT", help="move only jobs at or below this placement")
    transfer.add_argument(
        "--destination-placement",
        metavar="PLACEMENT",
        help="where the jobs land (default: their placement)",
    )
    _add_adapter_timeout(transfer)
    transfer.add_argument(
        "--strict-environment",
        action="store_true",
        help="block before moving state when destination environment precheck is unavailable or unresolved",
    )
    transfer.add_argument("--json", action="store_true", help="print what moved as one JSON document")


def build_transfer_operator_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare operator transfer inspection and recovery commands."""

    _, group = _group(
        subparsers,
        "transfer",
        summary="inspect and resolve workspace transfers (unavailable in this version)",
        description="Inspect transfers requiring attention and explicitly retire or reclaim them (unavailable in this version)",
    )
    for name, summary, handler in (
        ("status", "report transfers requiring attention", handle_transfer_status),
        ("retire", "retire acknowledged transfers", handle_transfer_retire),
        ("reclaim", "reclaim in-doubt transfers after the safety deadline", handle_transfer_reclaim),
    ):
        parser = _leaf(
            group,
            name,
            summary=f"{summary} (unavailable in this version)",
            description=f"{summary.capitalize()} (unavailable in this version)",
            handler=handler,
        )
        parser.add_argument(
            "--workspace",
            dest="operator_workspace",
            metavar="WORKSPACE",
            help="workspace name (default: selected workspace)",
        )
        parser.add_argument("--json", action="store_true", help="print the result as JSON")
        if name == "retire":
            parser.add_argument(
                "words",
                metavar="JOB_ID",
                nargs="+",
                type=lambda value: canonical_uuid(value, "job_id"),
                help="job IDs whose transfers to retire",
            )
            parser.set_defaults(destination_workspace_id=None, acknowledgements_json=None)
        elif name == "reclaim":
            parser.add_argument("jobs", metavar="JOB_ID", nargs="+", help="job IDs whose transfers to reclaim")
