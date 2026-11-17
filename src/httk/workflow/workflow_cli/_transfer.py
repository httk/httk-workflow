"""Remote and transfer command groups.

``job transfer`` moves jobs between workspaces as hold + copy + adopt + release
on :mod:`httk.workflow._moving`, locally or through the remote adapters'
``invoke``, ``deliver`` and ``pull``; ``job push``/``job pull`` are its everyday forms (an implicit
``--state ready``/``succeeded``, ``--tree``, the default workspace as the omitted end);
``transfer status`` lists held transfers. ``job push`` (unless ``--no-keep``) and ``job transfer --keep``
release the hold keeping the moved jobs in SRC as ``away`` copies, which a later transfer back replaces.
"""

import argparse
import contextlib
import json
import os
import re
import shlex
import stat
import sys
import tarfile
import tempfile
import time
from collections.abc import Callable, Collection, Iterator, Sequence
from contextlib import redirect_stdout
from copy import copy
from dataclasses import dataclass, replace
from functools import partial
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast

from httk.core.cli import CLIContext

from .. import _fs, _kernel, _landing, _moving, _pool, _store
from .._bundles import BundleError, read_manifest
from .._frames import DATA_LIMIT, StreamReader
from .._manager_scheduling import current_step
from .._util import write_json_atomic
from ..adapters import (
    REMOTE_JOB_ADOPT_COMMAND,
    REMOTE_JOB_TRANSFER_COMMAND,
    REMOTE_TRANSFER_STATUS_COMMAND,
    RequestAborted,
    SessionFailed,
    SessionUnsupported,
    TransferSession,
    add_remote,
    import_v1_remote,
    list_remotes,
    metadata_path,
    read_credentials,
    read_metadata,
    remote_settings,
    resolve_remote,
    run_adapter,
    split_settings,
    store_credentials,
)
from ..errors import WorkflowError
from ..hygiene import describe_remote, remove_remote
from ..introspection import iter_jobs, read_job, read_state, resolve_job_selectors
from ..models import WORKSPACE_DIRECTORY, canonical_uuid, normalize_placement
from ..registry import LOCAL_REMOTE, WorkspaceBinding, default_workspace
from ..seals import require_cli_modifiable
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _add_step_option,
    _durable,
    _field,
    _group,
    _leaf,
    _required,
    _run_remote_workspace,
    _settings,
    _workspace_argument,
    add_durability_arguments,
    confirm,
    remote_workspace_output,
)
from ._describe import _WORKFLOW_COMMAND, _stdout_to_stderr

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
    if json_output and results:
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
                "cli_prelude= (shell run before each httk command the CLI runs on the remote "
                "over ssh/mount, e.g. module loads and a venv activation; not at job start "
                "-- see environment.prelude; the tar push/pull stream is exempt), or "
                "interactive_auth= (yes|per-call lets ssh prompt in the "
                "terminal for a password or second factor); a secret one is stored in "
                "credentials.json (repeatable)"
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
        description=(
            "Remove one remote bundle; refused while a workspace registered on this machine holds a transfer "
            "bound for a workspace on it (holds in other workspaces are not checked)"
        ),
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
# job transfer: hold, copy, adopt, release
# ---------------------------------------------------------------------------

#: The format of ``transfer status --json``.
TRANSFER_STATUS_FORMAT = "httk-workflow-transfer-status"
_TRANSFER_ID = re.compile(r"[0-9a-f]{32}")
#: Transfer outcomes that moved the jobs.
_DELIVERED = ("adopted", "already_adopted")
#: The states a transfer selects from: never ``away``, whose copies stay until recovered or replaced.
_MOVABLE_STATES = tuple(state for state in _kernel.UNOWNED_STATES if state != "away")


def _workspace_reference(value: str, context: CLIContext) -> Path | WorkspaceBinding:
    """Resolve a registered name (``REMOTE:WS`` included) or a workspace directory: a local root or a remote binding."""

    binding, root = _workspace_argument(value, context)
    if root is not None:
        return root
    assert binding is not None
    return binding


def _protocol_workspace(value: str, context: CLIContext) -> Workspace:
    """Resolve a protocol workspace name, with narrow legacy path support."""

    reference = _workspace_reference(value, context)
    if isinstance(reference, WorkspaceBinding):
        raise ValueError(f"protocol workspace must be local on this machine: {value}")
    return Workspace(reference)


@dataclass(frozen=True)
class _End:
    """One end of a transfer: a local workspace, or a workspace on a remote."""

    locator: str
    workspace: Workspace | None = None
    binding: WorkspaceBinding | None = None
    #: A local workspace's id; a remote one's is learnt from its adoption report, never probed.
    workspace_id: str | None = None

    @property
    def plain(self) -> str:
        assert self.binding is not None
        return self.binding.name.split(":", 1)[1]


def _end(value: str | None, context: CLIContext, arguments: argparse.Namespace) -> _End:
    """Resolve one end; *value* ``None`` is the enclosing or default workspace. A remote one is not contacted: its
    commands name the workspace by its plain name, which the far side resolves in its own registry."""

    reference: Path | WorkspaceBinding
    if value is not None:
        reference = _workspace_reference(value, context)
    elif (discovered := Workspace.discover(context.cwd)) is not None:
        reference = discovered
    else:
        binding = default_workspace(project=context.cwd)
        local = binding.remote == LOCAL_REMOTE and binding.path is not None
        reference = Path(str(binding.path)) if local else binding
    if isinstance(reference, Path):
        workspace = Workspace(reference, durable=_durable(arguments))
        return _End(str(workspace.root), workspace=workspace, workspace_id=workspace.workspace_id)
    return _End(reference.name, binding=reference)


def _remote(
    end: _End, context: CLIContext, arguments: argparse.Namespace, argv: Sequence[str], cwd: str | None = None
) -> tuple[int, str, str]:
    assert end.binding is not None
    return remote_workspace_output(end.binding, context, argv, timeout=arguments.adapter_timeout, cwd=cwd)


#: The argument text one remote call carries at most: ssh hands the far side one command string, and Linux
#: limits a single argument string to 128 KiB.
_ARGUMENT_BUDGET = 64 * 1024


def _chunks(items: list[str], *, flag: str = "") -> list[list[str]]:
    """Split *items* into runs whose argument text (each item preceded by *flag*) fits :data:`_ARGUMENT_BUDGET`."""

    chunks: list[list[str]] = []
    size = _ARGUMENT_BUDGET
    for item in items:
        cost = len(shlex.quote(item)) + 1 + (len(flag) + 1 if flag else 0)
        if size + cost > _ARGUMENT_BUDGET:
            chunks.append([])
            size = 0
        chunks[-1].append(item)
        size += cost
    return chunks


def _adapter(
    end: _End,
    context: CLIContext,
    arguments: argparse.Namespace,
    operation: str,
    source: str,
    destination: str,
    files: list[str],
) -> None:
    """One adapter ``push`` or ``pull`` of *files* (relative to *source*, each landing below *destination*)."""

    assert end.binding is not None
    target = resolve_remote(end.binding.remote, project=context.cwd)
    run_adapter(
        target.bundle,
        operation,
        {"source": source, "destination": destination, "files": files},
        timeout=arguments.adapter_timeout,
    )


def _say(arguments: argparse.Namespace, message: str) -> None:
    """Print one progress line to stderr, unless ``--quiet``."""

    if not arguments.quiet:
        print(message, file=sys.stderr, flush=True)


def _owner(workspace: Workspace, label: str) -> _kernel.Owner:
    return _kernel.register_owner(workspace, kind="cli", label=label, allocation=None, advertised={})


def run_batch[T, R](
    owner: _kernel.Owner,
    function: Callable[[T], R],
    items: Sequence[T],
    workers: int,
    *,
    serial: Collection[int] = (),
    emit: Callable[[int, R], None] | None = None,
) -> list[R]:
    """Run *function* on every item of one command of *owner*: up to *workers* at once, then the items at the
    indices in *serial* one after another, in order; *emit* sees every result in input order.

    While a pool runs, *owner* counts one operation in flight (:meth:`~httk.workflow._kernel.Owner.enter_flight`),
    left only once no pooled item can run any more, so that the owner fail-stops rather than closes under one.
    With ``workers <= 1`` this is the plain loop over every item in order. When an item fails, the results of the
    items that finished are still emitted, in input order, before the failure is raised.

    :param owner: The command's owner.
    :param function: The per-item work, which must not print.
    :param items: The items.
    :param workers: The ``io.workers`` of the workspace the items act on.
    :param serial: The indices of items whose outcome depends on the order of the others.
    :param emit: Called as ``emit(index, result)`` in the calling thread, in input order.
    :return: The results, in input order.
    """

    results: dict[int, R] = {}
    shown = 0

    def record(index: int, result: R) -> None:
        nonlocal shown
        results[index] = result
        while shown in results:
            shown += 1
            if emit is not None:
                emit(shown - 1, results[shown - 1])

    pooled = [] if workers <= 1 else [index for index in range(len(items)) if index not in serial]
    pool = len(pooled) > 1
    if pool:
        owner.enter_flight()
    try:
        _pool.run_ordered(
            lambda index: function(items[index]),
            pooled,
            workers,
            lambda position, result: record(pooled[position], result),
            # Left only once no pooled item can run any more, while a pool's SIGINT handler is still installed.
            on_quiet=owner.leave_flight if pool else None,
        )
        for index in range(len(items)):
            if index not in results:
                record(index, function(items[index]))
    except BaseException:
        if emit is not None:
            with contextlib.suppress(BaseException):  # the first exception is the one raised
                for index in sorted(index for index in results if index >= shown):
                    emit(index, results[index])  # held back behind a serial item that did not run
        raise
    return [results[index] for index in range(len(items))]


def _repeats(items: Sequence[str]) -> set[int]:
    """The indices of items that repeat an earlier one."""

    first: dict[str, int] = {}
    for index, item in enumerate(items):
        first.setdefault(item, index)
    return {index for index, item in enumerate(items) if first[item] != index}


def eject_once(
    workspace: Workspace,
    owner: _kernel.Owner,
    ref: _kernel.JobRef,
    destination: Path | None,
    *,
    tree: bool,
    locator: str | None = None,
    destination_workspace_id: str | None = None,
    owner_key: str | None = None,
    workflows: bool = False,
) -> _moving.EjectReport | _moving.Hold | str:
    """Claim and eject one job, or hold it when *destination* is ``None``: the report, or why it cannot move now.

    :param workspace: The workspace.
    :param owner: The ejecting owner.
    :param ref: The job.
    :param destination: Where the bundle goes; ``None`` holds it in ``transfers/outgoing/``.
    :param tree: Move the job's descendants too.
    :param locator: The transfer destination a hold records.
    :param destination_workspace_id: The destination workspace's id a hold records.
    :param owner_key: The optional key that may control the adopted root through the workspace daemon.
    :param workflows: Have a hold report the workflows its members need.
    :return: The report (the hold, when held), or the reason; a job that cannot move stays where it is (as after
        any raised error).
    """

    root = _kernel.claim(workspace, owner, ref)
    if root is None:
        return f"{ref.job_key} moved before it could be claimed"
    try:
        if destination is not None:
            return _moving.eject(workspace, owner, root, destination=destination, tree=tree, owner_key=owner_key)
        return _moving.hold(
            workspace,
            owner,
            root,
            tree=tree,
            destination_locator=locator,
            destination_workspace_id=destination_workspace_id,
            workflows=workflows,
        )
    except _moving.Busy as exc:
        return str(exc)


def adopt_document(report: _moving.AdoptReport) -> dict[str, object]:
    """Return the JSON document ``job adopt --json`` prints for *report*.

    :param report: The adoption report.
    :return: The document.
    """

    return {
        "already_adopted": report.already_adopted,
        "published": [{"job_key": ref.job_key, "state": ref.state, "path": str(ref.path)} for ref in report.published],
        "missing_workflows": list(report.missing_workflows),
    }


def _holds(
    src: _End,
    context: CLIContext,
    arguments: argparse.Namespace,
    *,
    workflows: bool = True,
    source: "_SourceSession | None" = None,
) -> list[_moving.Hold]:
    """The holds of *src*; a local *src* reports their workflows only with *workflows*. A remote *src* answers
    ``transfer status --json``, with one ``holds`` request on the pull's transfer session (*source*) when it serves
    one."""

    if src.workspace is not None:
        return _moving.held(src.workspace, workflows=workflows)
    session = None if source is None else source.get()
    document: Any = None
    if session is not None:
        assert source is not None
        first = source.first_request()
        response = session.request("holds", {"workspace": src.plain})
        if response.get("ok") is not True and response.get("too_large") is True and first:
            # Nothing was changed: the whole command takes the adapter operations.
            source.fall_back(f"transfer status on {src.locator}: {response.get('error')}")
            session = None
        elif response.get("ok") is not True:
            raise RuntimeError(f"transfer status failed on {src.locator}: {response.get('error')}")
        else:
            document = response.get("document")
    if session is None:
        code, out, err = _remote(src, context, arguments, [*REMOTE_TRANSFER_STATUS_COMMAND, "--json", src.plain])
        if code:
            raise RuntimeError(f"transfer status failed on {src.locator}: {err.strip()}")
        document = json.loads(out)
    if not isinstance(document, dict) or not isinstance(document.get("holds"), list):
        raise ValueError(f"{src.locator} returned an invalid transfer status")
    return [_moving.Hold.from_mapping(hold) for hold in document["holds"]]


def _parent_here(
    ref: _kernel.JobRef, present: set[str], *, step: bool
) -> tuple[bool, str | None, str | None, str | None]:
    """Return whether *ref* moves only with its root, (with *step*) its step or why that is unknown, and the job
    id of its ``job.json`` parent (``None`` without one, or when ``job.json`` is unreadable).

    A job moves only in its root's tree when it is a non-detached child of a job
    in *present*. ``state.json`` is read only for such a child or for the step.
    """

    job, job_error = read_job(ref)
    if job is None:
        return False, None, f"{ref.job_key}: {job_error}" if step else None, None
    parent = None if job.parent is None else str(job.parent.get("job_id"))
    child = parent is not None and parent in present
    if not (child or step):
        return False, None, None, parent
    doc, state_error = read_state(ref)
    if step and state_error is not None:
        return False, None, f"{ref.job_key}: its step is unknown: {state_error}", parent
    return child and (doc is None or doc.detached is None), current_step(job, doc) if step else None, None, parent


def hold_selected(
    workspace: Workspace,
    context: CLIContext,
    arguments: argparse.Namespace,
    errors: list[str],
    *,
    locator: str,
    destination_id: str | None,
    workflows: bool,
    progress: Callable[[str, int | None], object] | None = None,
    before_holding: Callable[[int], object] | None = None,
) -> list[_moving.Hold]:
    """Hold every job the selection of *arguments* names in the local *workspace* for a transfer to *locator*.

    The selection is ``jobs``, ``state``, ``step``, ``placement`` and ``tree`` as ``job transfer`` parses them; a
    job that cannot move is an error appended to *errors*. Notes (a job left out by ``--step``, children that move
    only with their root) go to standard error. ``job transfer`` on a local SRC, ``job transfer --hold`` and the
    transfer session's ``hold`` op share this.

    :param workspace: The source workspace.
    :param context: The CLI invocation (``--job`` selectors resolve against its working directory).
    :param arguments: The selection.
    :param errors: Takes the errors.
    :param locator: The destination each hold records.
    :param destination_id: The destination workspace's id each hold records, if known.
    :param workflows: Have each hold report the workflows its members need.
    :param progress: Told ``list`` with the selectors resolved and jobs looked at while the selection is resolved,
        then, right before the command's owner is registered (from when on anything may have changed), ``hold`` with
        the selected roots handled so far (held or not).
    :param before_holding: Told how many roots were selected once the selection is resolved, before the owner is
        registered; it may raise to hold nothing.
    :return: The holds made, in the order they were made.
    """

    tell = progress or (lambda _phase, _count: None)
    holds: list[_moving.Hold] = []
    looked = 0

    def look() -> None:
        nonlocal looked
        looked += 1
        tell("list", looked)

    def selectors() -> Iterator[str]:
        for selector in arguments.jobs:
            yield selector
            look()  # resolved by the time the next one is asked for

    tell("list", looked)
    require_cli_modifiable(workspace)
    refs = resolve_job_selectors(workspace, context.cwd, selectors())
    steps = None if arguments.step is None else frozenset(arguments.step)
    if steps is not None:
        named, refs = refs, []
        for ref in named:
            look()
            _, step, damage, _ = _parent_here(ref, set(), step=True)
            if damage is not None:
                errors.append(damage)
            elif step in steps:
                refs.append(ref)
            else:
                print(
                    f"{ref.job_key} is at step {step}, not --step {', '.join(sorted(steps))}: left out",
                    file=sys.stderr,
                )
    children: list[_kernel.JobRef] = []
    # Held one by one: the --job selectors, then (concurrently, unless a listed root has its parent here, whose
    # hold looks at that parent's children) the listed roots.
    explicit, sequential = len(refs), False
    if arguments.state or (steps is not None and not arguments.jobs):
        seen = {ref.job_id for ref in refs}
        # Listed in full first: holding moves jobs out of the directories the listing walks.
        states = arguments.state or _MOVABLE_STATES
        listed = []
        for ref in iter_jobs(workspace, states, placement_prefix=arguments.placement):
            look()
            listed.append(ref)
        present = set()
        for ref in iter_jobs(workspace):
            look()
            present.add(ref.job_id)
        for ref in listed:
            if ref.job_id not in seen:
                look()
                seen.add(ref.job_id)
                parent_here, step, damage, parent = _parent_here(ref, present, step=steps is not None)
                if damage is not None:
                    errors.append(damage)
                    continue
                if steps is not None and step not in steps:
                    continue
                # A child whose parent is here moves only in its root's tree, never on its own.
                (children if parent_here else refs).append(ref)
                sequential = sequential or (not parent_here and parent in present)
    moving: set[str] = set()
    handled = 0

    def hold_one(ref: _kernel.JobRef) -> _moving.Hold | str:
        try:
            outcome = eject_once(
                workspace,
                owner,
                ref,
                None,
                tree=arguments.tree,
                locator=locator,
                destination_workspace_id=destination_id,
                workflows=workflows,
            )
        except _ERRORS as exc:
            return f"{ref.job_key}: {exc}"
        assert not isinstance(outcome, _moving.EjectReport)
        return outcome

    def record(_index: int, outcome: _moving.Hold | str) -> None:
        nonlocal handled
        if isinstance(outcome, str):
            errors.append(outcome)
        else:
            holds.append(outcome)
            moving.update(outcome.members)
        handled += 1
        tell("hold", handled)

    if before_holding is not None:
        before_holding(len(refs))
    workers = workspace.io_workers
    tell("hold", handled)  # before the owner's registration, which may leave state behind
    with _owner(workspace, "job transfer") as owner:
        for index, ref in enumerate(refs):
            if index == explicit and not sequential and workers > 1:
                # Disjoint trees: no listed root has a parent here, so none is in another's tree.
                listed_roots = [root for root in refs[index:] if root.job_key not in moving]
                run_batch(owner, hold_one, listed_roots, workers, emit=record)
                break
            if ref.job_key in moving:
                continue  # it went with an earlier selected tree
            record(index, hold_one(ref))
    if left := sum(ref.job_key not in moving for ref in children):
        print(
            f"{left} selected job(s) have a parent in {workspace.root} and move only with their root "
            + ("(detach them to move them alone)" if arguments.tree else "(use --tree, or detach them)"),
            file=sys.stderr,
        )
    return holds


def hold_document(
    workspace: Workspace,
    holds: list[_moving.Hold],
    locator: str,
    errors: list[str],
    progress: Callable[[str, int | None], object] | None = None,
) -> dict[str, Any]:
    """Return the document ``job transfer --hold --json`` prints (and the transfer session's ``hold`` op answers).

    Besides the new *holds* and the *errors*, it lists the other holds of *workspace* already bound for *locator*
    (``held_before``), so that a client needs no ``transfer status`` call; their workflows are resolved for these
    only, not for every hold of the workspace.

    :param workspace: The source workspace.
    :param holds: The holds just made.
    :param locator: The destination they record.
    :param errors: The errors of the hold.
    :param progress: Told ``list`` with the holds read so far (the listing, then the workflows of the earlier ones).
    :return: ``{"holds", "held_before", "errors"}``.
    """

    tell = progress or (lambda _phase, _count: None)
    read_count = 0

    def one() -> None:
        nonlocal read_count
        read_count += 1
        tell("list", read_count)

    tell("list", read_count)
    new = {hold.transfer_id for hold in holds}
    before = []
    for hold in _moving.held(workspace, on_read=one):
        if hold.destination_locator != locator or hold.transfer_id in new:
            continue
        try:
            read = read_manifest(Path(hold.path) / "bundle.json")
        except BundleError:
            read = None  # changed since it was listed: reported without workflows, as by an older SRC
        needed = None if read is None else _moving._needed_workflows(workspace, Path(hold.path), read[1])
        before.append(replace(hold, workflows=needed).as_mapping())
        one()
    return {"holds": [hold.as_mapping() for hold in holds], "held_before": before, "errors": errors}


def _hold_jobs(
    src: _End,
    dst: _End,
    context: CLIContext,
    arguments: argparse.Namespace,
    errors: list[str],
    source: "_SourceSession | None" = None,
) -> tuple[list[_moving.Hold], list[_moving.Hold] | None]:
    """Step 1: hold every selected job on *src*, recording *dst* and its id when known; a job that cannot move is
    an error.

    The result is the new holds, and for a remote *src* the other holds it reported bound for *dst*
    (``held_before``), or ``None`` (a local *src*, or a hold call that printed no document). A remote *src* is held
    over the pull's transfer session (*source*) when it serves one: one ``hold`` request for the whole selection.
    When that session cannot open (other than by not being served) or fails during the hold, the result is no holds
    and an empty ``held_before``, so that nothing is driven: nothing was held, or what was is left held there for
    ``--resume``.
    """

    if src.workspace is not None:
        held = hold_selected(
            src.workspace,
            context,
            arguments,
            errors,
            locator=dst.locator,
            destination_id=dst.workspace_id,
            # Only --hold prints the holds; a transfer drives them from the source's listing.
            workflows=arguments.hold,
        )
        return held, None
    for selector in arguments.jobs:
        try:
            canonical_uuid(selector, "JOB")
        except (WorkflowError, ValueError) as exc:
            raise ValueError(f"a remote source requires canonical job ids: {selector!r}") from exc
    try:
        session = None if source is None else source.get()
    except _ERRORS as exc:
        errors.append(f"{src.locator}: the transfer session did not open: {exc}; nothing was held")
        return [], []
    if session is not None:
        assert source is not None
        source.first_request()
        try:
            return _hold_over_session(session, src, dst, arguments, errors)
        except _TooLarge as exc:
            source.fall_back(str(exc))  # nothing was held: the adapter's hold call holds the selection
    holds: list[_moving.Hold] = []
    before: list[_moving.Hold] | None = []
    argv = [*REMOTE_JOB_TRANSFER_COMMAND, "--hold", "--json", *(["--tree"] if arguments.tree else [])]
    argv += [*(["--destination-id", dst.workspace_id] if dst.workspace_id is not None else [])]
    # --step narrows every selection, the --job ids too, so every call carries it. --state names no ids, so it
    # goes in one call: the last, as a --job of a later call that --state already held would no longer resolve there.
    step_items = [item for step in arguments.step or [] for item in ("--step", step)]
    states = [item for state in arguments.state for item in ("--state", state)]
    states += ["--placement", arguments.placement] if arguments.placement is not None else []
    chunks = _chunks(arguments.jobs, flag="--job") or [[]]
    for index, chunk in enumerate(chunks):
        selected = [item for selector in chunk for item in ("--job", selector)] + step_items
        selected += states if index == len(chunks) - 1 else []
        # Holds may exist after any failure here: the pass over the source's holds, or a later --resume, drives them.
        try:
            code, out, err = _remote(src, context, arguments, [*argv, *selected, "--", src.plain, dst.locator])
        except _ERRORS as exc:
            errors.append(
                f"job transfer --hold on {src.locator} failed: {exc}; jobs it already held are driven by this run's "
                f"pass over {src.locator}'s holds or by `httk job transfer --resume {src.locator} {dst.locator}`"
            )
            before = None
            continue
        try:
            document = json.loads(out)
            if not isinstance(document, dict) or not all(
                isinstance(document.get(key), list) for key in ("holds", "errors")
            ):
                raise ValueError("no hold document")
            held = [_moving.Hold.from_mapping(hold) for hold in document["holds"]]
            # An older SRC lists no held_before: this run then asks transfer status.
            reported = document.get("held_before")
            earlier = [_moving.Hold.from_mapping(hold) for hold in reported] if isinstance(reported, list) else None
        except (ValueError, TypeError):
            message = f"job transfer --hold on {src.locator} printed no hold document"
            message += f": {err.strip()}" if code and err.strip() else ""
            message += f": {out[:200]!r}" if out else ""
            if "unrecognized arguments" in err or "invalid choice" in err:
                message += f" ({src.locator} may run an older httk-workflow; upgrade it)"
            errors.append(message)
            before = None
            continue
        holds += held
        before = None if before is None or earlier is None else [*before, *earlier]
        errors.extend(str(error) for error in document["errors"])
        sys.stderr.write(err)  # the far side's notes, such as children left with their root
    return holds, before


class _TooLarge(RuntimeError):
    """SRC refused a pull's first request because its answer would not fit one frame (``too_large``): nothing was
    changed, and the command takes the adapter operations instead."""


def _hold_over_session(
    session: TransferSession, src: _End, dst: _End, arguments: argparse.Namespace, errors: list[str]
) -> tuple[list[_moving.Hold], list[_moving.Hold] | None]:
    """Hold the selection on a remote *src* with one ``hold`` request on the pull's *session*: what
    :func:`_hold_jobs` returns, its document read as the adapter path reads ``job transfer --hold --json``.

    :param session: The pull's transfer session.
    :param src: The remote source end.
    :param dst: The destination end.
    :param arguments: The selection.
    :param errors: Takes the errors.
    :return: The new holds and ``held_before`` (see :func:`_hold_jobs`).
    :raises _TooLarge: When SRC refused the hold as too large for one answer, holding nothing.
    """

    header: dict[str, Any] = {
        "workspace": src.plain,
        "jobs": list(arguments.jobs),
        "state": list(arguments.state),
        "step": list(arguments.step or []),
        "placement": arguments.placement,
        "tree": bool(arguments.tree),
        "destination": dst.locator,
        "destination_id": dst.workspace_id,
    }
    try:
        response = session.request("hold", header)
    except SessionFailed as exc:
        # Never reconnected: whatever the far side held stays held there.
        errors.append(
            f"job transfer --hold on {src.locator} failed: the transfer session failed: {exc}; the jobs it may have "
            f"held stay held there: `httk job transfer --resume {src.locator} {dst.locator}` drives them"
        )
        return [], []
    sys.stderr.write(str(response.get("stderr", "")))  # the far side's notes, such as children left with their root
    if response.get("ok") is not True and response.get("too_large") is True:
        raise _TooLarge(f"job transfer --hold on {src.locator}: {response.get('error')}")
    if response.get("ok") is not True:
        # Refused before anything was held: the pass over the source's holds still re-drives the earlier ones.
        errors.append(f"job transfer --hold on {src.locator} was refused: {response.get('error')}")
        return [], None
    try:
        if not all(isinstance(response.get(key), list) for key in ("holds", "errors")):
            raise ValueError("no hold document")
        holds = [_moving.Hold.from_mapping(hold) for hold in response["holds"]]
        reported = response.get("held_before")
        before = [_moving.Hold.from_mapping(hold) for hold in reported] if isinstance(reported, list) else None
    except (ValueError, TypeError):
        errors.append(f"job transfer --hold on {src.locator} answered no hold document")
        return [], None
    errors.extend(str(error) for error in response["errors"])
    return holds, before


#: The statuses of a ``job adopt --json`` entry.
_ADOPT_STATUSES = ("adopted", "already_adopted", "refused", "taken", "failed")


def _adopt_local(workspace: Workspace, owner: _kernel.Owner, source: Path, absent: frozenset[str]) -> dict[str, Any]:
    """Adopt one bundle of a batch (*absent* settled for it) into a local *workspace*: a ``job adopt --json`` entry."""

    try:
        # A hold moved in, or a committed landing: durable already.
        report = _moving.adopt(workspace, owner, source, untrusted=False, absent=absent, synced=True)
    except BundleError as exc:
        return {"status": "refused", "message": str(exc)}
    except _ERRORS as exc:
        return {"status": "failed", "message": str(exc)}
    if report is None:
        return {"status": "taken"}
    return {"status": "already_adopted" if report.already_adopted else "adopted", **adopt_document(report)}


def _adoption(out: str, bundles: list[str]) -> dict[str, Any]:
    """The checked document of a remote ``job adopt --json --stdin-tar`` of *bundles* from its printed text (see
    :func:`_checked_adoption`); :class:`ValueError` when it is no such document."""

    return _checked_adoption(json.loads(out), bundles)


def _checked_adoption(document: Any, bundles: list[str]) -> dict[str, Any]:
    """The checked document of a remote ``job adopt --json --stdin-tar`` of *bundles* (or of the transfer session's
    ``adopt`` op): as given, with ``entries`` by bundle added; :class:`ValueError` when it is no such document.

    A document with ``refused`` says that nothing was adopted and lists no bundle, and with ``sync_failed`` true
    that the far side's sync commit failed (a writeback error there, possibly another user's); any other names
    every bundle sent, each with a known status.
    """

    if not isinstance(document, dict) or not isinstance(document.get("bundles"), list):
        raise ValueError("not an adoption report")
    if not isinstance(document.get("sync_failed", False), bool):
        raise ValueError("an ambiguous sync_failed")
    workspace_id = document.get("workspace_id")
    if workspace_id is not None:
        document["workspace_id"] = canonical_uuid(str(workspace_id), "workspace_id")
    if "refused" in document:
        if document["bundles"] or not isinstance(document["refused"], str):
            raise ValueError("an ambiguous refusal")
        return document
    entries = {
        entry["bundle"]: entry
        for entry in document["bundles"]
        if isinstance(entry, dict) and entry.get("status") in _ADOPT_STATUSES
    }
    if workspace_id is None or not all(bundle in entries for bundle in bundles):
        raise ValueError("not an adoption report of the bundles sent")
    document["entries"] = {bundle: entries[bundle] for bundle in bundles}
    return document


def _deliver_call(
    end: _End, context: CLIContext, arguments: argparse.Namespace, source: str, files: list[str], argv: list[str]
) -> tuple[int, str, str]:
    """One adapter ``deliver``: *files* (relative to the local *source*) streamed to *argv* run on *end*."""

    assert end.binding is not None
    target = resolve_remote(end.binding.remote, project=context.cwd)
    result = run_adapter(
        target.bundle,
        "deliver",
        {"source": source, "files": files, "argv": argv},
        timeout=arguments.adapter_timeout,
    )
    return int(result.get("returncode", 0) or 0), str(result.get("stdout", "")), str(result.get("stderr", ""))


def _sync_barrier(
    workspace: Workspace, anchor: Path, arguments: argparse.Namespace, *, inline: bool
) -> contextlib.AbstractContextManager[_fs.Barrier | None]:
    """The sync barrier of a landing in *workspace* (``io.sync`` and ``io.sync_errors`` read afresh); none in a
    non-durable workspace."""

    if not workspace.durable:
        return contextlib.nullcontext(None)
    setting, errors = workspace.io_sync_policy()
    return _fs.Barrier(anchor, setting, errors=errors, inline=inline, quiet=arguments.quiet)


def _other_filesystem(holds: list[_moving.Hold], dst: Workspace) -> bool:
    """Whether the holds (their ``transfers/outgoing/``) are on another filesystem than *dst*'s control directory.

    Equal devices can still refuse a rename (bind mounts): adoption's own cross-filesystem copy remains for those.
    """

    outgoing = Path(holds[0].path).parent
    return os.stat(outgoing).st_dev != os.stat(dst.control).st_dev


def _land_copies(
    workspace: Workspace,
    owner: _kernel.Owner,
    holds: list[_moving.Hold],
    scratch: Path,
    arguments: argparse.Namespace,
) -> list[Path]:
    """Copy every hold into the landing *scratch* of *workspace*, durably before any of it is adopted.

    The copies run with *workspace*'s ``io.workers`` inside one sync barrier (inline fsync per file when it
    resolves to ``fsync``). When the barrier's commit fails, the copies are discarded and made once more with an
    fsync of every file (the holds are untouched); an error of that copy fails the chunk.

    :param workspace: The destination workspace.
    :param owner: The transfer's owner in *workspace*.
    :param holds: The chunk's holds, in a local source workspace.
    :param scratch: The owner's landing scratch.
    :param arguments: The command's arguments (``--quiet``).
    :return: The copies, in the order of *holds*.
    """

    outgoing = scratch / "outgoing"
    targets = [outgoing / hold.transfer_id for hold in holds]

    def copy_all(durable: bool) -> None:
        _fs.make_dirs(outgoing, durable=durable)
        run_batch(
            owner,
            lambda index: _fs.copy_tree(Path(holds[index].path), targets[index], durable=durable),
            range(len(holds)),
            workspace.io_workers,
        )

    try:
        with _sync_barrier(workspace, scratch, arguments, inline=True) as barrier:
            copy_all(barrier is not None and barrier.fsync_inline)
            if barrier is not None:
                barrier.commit(len(holds), unit="bundle")
    except _fs.SyncFailed as exc:
        _say(arguments, f"copying {len(holds)} bundle(s) again, with an fsync of every file: {exc}")
        owner.discard_tree(outgoing)
        copy_all(True)
    return targets


def _deliver(
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
) -> dict[str, dict[str, Any]]:
    """Steps 2 and 3 for a chunk bound for a local *dst*: copy its held bundles there and adopt them.

    Holds on the same filesystem move in (adoption takes them); with ``--keep``, or from another filesystem, they
    are copied into a landing scratch first (see :func:`_land_copies`); a remote *src*'s are pulled into one. A
    landing is made durable (a sync barrier entered before its first byte, committed before any adoption). When
    the ``syncfs`` commit of a pull fails under ``io.sync_errors`` ``fail``, the landing is discarded and the chunk
    pulled once more into a fresh one under a fresh barrier; a second failure (or any other failed commit, such
    as a walk of strategy ``fsync``, never pulled again) fails the chunk with the holds kept at the source.

    The result is each hold's adoption entry by transfer id (``status`` as ``job adopt --json`` gives it); an
    error raised fails the whole chunk.
    """

    assert dst.workspace is not None
    ids = [hold.transfer_id for hold in holds]
    # One workspace's holds share its transfers/outgoing/.
    outgoing = str(PurePosixPath(holds[0].path).parent)
    require_cli_modifiable(dst.workspace)
    failure: Exception | None = None
    entries: dict[str, dict[str, Any]] = {}
    with _owner(dst.workspace, "job transfer") as owner:
        # Errors are kept, not raised, inside: an owner left by an error stays open for recovery.
        scratch = None
        try:
            if src.workspace is not None and not arguments.keep and not _other_filesystem(holds, dst.workspace):
                # One filesystem: adoption moves each hold in, and a refusal moves it back.
                sources = [Path(hold.path) for hold in holds]
            elif src.workspace is not None:
                # Keeping away copies releases the hold itself, and another filesystem cannot take it by a rename:
                # the adoption takes a durable copy of it.
                scratch = owner.scratch("landing")
                sources = _land_copies(dst.workspace, owner, holds, scratch, arguments)
            else:
                _say(arguments, f"copying {len(ids)} bundle(s) from {src.locator}")
                retried = False
                while True:
                    scratch = owner.scratch("landing")
                    failed: _fs.SyncFailed | None = None
                    # Entered (and primed) before the adapter writes anything; committed before anything is adopted.
                    with _sync_barrier(dst.workspace, scratch, arguments, inline=False) as barrier:
                        _adapter(src, context, arguments, "pull", outgoing, str(scratch / "outgoing"), ids)
                        if barrier is not None:
                            try:
                                barrier.commit()
                            except _fs.SyncFailed as exc:
                                # Only a syncfs commit's error under io.sync_errors fail is pulled again (a primed
                                # syncfs barrier never walks): no later sync could clear it, and a clean commit of a
                                # fresh landing proves that one durable. A failed walk of strategy fsync is exact;
                                # under ignore only an unsupported syncfs fails, which a re-pull would not help.
                                if retried or barrier.strategy != "syncfs" or barrier.errors != "fail":
                                    raise
                                failed = exc
                    if failed is None:
                        break
                    retried = True
                    _say(arguments, f"pulling {len(ids)} bundle(s) from {src.locator} again: {failed}")
                    owner.discard_scratch(scratch)
                    scratch = None
                sources = [scratch / "outgoing" / transfer_id for transfer_id in ids]
            _say(arguments, f"adopting {len(ids)} bundle(s) in {dst.locator}")
            workspace = dst.workspace
            settled = _moving.settle_absent(workspace, sources)
            serial = {index for index, source in enumerate(sources) if source in settled.serial}
            adopted = run_batch(
                owner,
                lambda source: _adopt_local(workspace, owner, source, settled.absent),
                sources,
                workspace.io_workers,
                serial=serial,
            )
            entries.update(zip(ids, adopted, strict=True))
        except _ERRORS as exc:
            failure = exc
        finally:
            if scratch is not None:
                owner.discard_scratch(scratch)
    if failure is not None:
        raise failure
    return entries


def _deliver_remote(
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
    token: str,
    built_for: list[str],
    *,
    expected: str | None = None,
) -> dict[str, Any]:
    """Steps 2 and 3 for a chunk bound for a remote *dst*: one ``deliver`` of its held bundles to ``job adopt
    --stdin-tar`` there (a remote *src*'s bundles relayed through a local directory), asking about *built_for*.

    The result is the checked adoption document (see :func:`_adoption`). A :class:`RuntimeError` says that *dst*
    gave no usable answer, so whether anything was adopted is unknown. Once an adoption is read, the far side's
    standard error lines (its sync line, its warnings) are printed here (:func:`_relay`).

    A refusal marked ``sync_failed`` (the far side's sync commit failed under ``io.sync_errors`` ``fail``, maybe
    for another user's writeback error) is delivered once more, the same bundles from the same local directory
    (a remote *src*'s relay is not pulled again), to a fresh token with ``--sync fsync``, whose inline fsyncs no
    foreign error can fail; the answer to that delivery is the result, never retried itself. When it reports
    another ``workspace_id`` than the first, or an answer reports another than *expected* (the one an earlier
    delivery of the same batch reported), the result is unknown (:class:`RuntimeError`).
    """

    ids = [hold.transfer_id for hold in holds]
    outgoing = str(PurePosixPath(holds[0].path).parent)
    if src.workspace is not None:
        _say(arguments, f"delivering {len(ids)} bundle(s) to {dst.locator}")
        return _deliver_adoption(src, dst, context, arguments, outgoing, ids, token, built_for, expected)
    with tempfile.TemporaryDirectory(prefix="httk-transfer-") as relay:
        _say(arguments, f"copying {len(ids)} bundle(s) from {src.locator}")
        for chunk in _chunks(ids):  # a pull names its files on the far side's command line
            _adapter(src, context, arguments, "pull", outgoing, f"{relay}/outgoing", chunk)
        _say(arguments, f"delivering {len(ids)} bundle(s) to {dst.locator}")
        # Inside the relay's lifetime: a retry delivers the same copies again.
        source = f"{relay}/outgoing"
        return _deliver_adoption(src, dst, context, arguments, source, ids, token, built_for, expected)


def _deliver_adoption(
    src: _End,
    dst: _End,
    context: CLIContext,
    arguments: argparse.Namespace,
    source: str,
    ids: list[str],
    token: str,
    built_for: list[str],
    expected: str | None,
) -> dict[str, Any]:
    """Deliver the bundles *ids* in the local directory *source* to ``job adopt --stdin-tar`` on *dst*, once more
    with ``--sync fsync`` after a ``sync_failed`` refusal: the checked adoption document (see
    :func:`_deliver_remote`), whose ``workspace_id`` is *expected* unless either is unknown."""

    def deliver(landing: str, fsync: bool) -> tuple[dict[str, Any], str]:
        argv = [*REMOTE_JOB_ADOPT_COMMAND, "--json", "--stdin-tar", "--token", landing]
        argv += [
            *(["--sync", "fsync"] if fsync else []),
            "--workspace",
            dst.plain,
            *(item for workflow_id in built_for for item in ("--built-for", workflow_id)),
        ]
        code, out, err = _deliver_call(dst, context, arguments, source, ids, argv)
        try:
            return _adoption(out, ids), err
        except (ValueError, TypeError, WorkflowError):
            detail = err.strip() if code and err.strip() else f"{dst.locator} returned an unrecognised adoption report"
            if "unrecognized arguments" in err or "invalid choice" in err:
                detail += f" ({dst.locator} may run an older httk-workflow; upgrade it)"
            raise _unknown_adoption(detail, src, dst, arguments) from None

    document, err = _adopt_with_retry(deliver, src, dst, arguments, len(ids), token, expected)
    if "refused" not in document:
        _relay(dst, arguments, err)
    return document


def _adopt_with_retry(
    deliver: Callable[[str, bool], tuple[dict[str, Any], str]],
    src: _End,
    dst: _End,
    arguments: argparse.Namespace,
    count: int,
    token: str,
    expected: str | None,
    *,
    fsync: bool = False,
) -> tuple[dict[str, Any], str]:
    """Deliver once to *token* through *deliver* (``deliver(token, fsync)``: the checked adoption document and the
    far side's standard error), and once more to a fresh token with ``--sync fsync`` after a ``sync_failed``
    refusal; the answer to that is the result, never retried itself. The rules of a push, shared by a delivery
    through the adapter and a segment over a transfer session.

    :param deliver: One delivery of the *count* bundles.
    :param src: The source end.
    :param dst: The destination end.
    :param arguments: The command's arguments.
    :param count: How many bundles one delivery carries.
    :param token: The first delivery's landing.
    :param expected: The ``workspace_id`` an earlier delivery of the batch answered, if any.
    :param fsync: Ask for ``--sync fsync`` already in the first delivery.
    :return: The checked adoption document and the standard error of the delivery that gave it.
    :raises RuntimeError: For an answer whose ``workspace_id`` differs from *expected*, or from the first answer
        of a retry (an unknown outcome).
    """

    # The answers compared: this one, and the one that answered first.
    retried = ("the second delivery", "the first delivery")
    batch = ("this delivery", "the batch's earlier delivery")

    def same_workspace(document: dict[str, Any], first: str | None, pair: tuple[str, str]) -> None:
        second = document.get("workspace_id")
        if first is not None and second is not None and second != first:
            if "refused" not in document:
                # The jobs arrived, but in another workspace than the one that answered first: nothing is
                # released, and no away copy can name the right destination.
                raise RuntimeError(
                    f"{dst.locator} adopted the jobs into workspace {second}, not the expected workspace {first} "
                    f"that answered {pair[1]}; the holds are kept on {src.locator}: check with `httk job "
                    f"show` on {dst.locator}, then `httk job transfer --resume {src.locator} {dst.locator}` reports "
                    "them already adopted there and releases the holds"
                )
            detail = f"{dst.locator} answered {pair[0]} as workspace {second}, {pair[1]} as {first}"
            raise _unknown_adoption(detail, src, dst, arguments)

    document, err = deliver(token, fsync)
    same_workspace(document, expected, batch)
    if "refused" in document and document.get("sync_failed") is True:
        _say(
            arguments,
            f"{dst.locator} could not make the delivery durable ({document['refused']}); delivering {count} "
            "bundle(s) again, with an fsync of every file",
        )
        answered = document.get("workspace_id")
        document, err = deliver(_fs.fresh_token(), True)
        if answered is None:
            # The first attempt named no workspace: the retry is held to the batch's earlier delivery.
            same_workspace(document, expected, ("the second delivery", batch[1]))
        else:
            same_workspace(document, answered, retried)
    return document, err


def _unknown_adoption(detail: str, src: _End, dst: _End, arguments: argparse.Namespace) -> RuntimeError:
    """The error of a delivery without a usable answer: whether the jobs arrived is unknown, the holds stay."""

    return RuntimeError(
        f"{detail}; whether the jobs arrived is unknown: check with `httk job show` on {dst.locator}, then "
        f"deliver again with `httk job transfer --resume {src.locator} {dst.locator}` or, if they are there, "
        f"release with {_release_remedy(arguments, 'TRANSFER_ID', None, src, dst)}"
    )


#: The far side's standard error lines of an adoption that a push relays: the sync barrier's progress line,
#: ``warning:`` lines, and the barrier's and the sync settings' own logged warnings, which the far side's httk
#: renders as their bare message (so they are told apart by their fixed beginnings). Nothing else (ssh warnings, login banners, module
#: system output, tracebacks, a bundle's own message and its continuation lines) is relayed.
_RELAYED = ("syncing ", "made ", "warning:", "io.sync ", "io.sync_errors ", "the sync barrier")
#: Relayed lines the client reports itself (its workflow shipping), left out.
_REPORTED_HERE = ("warning: these workflows are not installed",)


def _relay(dst: _End, arguments: argparse.Namespace, err: str) -> None:
    """Print the far side's sync line and warnings of an adoption (:data:`_RELAYED`), each prefixed by *dst*,
    unless ``--quiet``."""

    for line in err.splitlines():
        if line.startswith(_RELAYED) and not line.startswith(_REPORTED_HERE):
            _say(arguments, f"{dst.locator}: {line}")


def _release_remedy(
    arguments: argparse.Namespace, transfer_id: str, destination_id: str | None, src: _End, dst: _End
) -> str:
    """The hand-run release of *transfer_id*, keeping away copies (for *destination_id*, or DST's id) with ``keep``."""

    command = f"httk job transfer --release {transfer_id}"
    if not arguments.keep:
        return f"`{command} {src.locator}`"
    if destination_id is not None:
        return f"`{command} --keep --destination-id {destination_id} {src.locator}`"
    return (
        f"`{command} --keep --destination-id ID {src.locator}`, ID being the workspace_id that "
        f"`httk workspace status --json {dst.locator}` reports"
    )


def _take_back(src: _End, holds: list[_moving.Hold], reason: str) -> dict[str, dict[str, Any]]:
    """Adopt the holds of a local *src* back into it after *dst* refused them before adopting any (only holds this
    run created, never one an earlier delivery may have adopted): the outcome members by transfer id
    (``not_moved``, or ``failed`` when one cannot be taken back and stays held)."""

    workspace = src.workspace
    assert workspace is not None
    updates: dict[str, dict[str, Any]] = {}
    message = f"refused before anything was adopted: {reason}; the jobs are back in {src.locator}"
    with _owner(workspace, "job transfer") as owner:
        sources = [Path(hold.path) for hold in holds]
        settled = _moving.settle_absent(workspace, sources)

        def take(source: Path) -> dict[str, Any]:
            failure: str | None
            try:
                report = _moving.adopt(workspace, owner, source, untrusted=False, absent=settled.absent, synced=True)
            except _ERRORS as exc:
                report, failure = None, str(exc)
            else:
                failure = None if report is not None else "another actor took the hold first"
            if failure is None:
                return {"status": "not_moved", "message": message}
            return {
                "status": "failed",
                "message": f"refused before anything was adopted: {reason}; taking it back failed: {failure}",
            }

        serial = {index for index, source in enumerate(sources) if source in settled.serial}
        taken = run_batch(owner, take, sources, workspace.io_workers, serial=serial)
        updates.update((hold.transfer_id, update) for hold, update in zip(holds, taken, strict=True))
    return updates


def _dst_workflows(dst: _End, ids: list[str]) -> dict[str, tuple[object, bool]]:
    """Which of *ids* a local *dst* has installed: each one's ``tree_sha256`` and whether it lacks its build there."""

    assert dst.workspace is not None
    found = {workflow_id: _store.lookup(dst.workspace, workflow_id) for workflow_id in ids}
    return {
        key: (installed.record.get("tree_sha256"), _store.built(installed) is False)
        for key, installed in found.items()
        if installed is not None
    }


def _reported_workflows(report: object) -> dict[str, tuple[object, bool]] | None:
    """What :func:`_dst_workflows` gives, from the ``workflows`` of a remote adoption document (``None`` when
    unusable)."""

    if not isinstance(report, list):
        return None
    return {
        row["id"]: (row.get("tree_sha256"), row.get("status") == "unbuilt")
        for row in report
        if isinstance(row, dict) and isinstance(row.get("id"), str) and row.get("status") in ("present", "unbuilt")
    }


def _install_shipped(
    src: _End,
    dst: _End,
    store: str,
    directories: list[str],
    context: CLIContext,
    arguments: argparse.Namespace,
    session: TransferSession | None = None,
) -> dict[str, dict[str, Any]]:
    """Install SRC's store entries ``<store>/<directory>/{install.json,package}`` in a local *dst* with
    :func:`httk.workflow._store.install_copy`, from SRC's store itself or from a landing scratch (a remote SRC's
    fetched with the adapter's ``pull``, or with one ``send`` request on the pull's transfer *session*).

    The result is a ``workflow install --from-store`` row by directory; an error raised fails them all.
    """

    assert dst.workspace is not None
    files = [f"{directory}/{name}" for directory in directories for name in ("install.json", "package")]
    label = f"transfer from {src.locator}"
    rows: dict[str, dict[str, Any]] = {}
    require_cli_modifiable(dst.workspace)
    failure: Exception | None = None
    with _owner(dst.workspace, "job transfer") as owner:
        # Errors are kept, not raised, inside: an owner left by an error stays open for recovery.
        scratch = None
        try:
            base = Path(store)
            if src.workspace is None:
                scratch = owner.scratch("landing")
                base = scratch / "workflows"
                if session is not None:
                    _receive_store(session, src, dst.workspace, base, directories)
                for chunk in _chunks(files) if session is None else ():
                    _adapter(src, context, arguments, "pull", store, str(base), chunk)
            for directory in directories:
                try:
                    status, installed = _store.install_copy(dst.workspace, owner, base / directory, source_label=label)
                    build = installed.record.get("build") is True
                    rows[directory] = {"id": installed.id, "status": status, "build": build}
                except _fs.SyncFailed as exc:
                    rows[directory] = {"status": "failed", "reason": str(exc)}
                except _ERRORS as exc:
                    rows[directory] = {"status": "refused", "reason": str(exc)}
        except _ERRORS as exc:
            failure = exc
        finally:
            if scratch is not None:
                owner.discard_scratch(scratch)
    if failure is not None:
        raise failure
    return rows


def _receive_store(
    session: TransferSession, src: _End, workspace: Workspace, landing: Path, directories: list[str]
) -> None:
    """Fetch the store entries *directories* of a remote *src* into *landing* in the local *workspace* with one
    ``send`` request on the pull's transfer *session* (not synchronized: the installation copies them durably).

    :param session: The pull's transfer session.
    :param src: The remote source end.
    :param workspace: The local destination workspace.
    :param landing: The directory they land in.
    :param directories: The store directory names.
    :raises RuntimeError: When they did not arrive whole (:class:`~httk.workflow.adapters.SessionFailed` when the
        session failed).
    """

    def download(stream: BinaryIO) -> None:
        assert isinstance(stream, StreamReader)
        _landing.receive_tar(workspace, landing, stream, finish=stream.finish)

    try:
        response = session.request(
            "send", {"workspace": src.plain, "kind": "store", "names": directories}, download=download
        )
    except RequestAborted as exc:
        raise RuntimeError(f"the workflows from {src.locator} did not arrive whole: {exc}") from None
    if response.get("ok") is not True:
        raise RuntimeError(f"{src.locator} did not send its workflows: {response.get('error')}")
    if response.get("sent") != directories:
        session.close()  # a far side that sent something else is not asked anything more
        raise RuntimeError(
            f"{src.locator} answered a send of the workflows {directories} with {response.get('sent')!r}"
        )


def _install_remote(
    src: _End,
    dst: _End,
    store: str | None,
    directories: list[str],
    rebuild: list[str],
    context: CLIContext,
    arguments: argparse.Namespace,
    session: TransferSession | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str | None]]:
    """In one remote ``httk`` start, install SRC's store entries ``<store>/<directory>/{install.json,package}``
    in a remote *dst* (one ``deliver`` to ``workflow install --stdin-tar --build``, a remote *src*'s entries relayed
    through a local directory) and build the installations *rebuild* names (``--also-build``); with no
    directories, only build them (one ``invoke``). With a push's transfer *session*, one ``install`` request on it
    instead (a local *src*'s entries uploaded), in the far side's ``httk`` start of the push.

    The result is the install rows by directory, and each *rebuild* id's build failure (``None`` when built); a
    :class:`RuntimeError` says that *dst* gave no usable answer.
    """

    label = f"transfer from {src.locator}"
    also = [item for workflow_id in rebuild for item in ("--also-build", workflow_id)]
    files = [f"{directory}/{name}" for directory in directories for name in ("install.json", "package")]
    argv = [*_WORKFLOW_COMMAND, "install", "--json", "--workspace", dst.plain, *also]
    if session is not None:
        _say(arguments, f"installing {len(directories)} and building {len(rebuild)} workflow(s) in {dst.locator}")
        code, out, err = _install_over_session(session, dst, store, directories, rebuild, label)
    elif not directories:
        _say(arguments, f"building {len(rebuild)} workflow(s) in {dst.locator}")
        code, out, err = _remote(dst, context, arguments, argv)
    else:
        assert store is not None
        argv += ["--stdin-tar", "--source-label", label, "--build"]
        _say(arguments, f"installing {len(directories)} workflow(s) in {dst.locator}")
        if src.workspace is not None:
            code, out, err = _deliver_call(dst, context, arguments, store, files, argv)
        else:
            with tempfile.TemporaryDirectory(prefix="httk-transfer-") as relay:
                for chunk in _chunks(files):
                    _adapter(src, context, arguments, "pull", store, f"{relay}/workflows", chunk)
                code, out, err = _deliver_call(dst, context, arguments, f"{relay}/workflows", files, argv)
    try:
        document = json.loads(out) if code in (0, 1) else None
        rows = document.get("workflows") if isinstance(document, dict) else None
        built = document.get("built", []) if isinstance(document, dict) else None
        if not isinstance(rows, list) or not isinstance(built, list):
            raise ValueError
        by_directory = {
            PurePosixPath(str(row["directory"])).name: row
            for row in rows
            if isinstance(row, dict) and isinstance(row.get("directory"), str)
        }
        reported = {entry["id"]: entry for entry in built if isinstance(entry, dict) and "id" in entry}
    except (ValueError, TypeError):
        older = " (it may run an older httk-workflow; upgrade it)" if "unrecognized arguments" in err else ""
        raise RuntimeError(f"workflow install failed on {dst.locator}{older}: {err.strip()}") from None
    failures: dict[str, str | None] = {}
    for workflow_id in rebuild:
        entry = reported.get(workflow_id, {"status": "failed", "reason": "not reported"})
        failures[workflow_id] = None if entry.get("status") == "built" else str(entry.get("reason"))
    return by_directory, failures


def _ship_workflows(
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
    have: dict[str, tuple[object, bool]] | None = None,
    session: TransferSession | None = None,
) -> dict[str, dict[str, Any]]:
    """Install in *dst*, under the same ids, the workflows the holds need that it lacks and *src* has.

    The result maps every needed id to ``{"status", "reason"?, "build"?}``: ``present`` or ``mismatch`` (*dst*
    already has it, with the same or another tree, and keeps it), ``missing`` (installed in neither),
    ``installed``, ``refused``, ``failed`` or ``unbuilt``; ``build`` marks one a local *dst*'s
    :func:`_build_shipped` builds. A remote *dst* reported what it has (*have*) with the adoption (or, over a push's
    transfer *session*, before it), and builds in the same start that installs (on the session when there is one).
    A local *dst* fetches the entries from a remote *src* over a pull's transfer *session* when there is one.
    Nothing is ever replaced, and a failure here never fails the transfer: the jobs then wait at *dst*.
    """

    needed = {entry[0]: entry for hold in holds for entry in hold.workflows or ()}
    if not needed:
        return {}
    if have is None:
        if dst.workspace is None:
            reason = f"{dst.locator} reported no workflows with the adoption"
            return {workflow_id: {"status": "failed", "reason": reason} for workflow_id in needed}
        have = _dst_workflows(dst, sorted(needed))
    shipping: dict[str, dict[str, Any]] = {}
    ship: dict[str, str] = {}
    for workflow_id, (_, directory, digest) in needed.items():
        if workflow_id in have:
            there, unbuilt = have[workflow_id]
            # An installation an earlier transfer could not build is built (or reported) again.
            shipping[workflow_id] = {"status": "present" if digest in (None, there) else "mismatch"}
            if unbuilt:
                shipping[workflow_id]["build"] = True
        elif directory is None:
            shipping[workflow_id] = {"status": "missing"}
        else:
            ship[directory] = workflow_id
    store = PurePosixPath(holds[0].path)
    expected = ("outgoing", "transfers", WORKSPACE_DIRECTORY)
    # One workspace's holds share its store: <root>/workflows beside <root>/.httk-workspace/transfers/outgoing.
    base = (
        str(store.parents[3] / "workflows") if tuple(parent.name for parent in store.parents[:3]) == expected else None
    )
    if ship and base is None:
        reason = f"the hold path {store} is not below a workspace's transfers/outgoing"
        shipping.update({workflow_id: {"status": "failed", "reason": reason} for workflow_id in ship.values()})
        ship = {}
    rebuild = [] if dst.workspace is not None else [key for key, entry in shipping.items() if entry.pop("build", False)]
    if not (ship or rebuild):
        return shipping
    failures: dict[str, str | None] = {}
    try:
        if dst.workspace is not None:
            _say(arguments, f"installing {len(ship)} workflow(s) in {dst.locator}")
            rows = _install_shipped(src, dst, str(base), sorted(ship), context, arguments, session)
        else:
            rows, failures = _install_remote(src, dst, base, sorted(ship), rebuild, context, arguments, session)
    except _ERRORS as exc:
        rows = {directory: {"status": "failed", "reason": str(exc)} for directory in ship}
        failures = dict.fromkeys(rebuild, str(exc))
    remedy = f"build it with `httk workflow build --workspace {dst.locator} {{}}`"
    for directory, workflow_id in ship.items():
        row = rows.get(directory, {"status": "failed", "reason": "not reported"})
        if row.get("status") not in ("installed", "present", "mismatch", "refused", "failed"):
            row = {"status": "failed", "reason": f"malformed report {row!r}"}
        if row["status"] not in ("refused", "failed") and row.get("id") != workflow_id:
            row = {"status": "refused", "reason": f"{directory} installed {row.get('id')!r}, not {workflow_id}"}
        shipping[workflow_id] = {key: row[key] for key in ("status", "reason") if key in row}
        if row["status"] == "installed" and row.get("build") and dst.workspace is not None:
            # Only what this transfer installed: another actor's installation is built only when DST lacked a
            # build when queried.
            shipping[workflow_id]["build"] = True
        if row.get("build_status") == "failed":
            failures[workflow_id] = str(row.get("build_reason"))
    for workflow_id, failure in failures.items():
        if failure is not None:
            shipping[workflow_id].update(status="unbuilt", reason=f"{failure}; {remedy.format(workflow_id)}")
    return shipping


def _build_shipped(dst: _End, shipping: dict[str, dict[str, Any]], arguments: argparse.Namespace) -> None:
    """Build in a local *dst* each needed workflow marked ``build``: one this transfer installed that declares
    ``[workflow.build]``, or one *dst* had without a build for its platform.

    A failed build marks it ``unbuilt`` with the reason and the remedy; the installation stays either way.
    """

    assert dst.workspace is not None
    built = [workflow_id for workflow_id, entry in shipping.items() if entry.pop("build", False)]
    built = [workflow_id for workflow_id in built if shipping[workflow_id]["status"] in _BUILDABLE]
    if built:
        _say(arguments, f"building {len(built)} workflow(s) in {dst.locator}")
    for workflow_id in built:
        reason = None
        try:
            with _owner(dst.workspace, "job transfer") as owner:
                try:
                    with _stdout_to_stderr(True):
                        _store.build(dst.workspace, owner, workflow_id)
                except _ERRORS as exc:
                    reason = str(exc)
        except _ERRORS as exc:
            reason = str(exc)
        if reason is not None:
            remedy = f"build it with `httk workflow build --workspace {dst.locator} {workflow_id}`"
            shipping[workflow_id].update(status="unbuilt", reason=f"{reason}; {remedy}")


#: The statuses whose installation in DST a transfer builds when marked ``build``.
_BUILDABLE = ("installed", "present", "mismatch")


def _release_argv(transfer_ids: list[str], keep: bool, destination_id: str | None = None) -> list[str]:
    """The ``job transfer --release ... [--keep [--destination-id ID]]`` arguments that release *transfer_ids* on a
    remote; *destination_id* is the adopting workspace's id for away copies of holds that record none."""

    releases = [item for transfer_id in transfer_ids for item in ("--release", transfer_id)]
    identified = ["--destination-id", destination_id] if destination_id is not None else []
    return [*REMOTE_JOB_TRANSFER_COMMAND, *releases, *(["--keep", *identified] if keep else [])]


def release_holds(
    workspace: Workspace,
    transfer_ids: Sequence[str],
    *,
    keep: bool,
    destination_id: str | None,
    label: str = "job transfer",
    progress: Callable[[str, int | None], object] | None = None,
) -> list[bool]:
    """Release the holds *transfer_ids* of the local *workspace*: discard each, or with *keep* keep its jobs there
    as away copies (recording *destination_id*, the adopting workspace, when a hold records none).

    ``job transfer --release``, the release step of a transfer from a local SRC and the transfer session's
    ``release`` op share this.

    :param workspace: The workspace holding them.
    :param transfer_ids: The transfer ids (a repeated one is released after the first, so it finds it gone).
    :param keep: Keep away copies.
    :param destination_id: The adopting workspace's id for away copies of holds that record none.
    :param label: The label of the command's owner.
    :param progress: Told ``release`` with the holds done so far, from right before the owner is registered.
    :return: For each id, in order, whether it was released here (false: there was no such hold).
    """

    tell = progress or (lambda _phase, _count: None)

    def done(index: int, _released: bool) -> None:
        tell("release", index + 1)

    tell("release", 0)  # before the owner's registration, which may leave state behind
    with _owner(workspace, label) as owner:
        return run_batch(
            owner,
            lambda transfer_id: _moving.release_hold(
                workspace, owner, transfer_id, keep=keep, destination_workspace_id=destination_id
            ),
            transfer_ids,
            workspace.io_workers,
            serial=_repeats(transfer_ids),
            emit=done,
        )


def _release(
    src: _End,
    transfer_ids: list[str],
    context: CLIContext,
    arguments: argparse.Namespace,
    destination_id: str | None = None,
    session: TransferSession | None = None,
) -> tuple[list[str], set[str]]:
    """Step 4: discard the holds on *src*, or with ``keep`` keep their jobs there as away copies (recording
    *destination_id*, the adopting workspace, when a hold records none); one already gone (adopted in on one
    filesystem) is fine. A remote *src* is released with one ``release`` request on the pull's transfer *session*
    when there is one.

    The result lists the ids a remote *src* did not confirm (an older remote takes only the last ``--release``),
    and the ids a local *src* released itself (the others were gone).
    """

    _say(arguments, f"releasing {len(transfer_ids)} hold(s) on {src.locator}")
    if src.workspace is not None:
        done = release_holds(src.workspace, transfer_ids, keep=arguments.keep, destination_id=destination_id)
        return [], {transfer_id for transfer_id, ok in zip(transfer_ids, done, strict=True) if ok}
    if session is not None:
        return _release_over_session(session, src, transfer_ids, arguments.keep, destination_id)
    argv = _release_argv(transfer_ids, arguments.keep, destination_id)
    code, out, err = _remote(src, context, arguments, [*argv, src.plain])
    if code not in (0, 1):
        older = arguments.keep and "unrecognized arguments: --keep" in err
        message = err.strip() or f"job transfer --release failed on {src.locator}"
        if older:
            message += f" ({src.locator} may run an older httk-workflow that keeps no away copies; upgrade it)"
        raise RuntimeError(message)
    # Confirmed: "released T" on stdout, or "T: no such hold ..." on stderr (gone already).
    confirmed = {line.removeprefix("released ") for line in out.splitlines() if line.startswith("released ")}
    confirmed |= {line.split(":", 1)[0] for line in err.splitlines() if ": no such hold" in line}
    unconfirmed = [transfer_id for transfer_id in transfer_ids if transfer_id not in confirmed]
    return unconfirmed, set(transfer_ids) - set(unconfirmed)


def _release_over_session(
    session: TransferSession, src: _End, transfer_ids: list[str], keep: bool, destination_id: str | None
) -> tuple[list[str], set[str]]:
    """Release *transfer_ids* on a remote *src* with one ``release`` request on the pull's *session*, an id confirmed
    when the far side released it or found it gone already.

    :param session: The pull's transfer session.
    :param src: The remote source end.
    :param transfer_ids: The holds to release.
    :param keep: Keep away copies.
    :param destination_id: The adopting workspace's id for away copies of holds that record none.
    :return: What :func:`_release` returns: the ids not confirmed, and those released.
    :raises RuntimeError: When the far side refused the request or answered unreadably;
        :class:`~httk.workflow.adapters.SessionFailed` when the session failed (whether it released is unknown).
    """

    header: dict[str, Any] = {"workspace": src.plain, "transfer_ids": transfer_ids, "keep": keep}
    if keep and destination_id is not None:
        header["destination_id"] = destination_id
    response = session.request("release", header)
    if response.get("ok") is not True:
        raise RuntimeError(f"job transfer --release failed on {src.locator}: {response.get('error')}")
    released, missing = response.get("released"), response.get("missing")
    if not isinstance(released, list) or not isinstance(missing, list):
        raise RuntimeError(f"{src.locator} answered the release with no released and missing lists")
    confirmed = {str(transfer_id) for transfer_id in [*released, *missing]}
    unconfirmed = [transfer_id for transfer_id in transfer_ids if transfer_id not in confirmed]
    return unconfirmed, set(transfer_ids) - set(unconfirmed)


#: A push over a transfer session closes a segment at this many bundles, ...
_SEGMENT_BUNDLES = 256
#: ... or this many regular-file bytes landed (a hard link counted with its target's size), ...
_SEGMENT_BYTES = 16 << 30
#: ... or this many tar members; a hold is never split, so a larger one is a segment of its own.
_SEGMENT_MEMBERS = 16384


def _measure(path: Path) -> tuple[int, int]:
    """The regular-file bytes and the tar members of the hold directory *path*, in one walk (an entry that cannot
    be read counts as one member of no bytes; the upload reports it)."""

    size, members = 0, 1
    pending = [path]
    while pending:
        try:
            with os.scandir(pending.pop()) as listing:
                for entry in listing:
                    members += 1
                    try:
                        found = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    if stat.S_ISDIR(found.st_mode):
                        pending.append(Path(entry.path))
                    elif stat.S_ISREG(found.st_mode):
                        size += found.st_size
        except OSError:
            continue
    return size, members


def _segments(
    holds: list[_moving.Hold], measure: Callable[[_moving.Hold], tuple[int, int]] | None = None
) -> list[list[_moving.Hold]]:
    """Split *holds*, in order, into the segments of a push: each closed once it reaches :data:`_SEGMENT_BUNDLES`,
    :data:`_SEGMENT_BYTES` or :data:`_SEGMENT_MEMBERS`, and before a hold that would take it beyond the bytes or
    the members (so a hold beyond either alone is a segment of its own). Each hold's bytes and members are what
    *measure* says, by default its local directory's (:func:`_measure`)."""

    segments: list[list[_moving.Hold]] = []
    current: list[_moving.Hold] = []
    size = members = 0
    for hold in holds:
        hold_size, hold_members = _measure(Path(hold.path)) if measure is None else measure(hold)
        if current and (size + hold_size > _SEGMENT_BYTES or members + hold_members > _SEGMENT_MEMBERS):
            segments.append(current)
            current, size, members = [], 0, 0
        current.append(hold)
        size += hold_size
        members += hold_members
        if len(current) >= _SEGMENT_BUNDLES or size >= _SEGMENT_BYTES or members >= _SEGMENT_MEMBERS:
            segments.append(current)
            current, size, members = [], 0, 0
    if current:
        segments.append(current)
    return segments


#: The ops a push needs of its destination's transfer session, ...
_PUSH_OPS = frozenset({"adopt", "install", "workflows"})
#: ... and a pull of its source's.
_PULL_OPS = frozenset({"hold", "holds", "send", "release"})


def _push_session(
    end: _End,
    context: CLIContext,
    arguments: argparse.Namespace,
    sessions: contextlib.ExitStack,
    needs: frozenset[str] = _PUSH_OPS,
) -> TransferSession | None:
    """Open the transfer session of a push to the remote *end* (or, with *needs* :data:`_PULL_OPS`, of a pull from
    it), closed with *sessions* at the end of the command; or ``None`` when the transfer takes the adapter
    operations instead, as before sessions: when the remote setting ``transfer_session`` is ``off`` or the remote is
    a ``mount-daemon`` one (nothing is started, nothing said), or when *end* cannot serve a session
    (:class:`~httk.workflow.adapters.SessionUnsupported`, said once). Any other failure to open it is raised.

    The session is opened with this process's terminal when it has one, so that ssh can prompt.
    """

    assert end.binding is not None
    target = resolve_remote(end.binding.remote, project=context.cwd)
    if (
        remote_settings(target.bundle).get("transfer_session") == "off"
        or read_metadata(target.bundle).get("kind") == "mount-daemon"
    ):
        return None
    session = sessions.enter_context(TransferSession(target.bundle, needs=needs, timeout=arguments.adapter_timeout))
    try:
        session.open(interactive=sys.stdin.isatty())
    except SessionUnsupported as exc:
        fallback = "one adapter call a delivery" if needs == _PUSH_OPS else "the adapter operations"
        _say(arguments, f"{end.locator} serves no transfer session ({exc}); using {fallback}")
        return None
    return session


class _SourceSession:
    """The transfer session of a pull from a remote SRC: opened at the command's first request to SRC (the hold, or
    the listing of its holds for ``--resume``), closed with the command's other sessions.

    :param src: The remote source end.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :param sessions: Closes the command's transfer sessions at its end.
    """

    def __init__(
        self, src: _End, context: CLIContext, arguments: argparse.Namespace, sessions: contextlib.ExitStack
    ) -> None:
        self._open = partial(_push_session, src, context, arguments, sessions, _PULL_OPS)
        self._src = src
        self._arguments = arguments
        self._opened = False
        self._session: TransferSession | None = None
        self._failure: str | None = None
        self._asked = False
        #: A landing's sync commit failed in this session: every later fetch of it, for any destination, goes with an
        #: fsync of every file.
        self.fsync = False

    def first_request(self) -> bool:
        """Return whether no request was made on the session yet (true once), so that a ``too_large`` refusal of the
        command's first request to SRC can still fall back.

        :return: Whether this is the first request.
        """

        first, self._asked = not self._asked, True
        return first

    def fall_back(self, reason: str) -> None:
        """Close the session and take the adapter operations for the rest of the command (one more login), saying
        why in one line.

        :param reason: Why the session's answer cannot carry the command.
        """

        _say(self._arguments, f"{reason}; using the adapter operations for this command")
        if self._session is not None:
            self._session.close()
        self._session = None

    def get(self) -> TransferSession | None:
        """Return the session, opening it at the first call; ``None`` when SRC takes the adapter operations.

        :return: The session (possibly failed since), or ``None``.
        :raises RuntimeError: When it could not be opened (other than by not being served), now or at the first call.
        """

        if not self._opened:
            self._opened = True
            try:
                self._session = self._open()
            except _ERRORS as exc:
                self._failure = str(exc)
                raise
        if self._failure is not None:
            raise RuntimeError(self._failure)
        return self._session


def _session_adopt(
    session: TransferSession,
    src: _End,
    dst: _End,
    arguments: argparse.Namespace,
    holds: list[_moving.Hold],
    token: str,
    fsync: bool,
) -> tuple[dict[str, Any], str]:
    """One ``adopt`` request of *holds* on *session*: the checked adoption document and the far side's standard
    error.

    The upload is one tar archive of the holds, each as ``<transfer_id>/...``, written as it is read. When this side's
    upload fails (a hold changed under the writer), the far side's answer to the aborted stream is used, and only a
    ``refused`` document is a usable one.

    :param session: The push's open transfer session.
    :param src: The local source end.
    :param dst: The remote destination end.
    :param arguments: The command's arguments.
    :param holds: The segment's holds.
    :param token: The landing's name.
    :param fsync: Ask for ``--sync fsync``.
    :return: The adoption document (:func:`_checked_adoption`) and the far side's captured standard error.
    :raises RuntimeError: When the answer is unusable or the session failed: whether anything was adopted is unknown.
    """

    ids = [hold.transfer_id for hold in holds]
    header: dict[str, Any] = {"workspace": dst.plain, "token": token}
    if fsync:
        header["sync"] = "fsync"

    def upload(stream: BinaryIO) -> None:
        with tarfile.open(fileobj=stream, mode="w|", bufsize=DATA_LIMIT) as archive:
            for hold in holds:
                archive.add(hold.path, arcname=hold.transfer_id)

    aborted = ""
    try:
        response = session.request("adopt", header, upload=upload)
    except RequestAborted as exc:
        response, aborted = exc.response, f"; this side's upload failed: {exc}"
    except SessionFailed as exc:
        raise _unknown_adoption(f"the transfer session to {dst.locator} failed: {exc}", src, dst, arguments) from None
    try:
        document = _checked_adoption(response.get("document"), ids)
        refused = "refused" in document
        if (response.get("ok") is True) == refused or not isinstance(document.get("bundle", ""), str):
            raise ValueError("an ambiguous answer")
        if not isinstance(document.get("stream_aborted", False), bool):
            raise ValueError("an ambiguous stream_aborted")
        if aborted and not refused:
            raise ValueError("an adoption of an aborted upload")
    except (ValueError, TypeError, WorkflowError):
        detail = f"{dst.locator} gave no usable adoption report ({response.get('error', 'no document')}){aborted}"
        raise _unknown_adoption(detail, src, dst, arguments) from None
    return document, str(response.get("stderr", ""))


def _session_workflows(
    session: TransferSession,
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
) -> dict[str, dict[str, Any]]:
    """Before a push's first segment: ask *dst* (one ``workflows`` request) which of the workflows the holds need it
    has, and install the missing ones and build the unbuilt ones there (one ``install`` request), so that every
    segment is runnable as soon as it is adopted. The result is :func:`_ship_workflows`'s; a failure is reported
    in it and never stops the push.

    :param session: The push's open transfer session.
    :param src: The local source end.
    :param dst: The remote destination end.
    :param holds: Every hold the push delivers.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :return: The status of every needed workflow, by id.
    :raises SessionFailed: When the session failed while asking (nothing was adopted).
    """

    needed = sorted({entry[0] for hold in holds for entry in hold.workflows or ()})
    if not needed:
        return {}
    response = session.request("workflows", {"workspace": dst.plain, "ids": needed})
    have = _reported_workflows(response.get("workflows")) if response.get("ok") is True else None
    if have is None:
        reason = f"{dst.locator} did not report its workflows: {response.get('error', 'an unusable answer')}"
        return {workflow_id: {"status": "failed", "reason": reason} for workflow_id in needed}
    return _ship_workflows(src, dst, holds, context, arguments, have, session)


def _install_over_session(
    session: TransferSession,
    dst: _End,
    store: str | None,
    directories: list[str],
    rebuild: list[str],
    label: str,
) -> tuple[int, str, str]:
    """One ``install`` request on a push's *session*: the local store entries ``<store>/<directory>/{install.json,
    package}`` uploaded (when there are directories), installed and built in *dst*, and *rebuild* built. The result
    reads like a ``workflow install --json`` run: exit status, printed document and standard error.
    """

    header: dict[str, Any] = {"workspace": dst.plain, "also_build": rebuild}
    if directories:
        header.update(source_label=label, build=True)

    def upload(stream: BinaryIO) -> None:
        with tarfile.open(fileobj=stream, mode="w|", bufsize=DATA_LIMIT) as archive:
            for directory in directories:
                for name in ("install.json", "package"):
                    archive.add(f"{store}/{directory}/{name}", arcname=f"{directory}/{name}")

    try:
        response = session.request("install", header, upload=upload if directories else None)
    except RequestAborted as exc:
        if exc.response.get("ok") is not False:
            # An installation claimed for an upload this side aborted: what DST did is unknown, so nothing more is
            # sent on this session (the push stops before its first segment).
            session.close()
            raise RuntimeError(f"{dst.locator} answered an aborted workflow upload as installed: unknown") from exc
        response = exc.response
    document = response.get("document")
    if response.get("ok") is not True or not isinstance(document, dict):
        return 2, "", str(response.get("error", "no document"))
    return int(response.get("returncode", 1)), json.dumps(document), str(response.get("stderr", ""))


class _Deliveries:
    """What the deliveries of one batch to a remote DST settled, and the rules that settle each answer.

    :param src: The source end.
    :param dst: The remote destination end.
    :param outcomes: The batch's outcomes by transfer id, updated in place.
    :param fresh: The transfer ids of the holds this run created in a local *src*.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    """

    def __init__(
        self,
        src: _End,
        dst: _End,
        outcomes: dict[str, dict[str, Any]],
        fresh: frozenset[str],
        context: CLIContext,
        arguments: argparse.Namespace,
    ) -> None:
        self.src, self.dst, self.outcomes, self.fresh = src, dst, outcomes, fresh
        self.context, self.arguments = context, arguments
        #: The ``workspace_id`` of the first answer: every later delivery of the command must report it.
        self.answered: str | None = None
        #: The adopting workspace's id, for the releases' away copies.
        self.destination_id: str | None = dst.workspace_id
        #: The adoption entries by transfer id.
        self.entries: dict[str, dict[str, Any]] = {}
        #: The ``workflows`` reports of the adopting deliveries.
        self.reports: list[object] = []
        #: The holds of the adopting deliveries, whose workflows are shipped.
        self.shipped_for: list[_moving.Hold] = []
        #: The holds already released (a segment's, right after it), and those whose release failed.
        self.released: set[str] = set()
        self.unreleased: list[tuple[str, str]] = []
        self.handled: set[str] = set()
        #: Why the session stopped: an unknown outcome, after which nothing more is sent.
        self.stopped: str | None = None
        #: A sync commit failed in this session: later segments go with ``--sync fsync`` directly.
        self.fsync = False
        #: The refusal of a whole segment that stopped the session: the segments never sent are settled by it.
        self.refusal: dict[str, Any] | None = None
        #: A pull's transfer session to the remote SRC, which then releases over it.
        self.session: TransferSession | None = None

    def fail(self, holds: list[_moving.Hold], message: str, *, complete: bool = False) -> None:
        """Report *holds* failed with *message*; they stay held, which the outcome adds unless *complete* (the
        message says where they are itself)."""

        entry: dict[str, Any] = {"status": "failed", "message": message}
        if complete:
            entry["complete"] = True
        self.entries.update({hold.transfer_id: dict(entry) for hold in holds})

    def settle(self, holds: list[_moving.Hold], document: dict[str, Any]) -> list[str]:
        """Settle the checked adoption *document* of a delivery of *holds*: the transfer ids it delivered.

        A ``refused`` document (nothing of the delivery adopted) takes back the holds this run created from a local
        SRC; a hold that predates the run, or one of a remote SRC, stays held, reported ``failed``.
        """

        src, dst, arguments = self.src, self.dst, self.arguments
        ids = [hold.transfer_id for hold in holds]
        self.answered = self.answered or document.get("workspace_id")
        if "refused" in document:
            # Definitively nothing of this delivery was adopted.
            reason = f"{dst.locator}: {document['refused']}"
            if src.workspace is None:
                message = f"refused before anything was adopted: {reason}; it stays held on {src.locator}"
                message += f" (`httk job transfer --resume {src.locator} {dst.locator}` delivers it again)"
                for key in ids:
                    self.outcomes[key].update(status="failed", message=message)
                return []
            if back := [hold for hold in holds if hold.transfer_id in self.fresh]:
                for transfer_id, update in _take_back(src, back, reason).items():
                    self.outcomes[transfer_id].update(update)
            for hold in holds:
                if hold.transfer_id not in self.fresh:
                    # An earlier run's delivery of this hold may have been adopted, its answer lost.
                    remedy = _release_remedy(
                        arguments, hold.transfer_id, self.destination_id or document.get("workspace_id"), src, dst
                    )
                    message = (
                        f"refused before anything was adopted: {reason}; it stays held on {src.locator}, because "
                        f"an earlier delivery may already have adopted it: check with `httk job show` on "
                        f"{dst.locator}, then deliver again with `httk job transfer --resume {src.locator} "
                        f"{dst.locator}` or, if the jobs are there, release with {remedy}, or, if they are not "
                        f"there, take it back with `httk job adopt --workspace {src.locator} {hold.path}`"
                    )
                    self.outcomes[hold.transfer_id].update(status="failed", message=message)
            return []
        self.entries.update(document["entries"])
        self.destination_id = self.destination_id or document["workspace_id"]
        self.reports.append(document.get("workflows"))
        self.shipped_for += holds
        return [key for key in ids if document["entries"][key]["status"] in _DELIVERED]

    def release(self, transfer_ids: list[str]) -> None:
        """Release the delivered *transfer_ids* on SRC (step 4), recording which were released."""

        self.handled.update(transfer_ids)
        # A session's request carries the ids in its header, not on a command line.
        chunks = (
            _chunks(transfer_ids, flag="--release") if self.session is None else [transfer_ids] * bool(transfer_ids)
        )
        for chunk in chunks:
            try:
                unconfirmed, done = _release(
                    self.src, chunk, self.context, self.arguments, self.destination_id, self.session
                )
                self.released |= done
                self.unreleased += [(transfer_id, "not confirmed by the source") for transfer_id in unconfirmed]
            except _ERRORS as exc:
                self.unreleased += [(transfer_id, str(exc)) for transfer_id in chunk]


def _deliver_segment(
    session: TransferSession, settled: "_Deliveries", holds: list[_moving.Hold], token: str, fsync: bool
) -> tuple[dict[str, Any], str]:
    """One delivery of a segment for :func:`_adopt_with_retry` (:func:`_session_adopt`), recording in *settled*
    that the session now goes with ``--sync fsync``."""

    settled.fsync = settled.fsync or fsync  # a retry after a failed commit: the rest go so too
    return _session_adopt(session, settled.src, settled.dst, settled.arguments, holds, token, fsync)


def _push_over_session(session: TransferSession, deliveries: list[list[_moving.Hold]], settled: _Deliveries) -> None:
    """Push the deliveries of a local SRC's batch over *session*, segment by segment (:func:`_segments`).

    Each segment is one ``adopt`` request with its own archive, landing, sync commit and adoption document, settled
    by the rules of a push (:meth:`_Deliveries.settle`, :func:`_adopt_with_retry`), and its adopted holds released
    right after it. A segment refused for one bundle's member settles that bundle as refused (taken back when this
    run held it) and is sent once more without it; a second refusal is final. A refusal of a whole segment (the far
    side names no bundle, no failed sync commit and no incomplete stream of this side; a member refusal it cannot
    attribute counts so too, conservatively) stops the session: every segment not yet
    sent is settled by the same refusal, as the one delivery of the adapter path would have been. An unknown
    outcome (an unusable answer, a failed session, another workspace answering) stops it too: that segment's holds
    and every later one's stay held for ``--resume``.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    segments = [segment for delivery in deliveries for segment in _segments(delivery)]
    for number, segment in enumerate(segments, 1):
        if settled.refusal is not None:
            settled.settle(segment, settled.refusal)  # never sent: this run's holds are taken back
            continue
        if settled.stopped is not None:
            settled.fail(
                segment,
                f"not delivered, since the transfer session stopped ({settled.stopped}); deliver it with "
                f"`httk job transfer --resume {src.locator} {dst.locator}`",
            )
            continue
        which = f" (segment {number} of {len(segments)})" if len(segments) > 1 else ""
        _say(arguments, f"delivering {len(segment)} bundle(s) to {dst.locator}{which}")
        holds: list[_moving.Hold] = segment
        resent = False
        while holds:
            deliver = partial(_deliver_segment, session, settled, holds)
            try:
                document, err = _adopt_with_retry(
                    deliver, src, dst, arguments, len(holds), _fs.fresh_token(), settled.answered, fsync=settled.fsync
                )
            except _ERRORS as exc:
                settled.fail(holds, str(exc))
                settled.stopped = str(exc).split(";", 1)[0]
                break
            refused = "refused" in document
            member = refused and not document.get("sync_failed") and not resent
            offender: _moving.Hold | None = next(
                (hold for hold in holds if hold.transfer_id == document.get("bundle")), None
            )
            if member and offender is not None:
                # One bundle's member was refused: that bundle alone is refused, the others go once more without it.
                settled.settle([offender], document)
                holds, resent = [hold for hold in holds if hold is not offender], True
                continue
            done = settled.settle(holds, document)
            if not refused:
                _relay(dst, arguments, err)
                settled.release(done)
            elif not any(document.get(key) for key in ("bundle", "sync_failed", "stream_aborted")):
                # Refused as a whole, by the far side's statement (an unknown or sealed workspace, a quota), whatever
                # this side's upload did: every later segment would be too.
                settled.stopped, settled.refusal = f"{dst.locator}: {document['refused']}", document
            break


def _receive_part(
    session: TransferSession, settled: _Deliveries, holds: list[_moving.Hold], landing: Path, fsync: bool
) -> tuple[str, Any]:
    """One ``send`` request of a pull's segment (or of what is left of it) on *session*, received into *landing* in
    the local DST: what happened, and what it is about.

    The download is extracted into *landing* (:func:`httk.workflow._landing.receive_tar`, the trust boundary), its
    terminal validated, and the landing made durable (a sync barrier entered before its first byte; with *fsync*
    every file synchronized as it is written instead) before the far side's answer is read. SRC may carry only a
    prefix of *holds* (it closes a request at :data:`_SEGMENT_BYTES` or :data:`_SEGMENT_MEMBERS`); its answer must
    name exactly what arrived.

    :param session: The pull's transfer session.
    :param settled: The batch's deliveries.
    :param holds: The holds to fetch, in order.
    :param landing: The landing directory, which must not exist yet.
    :param fsync: Synchronize every file as it is written.
    :return: ``("received", sent)`` (the transfer ids that arrived, a prefix of *holds*, in *landing*);
        ``("sync", reason)`` (the landing's ``syncfs`` commit failed under ``io.sync_errors`` ``fail``);
        ``("refused", (ids, reason, gone))`` (SRC could not send those holds, *gone* the ones it no longer has, or
        this side refused one's member); ``("failed", reason)`` (this side refused the stream naming no hold);
        ``("systemic", reason)`` (SRC refused the request naming no hold); ``("unknown", reason)`` (the session
        failed, or SRC answered something else).
        Except for ``received``, nothing of *landing* is left to use.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    assert dst.workspace is not None
    workspace = dst.workspace
    ids = [hold.transfer_id for hold in holds]
    header = {
        "workspace": src.plain,
        "kind": "holds",
        "names": ids,
        "limit_bytes": _SEGMENT_BYTES,
        "limit_members": _SEGMENT_MEMBERS,
    }
    received: list[str] = []

    def download(stream: BinaryIO) -> None:
        assert isinstance(stream, StreamReader)
        # The terminal is validated (stream.finish) before the commit, and the commit precedes the answer.
        received[:] = _landing.receive_tar(
            workspace, landing, stream, barrier=True, force_fsync=fsync, finish=stream.finish, quiet=arguments.quiet
        )

    cause: BaseException | None = None
    try:
        response = session.request("send", header, download=download)
    except RequestAborted as exc:
        response, cause = exc.response, exc.__cause__
    except SessionFailed as exc:
        return "unknown", f"the transfer session to {src.locator} failed: {exc}"
    if response.get("ok") is not True:
        missing = response.get("missing")
        gone = [key for key in ids if isinstance(missing, list) and key in missing]
        offenders = [key for key in ids if key in gone or key == response.get("bundle")]
        if offenders:
            return "refused", (offenders, f"{src.locator} could not send it: {response.get('error')}", gone)
        return "systemic", f"{src.locator} refused to send: {response.get('error')}"
    if cause is not None:
        # SRC answered that it sent its part whole; this side refused it, and its landing is gone.
        if isinstance(cause, _landing.StreamRefused) and cause.stream_aborted:
            session.close()  # it aborted the download all the same: a far side out of step is asked nothing more
            return "unknown", f"{src.locator} aborted the download of a send it answered as complete: {cause}"
        if isinstance(cause, _landing.CommitFailed):
            return "sync", str(cause)
        if isinstance(cause, _landing.StreamRefused) and cause.bundle in ids:
            return "refused", ([cause.bundle], f"refused in {dst.locator}: {cause}", [])
        return "failed", f"the copy from {src.locator} was refused in {dst.locator}: {cause}"
    sent = response.get("sent")
    if not isinstance(sent, list) or not sent or sent != ids[: len(sent)] or sorted(sent) != received:
        session.close()  # a far side that answers something else is not asked anything more
        return "unknown", (
            f"{src.locator} answered a send of {len(ids)} bundle(s) with {sent!r}, not the prefix of them that arrived"
        )
    return "received", sent


def _pull_part(
    session: TransferSession, settled: _Deliveries, holds: list[_moving.Hold], fsync: bool
) -> tuple[str, Any]:
    """Fetch a prefix of *holds* from a remote SRC over a pull's *session* into an owner scratch landing in the local
    DST and, once it is durable, adopt it there (:func:`_adopt_local`). The landing is discarded either way.

    :param session: The pull's transfer session.
    :param settled: The batch's deliveries.
    :param holds: The holds to fetch, in order.
    :param fsync: Synchronize every landed file as it is written.
    :return: ``("adopted", (sent, entries))``, the adoption entries of the transfer ids that arrived by id, or what
        :func:`_receive_part` says went wrong (nothing adopted); ``("failed", reason)`` also when no landing could be
        made before the request (the owner or its scratch), a local refusal naming no hold.
    :raises WorkflowError: Or another error of the CLI's kinds, when the adoption failed (some may be adopted).
    """

    dst, arguments = settled.dst, settled.arguments
    assert dst.workspace is not None
    workspace = dst.workspace
    unusable = f"no landing could be made in {dst.locator}"
    try:
        require_cli_modifiable(workspace)
        registered = _owner(workspace, "job transfer")
    except _ERRORS as exc:
        return "failed", f"{unusable}: {exc}"
    outcome: tuple[str, Any] = ("failed", unusable)
    failure: Exception | None = None
    with registered as owner:
        # Errors are kept, not raised, inside: an owner left by an error stays open for recovery.
        scratch = None
        try:
            try:
                scratch = owner.scratch("landing")
            except _ERRORS as exc:
                return "failed", f"{unusable}: {exc}"  # before the request: nothing was asked of SRC
            landing = scratch / "outgoing"
            outcome = _receive_part(session, settled, holds, landing, fsync)
            if outcome[0] == "received":
                sent: list[str] = outcome[1]
                sources = [landing / transfer_id for transfer_id in sent]
                _say(arguments, f"adopting {len(sent)} bundle(s) in {dst.locator}")
                absent = _moving.settle_absent(workspace, sources)
                serial = {index for index, source in enumerate(sources) if source in absent.serial}
                adopted = run_batch(
                    owner,
                    lambda source: _adopt_local(workspace, owner, source, absent.absent),
                    sources,
                    workspace.io_workers,
                    serial=serial,
                )
                outcome = ("adopted", (sent, dict(zip(sent, adopted, strict=True))))
        except _ERRORS as exc:
            failure = exc
        finally:
            if scratch is not None:
                owner.discard_scratch(scratch)
    if failure is not None:
        raise failure
    return outcome


def _pull_over_session(source: _SourceSession, holds: list[_moving.Hold], settled: _Deliveries) -> None:
    """Pull the batch *holds* from a remote SRC into a local DST over the pull's transfer session, segment by segment.

    Segments close at :data:`_SEGMENT_BUNDLES`; SRC closes each ``send`` request at :data:`_SEGMENT_BYTES` of landed
    bytes or :data:`_SEGMENT_MEMBERS` archive members (it measures the bundles), so a segment may take several
    requests, each a prefix of what is left of it. Each request has its own landing, durable before anything of it is
    adopted (:func:`_pull_part`), and the holds it adopted (or found adopted already) are released on SRC right after
    it, over the session. Per segment:

    - a failed ``syncfs`` commit of a landing (``io.sync_errors`` ``fail``) fetches it once more into a fresh landing
      with every file synchronized as it is written, as every later request of the session then is (for every
      destination of the command); a second failure stops the pull as below;
    - holds SRC cannot send (gone from it, or changed while they were read), or whose member this side refuses, fail
      and the rest of the segment is fetched once more without them; after a second such refusal the rest of the
      segment is not fetched (held on SRC; a head-of-line limit until ``--resume``);
    - a refusal naming no hold (by SRC, or by this side: a full quota, a landing that cannot be made or used, a
      failed sync under ``io.sync_errors`` ``ignore``, an fsync failing after the re-fetch), or a failed session (or
      an answer naming other holds than arrived), stops the pull: nothing of that request was adopted, and its holds
      and every later one stay held on SRC for ``--resume``;
    - a release that fails leaves the jobs in DST with their holds on SRC (reported, with the remedy); a session
      failing in it stops the pull too.

    A failed adoption fails what is left of its segment (held on SRC; some may be adopted, which ``--resume``
    reports as already adopted).

    :param source: The command's transfer session to SRC (open).
    :param holds: The batch, in order.
    :param settled: The batch's deliveries, updated in place.
    """

    session = source.get()
    assert session is not None
    src, dst, arguments = settled.src, settled.dst, settled.arguments
    resume = f"`httk job transfer --resume {src.locator} {dst.locator}`"
    # The bundles are SRC's to measure: here a segment closes at bundles only.
    segments = _segments(holds, measure=lambda _hold: (0, 0))
    for number, segment in enumerate(segments, 1):
        if settled.stopped is not None:
            settled.fail(
                segment,
                f"not delivered, since the transfer session stopped ({settled.stopped}); deliver it with {resume}",
            )
            continue
        which = f" (segment {number} of {len(segments)})" if len(segments) > 1 else ""
        _say(arguments, f"copying {len(segment)} bundle(s) from {src.locator}{which}")
        pending: list[_moving.Hold] = segment
        retried = resent = False
        while pending:
            try:
                kind, detail = _pull_part(session, settled, pending, source.fsync)
            except _ERRORS as exc:
                settled.fail(pending, str(exc))
                break
            if kind == "adopted":
                sent, entries = detail
                settled.entries.update(entries)
                settled.release([key for key in sent if entries[key]["status"] in _DELIVERED])
                pending = pending[len(sent) :]
                if not session.is_open:
                    settled.stopped = f"{src.locator}: the transfer session failed while releasing"
                    settled.fail(pending, f"not delivered, since {settled.stopped}; deliver it with {resume}")
                    break
            elif kind == "sync" and not retried:
                _say(
                    arguments,
                    f"copying {len(pending)} bundle(s) from {src.locator} again, with an fsync of every file: {detail}",
                )
                retried = source.fsync = True
            elif kind == "refused":
                offenders, reason, gone = detail
                _fail_offenders(settled, [hold for hold in pending if hold.transfer_id in offenders], reason, gone)
                pending = [hold for hold in pending if hold.transfer_id not in offenders]
                if resent:
                    # The segment is not fetched a third time: what is left of it waits for --resume.
                    message = (
                        f"not fetched: another hold of its segment was refused twice ({offenders[0]}: {reason}); "
                        f"it stays held on {src.locator}: --resume retries it"
                    )
                    settled.fail(pending, message, complete=True)
                    break
                resent = True
            else:
                # SRC refused naming no hold, the session failed, SRC answered something else, this side refused the
                # stream naming no hold (an inline fsync failing after the re-fetch among them), or no landing could
                # be made; a second failed commit is not expected after the re-fetch, whose files are fsynced inline,
                # and stops as well.
                settled.stopped = str(detail)
                settled.fail(pending, f"{detail}; nothing of it was adopted in {dst.locator}; deliver it with {resume}")
                break


def _fail_offenders(settled: _Deliveries, holds: list[_moving.Hold], reason: str, gone: list[str]) -> None:
    """Fail the holds SRC could not send (or this side refused) with *reason*; those in *gone* are no longer on SRC
    (another actor released or took them), which the message says instead of their staying held there."""

    src, dst = settled.src, settled.dst
    for hold in holds:
        if hold.transfer_id in gone:
            message = (
                f"{reason}; it is gone from {src.locator} (released or taken by another actor): check with "
                f"`httk job show` on {src.locator} and on {dst.locator} where its jobs are"
            )
            settled.fail([hold], message, complete=True)
        else:
            settled.fail([hold], reason)


def _drive(
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
    *,
    fresh: frozenset[str] = frozenset(),
    sessions: contextlib.ExitStack,
    source: "_SourceSession | None" = None,
) -> list[dict[str, Any]]:
    """Copy, adopt and release a batch of holds bound for *dst*: one outcome per hold, in order.

    A hold that was not adopted stays held on *src*, except when a remote *dst* refused a delivery before adopting
    any of it: a local *src* then takes back the holds of that delivery that this run created (*fresh*, their
    transfer ids), reported ``not_moved``. A hold that existed before the run may have been adopted by an earlier
    delivery whose answer was lost, so it is never taken back: it stays held, reported ``failed``. From a local
    *src*, the holds that predate the run and this run's are therefore delivered to a remote *dst* separately
    (those first, each delivery with its own token), one delivery when either set is empty. The workflows the
    holds need are shipped to a local *dst* before the adoption, and to a remote one once after the deliveries,
    from what their adoptions reported.

    From a local *src* to a remote *dst*, the deliveries go over one transfer session (opened at the first, closed
    with *sessions*): the workflows first (:func:`_session_workflows`), then segment by segment
    (:func:`_push_over_session`); a *dst* that serves no session gets one adapter ``deliver`` a delivery instead.
    From a remote *src* to a local *dst*, the pull's transfer session to *src* (*source*, opened by the hold or the
    listing of the holds) carries the workflows and then the holds, segment by segment (:func:`_pull_over_session`);
    without one, the adapter's ``pull`` copies them a chunk at a time and ``invoke`` releases them.

    :param src: The end holding the jobs.
    :param dst: The destination every hold of the batch records.
    :param holds: The batch.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :param fresh: The transfer ids of the holds this run created in a local *src*.
    :param sessions: Closes the command's transfer sessions at its end.
    :param source: The command's transfer session to a remote *src* that a pull uses, if any.
    :return: One outcome per hold, in the order of *holds*.
    """

    outcomes: dict[str, dict[str, Any]] = {
        hold.transfer_id: {"transfer_id": hold.transfer_id, "members": list(hold.members), "destination": dst.locator}
        for hold in holds
    }
    ready = []
    for hold in holds:
        # A remote DST's id is unknown here: it refuses a bundle bound for another workspace itself.
        if dst.workspace_id is not None and hold.destination_workspace_id not in (None, dst.workspace_id):
            message = (
                f"the hold's destination is not this workspace: {dst.locator} is {dst.workspace_id}, the hold is "
                f"bound for {hold.destination_workspace_id}; it stays held on {src.locator}"
            )
            outcomes[hold.transfer_id].update(status="refused", message=message)
        else:
            ready.append(hold)
    workflows = bool(ready) and not arguments.no_workflows
    if workflows and any(hold.workflows is None for hold in ready):
        print(
            f"warning: {src.locator} reported no usable workflows for its holds (it may run an older or newer "
            "httk-workflow); workflows were not shipped",
            file=sys.stderr,
        )
    # A pull from a remote SRC over its transfer session (opened already, by the hold or the listing of the holds).
    pulling = source.get() if source is not None and src.workspace is None and dst.workspace is not None else None
    shipping: dict[str, dict[str, Any]] = {}
    # The holds whose workflows are shipped: a local DST's all, a remote DST's those of an adopting delivery.
    shipped_for = ready
    if workflows and dst.workspace is not None:
        # Once a batch, before the first adoption: a job adopted after its workflow can start at once.
        shipping = _ship_workflows(src, dst, ready, context, arguments, session=pulling)
    entries: dict[str, dict[str, Any]] = {}
    destination_id = dst.workspace_id
    report: object = None
    by_id = {hold.transfer_id: hold for hold in ready}
    settled = _Deliveries(src, dst, outcomes, fresh, context, arguments)
    settled.session = pulling
    session: TransferSession | None = None
    if pulling is not None:
        assert source is not None
        _pull_over_session(source, ready, settled)
        entries = settled.entries
    elif dst.workspace is not None:
        for chunk in _chunks(list(by_id)):
            try:
                entries.update(_deliver(src, dst, [by_id[transfer_id] for transfer_id in chunk], context, arguments))
            except _ERRORS as exc:
                entries.update({transfer_id: {"status": "failed", "message": str(exc)} for transfer_id in chunk})
    elif ready:
        # The bundles travel in the stream, not on the command line; from a local SRC the holds that predate this
        # run are delivered apart from this run's, so that a refusal can take these back.
        deliveries = [ready]
        if src.workspace is not None:
            deliveries = [
                [hold for hold in ready if hold.transfer_id not in fresh],
                [hold for hold in ready if hold.transfer_id in fresh],
            ]
        deliveries = [delivery for delivery in deliveries if delivery]
        if src.workspace is not None:
            try:
                session = _push_session(dst, context, arguments, sessions)
            except _ERRORS as exc:
                # Nothing was sent: the holds stay, as after any failed delivery.
                for delivery in deliveries:
                    settled.fail(delivery, f"{dst.locator}: the transfer session did not open: {exc}")
                deliveries = []
        # The holds whose workflows were installed before the first segment: reported whatever their segments do.
        stepped: list[_moving.Hold] | None = None
        if session is not None and workflows and deliveries:
            # Before the first segment, so that each adopted segment can start at once.
            stepped = [hold for delivery in deliveries for hold in delivery]
            try:
                shipping = _session_workflows(session, src, dst, stepped, context, arguments)
                failure = None if session.is_open else "while installing the workflows"
            except SessionFailed as exc:
                failure = str(exc)
            if failure is not None:
                message = (
                    f"{dst.locator}: the transfer session failed before anything was adopted ({failure}); deliver "
                    f"it with `httk job transfer --resume {src.locator} {dst.locator}`"
                )
                for delivery in deliveries:
                    settled.fail(delivery, message)
                deliveries = []
        if session is not None:
            _push_over_session(session, deliveries, settled)
        for delivery in deliveries if session is None else ():
            built_for = sorted({entry[0] for hold in delivery for entry in hold.workflows or ()}) if workflows else []
            try:
                document = _deliver_remote(
                    src, dst, delivery, context, arguments, _fs.fresh_token(), built_for, expected=settled.answered
                )
            except _ERRORS as exc:
                settled.fail(delivery, str(exc))
                continue
            settled.settle(delivery, document)
        entries, destination_id, shipped_for = settled.entries, settled.destination_id, settled.shipped_for
        if stepped is not None:
            shipped_for = stepped
        if any(isinstance(rows, list) for rows in settled.reports):
            # Each delivery asked about its own holds' workflows: together, about the batch's.
            report = [row for rows in settled.reports if isinstance(rows, list) for row in rows]
    delivered = []
    for transfer_id, entry in entries.items():
        outcome, status = outcomes[transfer_id], entry["status"]
        if status in _DELIVERED:
            outcome.update({key: value for key, value in entry.items() if key not in ("bundle", "message")})
            delivered.append(transfer_id)
        elif status == "refused":
            # The adoption's own refusals already say "bundle refused:"; its other errors do not.
            reason = str(entry.get("message")).removeprefix("bundle refused: ")
            message = f"bundle refused: {reason}; it stays held on {src.locator}"
            outcome.update(status="refused", message=message)
        elif status == "taken":
            outcome.update(status="taken", message="another actor took the held bundle first")
        elif entry.get("complete"):
            outcome.update(status="failed", message=entry.get("message"))
        else:
            outcome.update(status="failed", message=f"{entry.get('message')}; it stays held on {src.locator}")
    if workflows and dst.workspace is None and delivered and session is None:
        # After the adoption, from its report: a manager skips a ready job whose workflow is not installed yet.
        # Only for the holds of a delivery that answered with an adoption (its report covers no other). Over a
        # session they were installed before the first segment.
        shipping = _ship_workflows(src, dst, shipped_for, context, arguments, _reported_workflows(report))
    settled.release([transfer_id for transfer_id in delivered if transfer_id not in settled.handled])
    for transfer_id in delivered:
        # A local release keeps copies only of a hold it took that records its destination's id, or for which the
        # adoption reported it; a remote one is taken at its word.
        hold = by_id[transfer_id]
        recorded = src.workspace is None or hold.destination_workspace_id is not None or destination_id is not None
        outcomes[transfer_id]["kept"] = arguments.keep and transfer_id in settled.released and recorded
    for transfer_id, reason in settled.unreleased:
        outcomes[transfer_id]["message"] = (
            f"the jobs are in {dst.locator}, but the hold was not released: {reason}; "
            f"run `httk job transfer --resume` or {_release_remedy(arguments, transfer_id, destination_id, src, dst)}"
        )
    if dst.workspace is not None:
        _build_shipped(dst, shipping, arguments)
    for hold in shipped_for:
        if shipping and hold.workflows is not None:
            outcomes[hold.transfer_id]["workflows"] = [
                {"id": workflow_id, **shipping[workflow_id]} for workflow_id, _, _ in hold.workflows
            ]
    return list(outcomes.values())


#: The report lines of the shipped workflows by status (those with a reason get a line each).
_SHIPPED = {
    "installed": "installed these workflows in the destination",
    "mismatch": "warning: the destination has these workflows with another tree, left as they are (the jobs run it)",
    "missing": "warning: these workflows are installed in neither workspace, so their jobs wait",
}


def _report(
    outcomes: list[dict[str, Any]],
    errors: list[str],
    *,
    json_output: bool,
    source: str,
    empty: str = "no held transfers to drive",
) -> int:
    missing = sorted({workflow for outcome in outcomes for workflow in outcome.get("missing_workflows", ())})
    # One warning per id: a root the adoption names as missing is left to its line below.
    shipped = {
        entry["id"]: entry
        for outcome in outcomes
        for entry in outcome.get("workflows", ())
        if not (entry["status"] == "missing" and entry["id"] in missing)
    }
    for status, text in _SHIPPED.items():
        if listed := sorted(workflow_id for workflow_id, entry in shipped.items() if entry["status"] == status):
            print(f"{text}: {', '.join(listed)}", file=sys.stderr)
    for workflow_id, entry in sorted(shipped.items()):
        if "reason" in entry:
            print(f"warning: workflow {workflow_id} {entry['status']}: {entry['reason']}", file=sys.stderr)
    if missing:
        print(f"warning: these workflows are not installed, so their jobs wait: {', '.join(missing)}", file=sys.stderr)
    for error in errors:
        print(error, file=sys.stderr)
    if json_output:
        print(json.dumps({"transfers": outcomes, "errors": errors}, indent=2, sort_keys=True))
    else:
        if not outcomes and not errors:
            print(empty)
        for outcome in outcomes:
            print(
                f"{outcome['transfer_id']}\t{outcome['status']}\t{len(outcome['members'])} job(s)\t{outcome['destination']}"
            )
        if kept := sum(len(outcome["members"]) for outcome in outcomes if outcome.get("kept")):
            print(f"kept {kept} away copies in {source} (httk job list --state away; httk job recover JOB to resend)")
    for outcome in outcomes:
        if "message" in outcome:
            print(f"{outcome['transfer_id']}: {outcome['message']}", file=sys.stderr)
    failed = errors or any(outcome["status"] not in _DELIVERED or "message" in outcome for outcome in outcomes)
    return 1 if failed else 0


def _handle_release(arguments: argparse.Namespace, context: CLIContext) -> int:
    if arguments.destination is not None or any(
        (arguments.jobs, arguments.state, arguments.step, arguments.resume, arguments.hold, arguments.no_workflows)
    ):
        raise ValueError("--release takes transfer ids and at most the workspace holding them")
    for transfer_id in arguments.release:
        if not _TRANSFER_ID.fullmatch(transfer_id):
            raise ValueError(f"not a transfer id: {transfer_id!r}")
    destination_id = arguments.destination_id
    if destination_id is not None:
        if not arguments.keep:
            raise ValueError("--destination-id with --release names the workspace away copies record; it needs --keep")
        destination_id = canonical_uuid(destination_id, "--destination-id")
    src = _end(arguments.source, context, arguments)
    if src.workspace is None:
        argv = _release_argv(arguments.release, arguments.keep, destination_id)
        code, out, err = _remote(src, context, arguments, [*argv, src.plain])
        sys.stdout.write(out)
        sys.stderr.write(err)
        return code
    released = release_holds(
        src.workspace,
        arguments.release,
        keep=arguments.keep,
        destination_id=destination_id,
        label="job transfer --release",
    )
    for transfer_id, done in zip(arguments.release, released, strict=True):
        if done:
            print(f"released {transfer_id}")
        else:
            print(f"{transfer_id}: no such hold in {src.locator}", file=sys.stderr)
    return 0 if all(released) else 1


def _handle_hold(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Hold the selected jobs of a local SRC for a transfer to the DST locator, recorded as given."""

    selected = arguments.jobs or arguments.state or arguments.step
    if arguments.resume or arguments.source is None or arguments.destination is None or not selected:
        raise ValueError("--hold takes --job, --state or --step, SRC and the destination to record")
    if arguments.no_workflows:
        raise ValueError("--no-workflows applies to a transfer, not to --hold")
    if arguments.keep:
        raise ValueError("--keep applies when a hold is released, not to --hold")
    src = _end(arguments.source, context, arguments)
    if src.workspace is None:
        raise ValueError(f"--hold holds jobs in a workspace on this machine, not in {src.locator}")
    destination_id = arguments.destination_id
    if destination_id is not None:
        destination_id = canonical_uuid(destination_id, "--destination-id")
    errors: list[str] = []
    dst = _End(arguments.destination, workspace_id=destination_id)
    holds, _ = _hold_jobs(src, dst, context, arguments, errors)
    if arguments.json:
        print(json.dumps(hold_document(src.workspace, holds, dst.locator, errors), indent=2, sort_keys=True))
    else:
        for error in errors:
            print(error, file=sys.stderr)
        for hold in holds:
            print(f"held {len(hold.members)} job(s) in {hold.path}")
    return 1 if errors else 0


def handle_transfer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move jobs between two workspaces: hold, copy, adopt, then release (see the verb's help).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: 0 when every transfer was adopted (or already was), else 1.
    """

    everyday = arguments.transfer_verb != "transfer"
    if everyday:
        # job push [SRC] DST / job pull SRC [DST]: the omitted end stays None, which _end resolves as the enclosing
        # or default workspace; the implicit state is set before the guards so that --placement sees it.
        if arguments.source is None:
            end = "DST" if arguments.transfer_verb == "push" else "SRC"
            raise ValueError(f"name the other workspace: httk job {arguments.transfer_verb} {end} (e.g. REMOTE:WS)")
        if arguments.transfer_verb == "push" and arguments.destination is None:
            arguments.source, arguments.destination = None, arguments.source
        if not (arguments.jobs or arguments.state or arguments.step):
            arguments.state = ["ready" if arguments.transfer_verb == "push" else "succeeded"]
    if arguments.placement is not None:
        if not (arguments.state or (arguments.step and not arguments.jobs)):
            raise ValueError("--placement narrows --state, or --step without --job; it needs one of them")
        if not normalize_placement(arguments.placement).parts:
            raise ValueError("--placement needs a placement prefix; leave it out to select under every placement")
    if arguments.release:
        return _handle_release(arguments, context)
    if arguments.hold:
        return _handle_hold(arguments, context)
    if arguments.destination_id is not None:
        raise ValueError("--destination-id is recorded by --hold; it needs --hold")
    selected = arguments.jobs or arguments.state or arguments.step
    ends_named = everyday or (arguments.source is not None and arguments.destination is not None)
    if not arguments.resume and not (ends_named and selected):
        raise ValueError(
            "name jobs with --job, --state or --step, and SRC and DST; or re-drive held transfers with --resume"
        )
    if arguments.resume and selected:
        raise ValueError("--resume re-drives held transfers and takes no --job, --state or --step")
    ends: dict[str, _End] = {}
    dst = None
    resolve_dst = arguments.destination is not None or everyday
    if resolve_dst and arguments.source is None:
        # The named end first: an invalid DST must not create the per-user default workspace as SRC.
        dst = _end(arguments.destination, context, arguments)
    src = _end(arguments.source, context, arguments)
    if resolve_dst:
        dst = dst or _end(arguments.destination, context, arguments)
        if dst.locator == src.locator:
            raise ValueError(f"SRC and DST are the same workspace: {src.locator}")
        ends[dst.locator] = dst
    errors: list[str] = []
    known: list[_moving.Hold] | None = None
    # The holds this run creates: only these may be taken back after a refusal (an earlier one may be adopted).
    fresh: frozenset[str] = frozenset()
    outcomes = []
    # Each remote end's transfer session, closed at the end: a remote DST's of a push, a remote SRC's of a pull (a
    # remote-to-remote transfer still takes the adapter operations).
    with contextlib.ExitStack() as sessions:
        pull = src.workspace is None and (dst is None or dst.workspace is not None)
        source = _SourceSession(src, context, arguments, sessions) if pull else None
        if selected:
            assert dst is not None
            if dst.workspace is not None:
                # Before anything is held: a destination that cannot take the jobs would only leave them held.
                require_cli_modifiable(dst.workspace)
            what = [f"{len(arguments.jobs)} job(s)"] if arguments.jobs else []
            what += [f"every {state} job" for state in arguments.state]
            at = f" at step {' or '.join(arguments.step)}" if arguments.step else ""
            _say(arguments, f"holding {', '.join(what or ['every job'])}{at} on {src.locator}")
            held, before = _hold_jobs(src, dst, context, arguments, errors, source)
            fresh = frozenset(hold.transfer_id for hold in held)
            if before is not None:
                # A remote SRC listed the other holds bound for DST with the hold: no transfer status call.
                known = sorted(
                    {hold.transfer_id: hold for hold in [*before, *held]}.values(), key=lambda h: h.transfer_id
                )
        # Every run re-drives the holds bound for its destination (with --resume and no DST: every hold), each
        # destination's holds as one batch.
        batches: dict[str, list[_moving.Hold]] = {}
        if known is None:
            known = _holds(src, context, arguments, workflows=not arguments.no_workflows, source=source)
        for hold in known:
            locator = hold.destination_locator
            if dst is not None and locator not in ends:
                continue
            if locator is None:
                errors.append(f"{hold.transfer_id}: the hold records no destination; adopt or release it by hand")
                continue
            if locator not in ends:
                try:
                    ends[locator] = _end(locator, context, arguments)
                except _ERRORS as exc:
                    errors.append(f"{hold.transfer_id}: cannot reach its destination {locator}: {exc}")
                    continue
            batches.setdefault(locator, []).append(hold)
        for locator, holds in batches.items():
            outcomes.extend(
                _drive(src, ends[locator], holds, context, arguments, fresh=fresh, sessions=sessions, source=source)
            )
    if arguments.state or arguments.step:
        under = f" under {arguments.placement}" if arguments.placement is not None else ""
        which = f"{' or '.join(arguments.state)} " if arguments.state else ""
        at = f" at step {' or '.join(arguments.step)}" if arguments.step else ""
        empty = f"no {which}job{at} to move in {src.locator}{under}"
        return _report(outcomes, errors, json_output=arguments.json, source=src.locator, empty=empty)
    return _report(outcomes, errors, json_output=arguments.json, source=src.locator)


def status_document(
    workspace: Workspace, progress: Callable[[str, int | None], object] | None = None
) -> dict[str, Any]:
    """Return the document ``transfer status --json`` prints for a local *workspace* (and the transfer session's
    ``holds`` op answers): every hold, with the workflows its members need.

    :param workspace: The workspace.
    :param progress: Told ``list`` with the holds read so far.
    :return: ``{"format", "format_version", "workspace", "holds"}``.
    """

    tell = progress or (lambda _phase, _count: None)
    read_count = 0

    def one() -> None:
        nonlocal read_count
        read_count += 1
        tell("list", read_count)

    tell("list", read_count)
    now = time.time()
    listed = [hold.as_mapping(now) for hold in _moving.held(workspace, workflows=True, on_read=one)]
    return {"format": TRANSFER_STATUS_FORMAT, "format_version": 1, "workspace": str(workspace.root), "holds": listed}


def handle_transfer_status(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List a workspace's held transfers (read only).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: Exit status 0.
    """

    src = _end(arguments.workspace, context, arguments)
    if src.workspace is None:
        assert src.binding is not None
        tail = ["--json"] if arguments.json else []
        return _run_remote_workspace(
            src.binding, context, [*REMOTE_TRANSFER_STATUS_COMMAND, *tail, src.plain], timeout=arguments.adapter_timeout
        )
    if arguments.json:
        assert src.workspace is not None
        print(json.dumps(status_document(src.workspace), indent=2, sort_keys=True))
        return 0
    now = time.time()
    holds = _holds(src, context, arguments, workflows=False)
    if not holds:
        print(f"no held transfers in {src.locator}")
    for hold in holds:
        age = cast(int, hold.as_mapping(now)["age_seconds"]) // 3600
        destination = hold.destination_locator or "-"
        if hold.destination_workspace_id is not None:
            destination = f"{destination} ({hold.destination_workspace_id})"
        print(f"{hold.transfer_id}\t{hold.members[0]}\t{len(hold.members)} job(s)\tto {destination}\t{age}h old")
    return 0


_TRANSFER_DESCRIPTION = """\
Move jobs between two workspaces (the everyday forms are `job push [SRC] DST`
for the ready jobs and `job pull SRC [DST]` for the succeeded ones):
`job transfer --job JOB [--tree] SRC DST`,
or every job in a state: `job transfer --state succeeded [--placement PREFIX]
[--tree] SRC DST` (--state and --job combine; both repeat). `--step STEP`
(repeatable) narrows the selection to jobs at that step (the current
activation's, else the initial step); without --state it selects from every
unowned state but away. --state and --step never split a tree: they leave out jobs whose
parent is in SRC, which move only in their root's tree. Without --tree a selected job that has children present is
refused: --tree moves a finished campaign's roots with their children (failed,
cancelled or paused ones too). A root with children that are not all
terminal or paused (a parent that an any_terminal, any_succeeded or at_least
join let finish first) is refused and stays, never split.

Each of SRC and DST is tried first as a registered workspace name (REMOTE:WS
for a workspace on a remote), then as a workspace directory; `./NAME`
addresses a directory unambiguously. Any combination of local and remote ends
works; a remote-to-remote transfer is relayed through this client.

A transfer holds the jobs on SRC (an eject into SRC's own
.httk-workspace/transfers/outgoing/<transfer id>, where the jobs are in
neither workspace), copies the held bundle to DST, adopts it there into the
states it left, and only then releases the hold. A bundle DST refuses (say,
because one of its jobs is already there) stays held on SRC and is reported.
The holds bound for one DST travel as one batch: one copy and one adoption
call, however many jobs move. Progress goes to stderr unless -q is given.

Workflows travel too: before adopting, the transfer installs in DST, under the
same ids, the workflows the jobs need (each job's effective workflow and its
calls) that DST lacks and SRC has, copying only their sources, and after the
adoption builds those with [workflow.build] there. An installation DST already
has is never replaced, even with another tree (reported). --no-workflows ships
none.

Away copies: with --keep the release keeps each moved job in SRC as an inert
copy in the state `away` (`job push` does so unless --no-keep), recording
DST, the transfer and the state it left; it never runs or moves there. A
later transfer back from DST replaces the copies (`job pull` does), and
`job recover JOB` returns a lost job's copy to the state it left, to push
again. `--state away` is no choice.

Recovery: delivery is at least once. Every run, and `job transfer --resume
[SRC] [DST]`, first re-drives the holds bound for DST (with --resume and no DST,
every hold, to the destination it records); DST reports "already adopted" for
a bundle it already has, and the hold is released (keeping away copies with
the --keep of the run that releases it). `transfer status` and
`job why` list held bundles. A transfer abandoned for good is taken back into
SRC with `job adopt SRC/.httk-workspace/transfers/outgoing/<transfer id>`,
which is safe only after checking that DST does not have the jobs.
`job transfer --release T [--release T ...] [--keep] [WS]` discards holds
whose jobs DST has (with --keep, keeps them as away copies); `job transfer --hold (--job JOB | --state STATE | --step STEP) ... SRC DST` only
holds, recording DST as given (the first leg a client runs on a remote SRC)."""


_EVERYDAY_DESCRIPTION = """\
`job {verb} {usage}` moves every {state} job ({meaning})
{direction}, each root with its children. It is
`job transfer --tree --state {state} SRC DST` with the omitted {omitted}
defaulting to the enclosing workspace, else the project's default
workspace, else the per-user default workspace (which this creates on first
use). With one positional it names {named}: `httk job {verb} REMOTE:WS`;
with none the command is refused.

--job, --state or --step replaces the implicit --state {state};
--step without --state selects from every unowned state but away. A selected job
whose parent is in SRC moves only in its root's tree. A root moves only whole: one whose
children are not all terminal or paused (a parent that an any_terminal,
any_succeeded or at_least join let finish before its children) is refused
and stays, never split. The workflows the jobs need that DST lacks and SRC
has are installed there too (--no-workflows ships none; see `job transfer`).
{keeping}
Every run re-drives the held transfers bound for DST; --resume, --release and --hold stay on `job transfer`, which also
covers the general case."""

_EVERYDAY = {
    "push": {
        "usage": "[SRC] DST",
        "state": "ready",
        "meaning": "created and waiting to start",
        "direction": "to DST, typically a remote workspace",
        "omitted": "SRC",
        "named": "DST",
        "summary": "move the ready jobs of a workspace (by default this one) to another",
        "keeping": (
            "SRC keeps each moved job as an away copy (state away, never run\n"
            "there; --no-keep keeps none, also for the held transfers this run\n"
            "re-drives): `job pull` of the finished jobs replaces the copies, and\n"
            "`job recover JOB` returns a copy whose job was lost there, to push again."
        ),
    },
    "pull": {
        "usage": "SRC [DST]",
        "state": "succeeded",
        "meaning": "finished successfully",
        "direction": "from SRC, typically a remote workspace",
        "omitted": "DST",
        "named": "SRC",
        "summary": "move the succeeded jobs of another workspace to one (by default this one)",
        "keeping": (
            "A pull keeps no copies in SRC; the away copies a `job push` kept in\n"
            "DST are replaced by the arriving jobs."
        ),
    },
}


def build_transfer_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``job transfer``, ``job push`` and ``job pull`` verbs: move jobs between two workspaces."""

    transfer = _leaf(
        subparsers,
        "transfer",
        summary="move jobs between two registered workspace names or workspace directories",
        description=_TRANSFER_DESCRIPTION,
        handler=handle_transfer,
    )
    transfer.set_defaults(transfer_verb="transfer")
    parsers = [transfer]
    for verb, text in _EVERYDAY.items():
        parser = _leaf(
            subparsers,
            verb,
            summary=text["summary"],
            description=_EVERYDAY_DESCRIPTION.format(verb=verb, **text),
            handler=handle_transfer,
        )
        # The options job transfer keeps for itself; push and pull always move whole trees.
        parser.set_defaults(transfer_verb=verb, resume=False, release=[], hold=False, destination_id=None, tree=True)
        if verb == "push":
            parser.add_argument(
                "--no-keep",
                dest="keep",
                action="store_false",
                help="move the jobs without keeping away copies here",
            )
        else:
            parser.set_defaults(keep=False)  # a pull never keeps copies in SRC
        parsers.append(parser)
    for parser in parsers:
        everyday = parser is not transfer
        parser.add_argument(
            "source",
            metavar="SRC",
            nargs="?",
            help="the workspace the jobs leave" + ("" if everyday else " (with --release: the one holding it)"),
        )
        parser.add_argument("destination", metavar="DST", nargs="?", help="the workspace the jobs arrive in")
        parser.add_argument(
            "--job",
            action="append",
            default=[],
            dest="jobs",
            metavar="JOB",
            help="a job UUID, key, unique prefix or path in SRC; a remote SRC requires canonical job UUIDs (repeatable)",
        )
        parser.add_argument(
            "--state",
            action="append",
            default=[],
            metavar="STATE",
            choices=_MOVABLE_STATES,
            help="select every job of SRC in this state (repeatable; combinable with --job)",
        )
        _add_step_option(
            parser,
            help_text=(
                "select only jobs at this step: the current activation's, else the initial step (repeatable; "
                "narrows --job and --state; without --state, selects from every unowned state but away)"
            ),
        )
        parser.add_argument(
            "--placement",
            metavar="PREFIX",
            help="with --state, or --step without --job: only jobs under this placement prefix",
        )
        if not everyday:
            parser.add_argument("--tree", action="store_true", help="move each job's descendants with it")
            parser.add_argument("--resume", action="store_true", help="re-drive the held transfers of SRC (to DST)")
            parser.add_argument(
                "--release",
                action="append",
                default=[],
                metavar="TRANSFER_ID",
                help="discard a hold whose jobs DST already has (repeatable)",
            )
            parser.add_argument(
                "--keep",
                action="store_true",
                help="keep an away copy of each moved job in SRC; a later transfer back replaces it "
                "(with --release: keep the held jobs as away copies)",
            )
            parser.add_argument(
                "--hold",
                action="store_true",
                help="only hold the jobs in SRC, recording DST as given (the first leg, as a client drives it remotely)",
            )
            parser.add_argument(
                "--destination-id",
                metavar="WORKSPACE_ID",
                help="with --hold: the destination workspace's id, recorded so that the transfer adopts only there "
                "(with --release --keep: the id the away copies record when the hold records none)",
            )
        parser.add_argument(
            "--no-workflows",
            action="store_true",
            help="do not install the workflows the jobs need in DST (by default those DST lacks and SRC has travel)",
        )
        parser.add_argument("-q", "--quiet", action="store_true", help="print no progress lines to stderr")
        _add_adapter_timeout(parser)
        parser.add_argument("--json", action="store_true", help="print the outcomes as one JSON document")
        add_durability_arguments(parser)


def build_transfer_operator_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``transfer`` group: inspecting held transfers."""

    _, group = _group(
        subparsers,
        "transfer",
        summary="inspect held workspace transfers",
        description="Inspect the held transfers of a workspace (move jobs with `httk job transfer`)",
    )
    status = _leaf(
        group,
        "status",
        summary="list held transfers",
        description="List the held transfers of a workspace: transfer id, root job, members, destination and age",
        handler=handle_transfer_status,
    )
    status.add_argument("workspace", metavar="WS", nargs="?", help="the workspace (default: the selected one)")
    status.add_argument("--json", action="store_true", help="print the holds as one JSON document")
    _add_adapter_timeout(status)
