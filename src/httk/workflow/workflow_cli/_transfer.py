"""Remote and transfer command groups.

``job transfer`` moves jobs between workspaces as hold + copy + adopt + release
on :mod:`httk.workflow._moving`, locally or through the remote adapters'
``invoke``, ``deliver`` and ``pull``; ``job push``/``job pull`` are its everyday forms (an implicit
``--state ready``/``succeeded``, ``--tree``, the default workspace as the omitted end);
``transfer status`` lists held transfers. ``job push`` (unless ``--no-keep``) and ``job transfer --keep``
release the hold keeping the moved jobs in SRC as ``away`` copies, which a later transfer back replaces.
"""

import argparse
import collections
import contextlib
import enum
import json
import math
import os
import re
import shlex
import stat
import sys
import tarfile
import tempfile
import time
import uuid
from collections.abc import Callable, Collection, Container, Iterator, Sequence
from contextlib import redirect_stdout
from copy import copy
from dataclasses import dataclass, replace
from functools import partial
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, NamedTuple, cast

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
from ..errors import StreamAborted, TransferProtocolError, WorkflowError
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
    return _eject_claimed(
        workspace,
        owner,
        root,
        destination,
        tree=tree,
        locator=locator,
        destination_workspace_id=destination_workspace_id,
        owner_key=owner_key,
        workflows=workflows,
    )


def _eject_claimed(
    workspace: Workspace,
    owner: _kernel.Owner,
    root: _kernel.OwnedJob,
    destination: Path | None,
    *,
    tree: bool,
    locator: str | None = None,
    destination_workspace_id: str | None = None,
    owner_key: str | None = None,
    workflows: bool = False,
) -> _moving.EjectReport | _moving.Hold | str:
    """Eject or hold the claimed *root*, as :func:`eject_once` does once it won the claim.

    :param workspace: The workspace.
    :param owner: The ejecting owner.
    :param root: The claimed job.
    :param destination: Where the bundle goes; ``None`` holds it in ``transfers/outgoing/``.
    :param tree: Move the job's descendants too.
    :param locator: The transfer destination a hold records.
    :param destination_workspace_id: The destination workspace's id a hold records.
    :param owner_key: The optional key that may control the adopted root through the workspace daemon.
    :param workflows: Have a hold report the workflows its members need.
    :return: The report (the hold, when held), or why it cannot move now.
    """

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
    src: _End, context: CLIContext, arguments: argparse.Namespace, *, workflows: bool = True
) -> list[_moving.Hold]:
    """The holds of *src*, all at once; a local *src* reports their workflows only with *workflows*, a remote one
    answers ``transfer status --json``."""

    if src.workspace is not None:
        return _moving.held(src.workspace, workflows=workflows)
    code, out, err = _remote(src, context, arguments, [*REMOTE_TRANSFER_STATUS_COMMAND, "--json", src.plain])
    if code:
        raise RuntimeError(f"transfer status failed on {src.locator}: {err.strip()}")
    document = json.loads(out)
    if not isinstance(document, dict) or not isinstance(document.get("holds"), list):
        raise ValueError(f"{src.locator} returned an invalid transfer status")
    return [_moving.Hold.from_mapping(hold) for hold in document["holds"]]


class _Parentage(NamedTuple):
    """What :func:`_parent_here` read of a job."""

    #: It moves only with its root: a non-detached child of a job present here.
    deferred: bool
    #: Its step (when asked for).
    step: str | None
    #: Why its step is unknown (when asked for).
    damage: str | None
    #: The job id of its ``job.json`` parent; ``None`` without one, or when ``job.json`` is unreadable.
    parent: str | None
    #: Its ``job.json`` (or, when its step was asked for, its ``state.json``) could not be read.
    unreadable: bool


def _parent_here(ref: _kernel.JobRef, present: Container[str], *, step: bool) -> _Parentage:
    """Return whether *ref* moves only with its root, (with *step*) its step or why that is unknown, the job id of
    its ``job.json`` parent, and whether a document could not be read.

    A job moves only in its root's tree when it is a non-detached child of a job
    in *present*. ``state.json`` is read only for such a child or for the step.
    """

    job, job_error = read_job(ref)
    if job is None:
        return _Parentage(False, None, f"{ref.job_key}: {job_error}" if step else None, None, True)
    parent = None if job.parent is None else str(job.parent.get("job_id"))
    child = parent is not None and parent in present
    if not (child or step):
        return _Parentage(False, None, None, parent, False)
    doc, state_error = read_state(ref)
    if step and state_error is not None:
        return _Parentage(False, None, f"{ref.job_key}: its step is unknown: {state_error}", parent, True)
    deferred = child and (doc is None or doc.detached is None)
    return _Parentage(deferred, current_step(job, doc) if step else None, None, parent, False)


def _deferred_note(left: int, workspace: Workspace, tree: bool) -> str:
    """The note on the *left* selected jobs that move only with their root."""

    return f"{left} selected job(s) have a parent in {workspace.root} and move only with their root " + (
        "(detach them to move them alone)" if tree else "(use --tree, or detach them)"
    )


def hold_selected(
    workspace: Workspace,
    context: CLIContext,
    arguments: argparse.Namespace,
    errors: list[str],
    *,
    locator: str,
    destination_id: str | None,
    workflows: bool,
) -> list[_moving.Hold]:
    """Hold every job the selection of *arguments* names in the local *workspace* for a transfer to *locator*, all
    of them before anything is driven (``job transfer --hold``; a transfer holds page by page, :class:`_Selection`).

    The selection is ``jobs``, ``state``, ``step``, ``placement`` and ``tree`` as ``job transfer`` parses them; a
    job that cannot move is an error appended to *errors*. Notes (a job left out by ``--step``, children that move
    only with their root) go to standard error.

    :param workspace: The source workspace.
    :param context: The CLI invocation (``--job`` selectors resolve against its working directory).
    :param arguments: The selection.
    :param errors: Takes the errors.
    :param locator: The destination each hold records.
    :param destination_id: The destination workspace's id each hold records, if known.
    :param workflows: Have each hold report the workflows its members need.
    :return: The holds made, in the order they were made.
    """

    holds: list[_moving.Hold] = []
    require_cli_modifiable(workspace)
    refs = resolve_job_selectors(workspace, context.cwd, arguments.jobs)
    steps = None if arguments.step is None else frozenset(arguments.step)
    if steps is not None:
        named, refs = refs, []
        for ref in named:
            found = _parent_here(ref, set(), step=True)
            if found.damage is not None:
                errors.append(found.damage)
            elif found.step in steps:
                refs.append(ref)
            else:
                print(_left_out(ref, found.step, steps), file=sys.stderr)
    children: list[_kernel.JobRef] = []
    # Held one by one: the --job selectors, then (concurrently, unless a listed root has its parent here, whose
    # hold looks at that parent's children) the listed roots.
    explicit, sequential = len(refs), False
    if arguments.state or (steps is not None and not arguments.jobs):
        seen = {ref.job_id for ref in refs}
        # Listed in full first: holding moves jobs out of the directories the listing walks.
        states = arguments.state or _MOVABLE_STATES
        listed = list(iter_jobs(workspace, states, placement_prefix=arguments.placement))
        present = {ref.job_id for ref in iter_jobs(workspace)}
        for ref in listed:
            if ref.job_id not in seen:
                seen.add(ref.job_id)
                found = _parent_here(ref, present, step=steps is not None)
                if found.damage is not None:
                    errors.append(found.damage)
                    continue
                if steps is not None and found.step not in steps:
                    continue
                # A child whose parent is here moves only in its root's tree, never on its own.
                (children if found.deferred else refs).append(ref)
                sequential = sequential or (not found.deferred and found.parent in present)
    moving: set[str] = set()

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
        if isinstance(outcome, str):
            errors.append(outcome)
        else:
            holds.append(outcome)
            moving.update(outcome.members)

    workers = workspace.io_workers
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
        print(_deferred_note(left, workspace, arguments.tree), file=sys.stderr)
    return holds


def _left_out(ref: _kernel.JobRef, step: str | None, steps: frozenset[str]) -> str:
    """The note on a ``--job`` selection left out by ``--step``."""

    return f"{ref.job_key} is at step {step}, not --step {', '.join(sorted(steps))}: left out"


#: A page of a paged selection (plan §7.8) is decided before anything of it is held: it closes after this many
#: candidates to attempt (each makes at most one hold), ...
_PAGE_HOLDS = 256
#: ... this many errors, ...
_PAGE_ERRORS = 256
#: ... or this many candidates scanned; while it is held, it stops once its holds carry this many job keys (checked
#: between sub-batches of ``io.workers`` roots, so it may overshoot by one sub-batch; a tree is never split).
_PAGE_SCANNED = 4096
_PAGE_MEMBERS = 16384


class _Ids(Container[str]):
    """A set of job UUIDs kept as 16-byte values (a snapshot's)."""

    __slots__ = ("_ids",)

    def __init__(self) -> None:
        self._ids: set[bytes] = set()

    def add(self, job_id: str) -> None:
        """Add *job_id*.

        :param job_id: A job UUID.
        """

        self._ids.add(uuid.UUID(job_id).bytes)

    def __contains__(self, job_id: object) -> bool:
        try:
            return uuid.UUID(str(job_id)).bytes in self._ids
        except ValueError:
            return False


class _Attempt(NamedTuple):
    """A candidate a page decided to hold."""

    ref: _kernel.JobRef
    #: Named by ``--job`` (held one at a time, in order; re-located once when it moved).
    explicit: bool
    #: Its hold looks at another selected job's children (a listed root whose parent is here): held one at a time.
    serial: bool
    #: Re-located already.
    relocated: bool = False


class _NotHeld(enum.Enum):
    """Why an attempt made no hold and is no error."""

    #: A listed job that changed since the listing (its claim missed): counted.
    CHANGED = "changed"
    #: A re-located ``--job`` job no longer at a ``--step`` step: left out with its line.
    LEFT_OUT = "left out"


@dataclass(frozen=True)
class _Page:
    """One page of a :class:`_Selection`, held."""

    #: The holds made, in order.
    holds: list[_moving.Hold]
    #: The page's errors.
    errors: list[str]
    #: Listed jobs left out because they changed since the listing.
    changed: int
    #: Nothing of the selection is left.
    done: bool


def _key_id(job_key: str) -> bytes:
    """The job UUID a job key ends with, as 16 bytes."""

    return uuid.UUID(job_key[-36:]).bytes


class _Selection:
    """The selection of one transfer from a local SRC, held page by page (plan §7.8).

    Constructing it resolves and validates the ``--job`` selectors (step 0: a bad one raises, before anything is
    held); :meth:`snapshot` then takes the ``--state``/``--placement`` listing and the ids present in SRC once
    (step 2); :meth:`next_page` decides and holds the next page (step 3), and :meth:`note` is the note on the jobs
    left to move with their root once the last page is held. Per candidate, in order (the ``--job`` refs first): a
    job that went with an earlier hold of this selection (:attr:`moving`) is passed over unread; a listed one whose
    documents are gone (or whose claim misses) changed since the listing and is counted, a ``--job`` one is re-located
    by id once (and its step read again); the parent rule and ``--step`` apply as :func:`hold_selected` applies them.

    :param workspace: The source workspace.
    :param cwd: The directory ``--job`` selectors resolve against.
    :param arguments: The selection: ``jobs``, ``state``, ``step``, ``placement`` and ``tree``.
    :param locator: The destination each hold records.
    :param destination_id: The destination workspace's id each hold records, if known.
    :param workflows: Have each hold report the workflows its members need.
    :param tell: Told ``list`` with the selectors resolved and jobs looked at, and ``hold`` with the attempts of a
        page made, from right before the page's owner is registered (from when on anything may have changed).
    :raises ValueError: For a selector that names no job (or the other errors of resolving one).
    """

    def __init__(
        self,
        workspace: Workspace,
        cwd: Path,
        arguments: argparse.Namespace,
        *,
        locator: str,
        destination_id: str | None,
        workflows: bool,
        tell: Callable[[str, int | None], object] | None = None,
    ) -> None:
        self.workspace = workspace
        self._locator, self._destination_id, self._workflows = locator, destination_id, workflows
        self._tell = tell or (lambda _phase, _count: None)
        self._looked = 0
        self._tell("list", 0)
        require_cli_modifiable(workspace)
        self._explicit = resolve_job_selectors(workspace, cwd, self._resolving(arguments.jobs))
        self._steps = None if arguments.step is None else frozenset(arguments.step)
        self._listing = bool(arguments.state or (self._steps is not None and not arguments.jobs))
        self._states = tuple(arguments.state or _MOVABLE_STATES)
        self._placement: str | None = arguments.placement
        self._tree = bool(arguments.tree)
        #: The snapshot: each listed job as ``(state, placement, name)``; taken once (``_snapshot``).
        self._listed: list[tuple[str, str, str]] = []
        self._present = _Ids()
        self._snapshot = not self._listing
        self._cursor = 0
        #: Decided candidates a page's limits left for the next one.
        self._pending: list[_Attempt] = []
        #: The job keys this selection's holds took.
        self.moving: set[str] = set()
        self._deferred: set[str] = set()
        #: The ids of the candidates no page looked at yet, and those of them that moved with an earlier hold.
        self._unconsumed = {uuid.UUID(ref.job_id).bytes for ref in self._explicit}
        self._ahead: set[bytes] = set()
        #: The attempts made (held, failed or left out), cumulative.
        self._attempted = 0

    def _look(self) -> None:
        self._looked += 1
        self._tell("list", self._looked)

    def _resolving(self, selectors: Sequence[str]) -> Iterator[str]:
        for selector in selectors:
            yield selector
            self._look()  # resolved by the time the next one is asked for

    @property
    def candidates(self) -> int:
        """How many candidates the selection has: the ``--job`` refs and the listed jobs."""

        return len(self._explicit) + len(self._listed)

    @property
    def snapshotted(self) -> bool:
        """Whether the listing was taken (or the selection needs none)."""

        return self._snapshot

    @property
    def consumed(self) -> int:
        """The candidates looked at and the attempts made or retired (gone with an earlier tree) so far: it grows with
        every page that is not empty."""

        return self._cursor + self._attempted

    @property
    def done(self) -> bool:
        """Whether no candidate is left."""

        return self._snapshot and not self._pending and self._cursor >= self.candidates

    def snapshot(self, tell: Callable[[str, int | None], object] | None = None) -> None:
        """Take the listing of the selected states (under the placement) and the ids of every job present in SRC
        (``owned/`` and ``away/`` included) once (step 2), de-duplicated against the ``--job`` refs; a later call does
        nothing. Nothing is taken when this raises.

        :param tell: Told the progress instead of the selection's own *tell* from now on.
        """

        if tell is not None:
            self._tell = tell
        if self._snapshot:
            return
        seen = _Ids()
        for ref in self._explicit:
            seen.add(ref.job_id)
        listed: list[tuple[str, str, str]] = []
        ids: set[bytes] = set()
        for ref in iter_jobs(self.workspace, self._states, placement_prefix=self._placement):
            self._look()
            if ref.job_id not in seen:
                seen.add(ref.job_id)
                listed.append((sys.intern(ref.state), sys.intern(str(ref.placement)), ref.path.name))
                ids.add(uuid.UUID(ref.job_id).bytes)
        present = _Ids()
        for ref in iter_jobs(self.workspace):
            self._look()
            present.add(ref.job_id)
        self._listed, self._present, self._snapshot = listed, present, True
        self._unconsumed |= ids

    def remaining(self) -> int:
        """How many selected jobs no page attempted yet, those that moved with an earlier hold not counted.

        :return: The count.
        """

        pending = sum(attempt.ref.job_key not in self.moving for attempt in self._pending)
        return self.candidates - self._cursor - len(self._ahead) + pending

    def note(self) -> str | None:
        """The note on the selected jobs that move only with their root (and were not moved with it).

        :return: The note, or ``None`` when there are none.
        """

        left = len(self._deferred - self.moving)
        return _deferred_note(left, self.workspace, self._tree) if left else None

    def _candidate(self, index: int) -> tuple[_kernel.JobRef, bool]:
        if index < len(self._explicit):
            return self._explicit[index], True
        state, placement, name = self._listed[index - len(self._explicit)]
        where = PurePosixPath(placement)
        return _kernel.JobRef.from_path(self.workspace.jobs / state / where / name, state=state, placement=where), False

    def _relocate(self, ref: _kernel.JobRef) -> _kernel.JobRef | None:
        """Find a ``--job`` ref that moved, by id at its placement (``owned/`` included): unowned elsewhere, or
        ``None``."""

        found = _kernel.locate(self.workspace, ref.job_id, placement_hint=ref.placement)
        return found if found is not None and found.state != _kernel.OWNED and found.path != ref.path else None

    def _decide(
        self, ref: _kernel.JobRef, explicit: bool, deferred: list[str], changed: list[str], *, relocated: bool = False
    ) -> _Attempt | str | None:
        """Decide one candidate: an attempt, an error, or ``None`` (left out: by ``--step``, as a child that moves only
        with its root, or as changed since the listing, which *changed* records)."""

        steps = self._steps
        if explicit and steps is None:
            return _Attempt(ref, True, True, relocated)
        found = _parent_here(ref, frozenset() if explicit else self._present, step=steps is not None)
        if found.unreadable and _gone(ref.path):
            if not explicit:
                changed.append(ref.job_key)
                return None
            if not relocated and (moved := self._relocate(ref)) is not None:
                return self._decide(moved, True, deferred, changed, relocated=True)
            return f"{ref.job_key} moved before it could be claimed"
        if found.damage is not None:
            return found.damage
        if steps is not None and found.step not in steps:
            if explicit:
                print(_left_out(ref, found.step, steps), file=sys.stderr)
            return None
        if explicit:
            return _Attempt(ref, True, True, relocated)
        if found.deferred:
            # A child whose parent is here moves only in its root's tree, never on its own.
            deferred.append(ref.job_key)
            return None
        return _Attempt(ref, False, found.parent is not None and found.parent in self._present)

    def _recheck(self, ref: _kernel.JobRef) -> str | _NotHeld | None:
        """Check a ``--job`` ref re-located at its claim as it was decided (its ``--step``; never the parent rule): an
        error, :attr:`_NotHeld.LEFT_OUT`, or ``None`` to claim it."""

        if self._steps is None:
            return None
        found = _parent_here(ref, frozenset(), step=True)
        if found.unreadable and _gone(ref.path):
            return f"{ref.job_key} moved before it could be claimed"
        if found.damage is not None:
            return found.damage
        if found.step not in self._steps:
            print(_left_out(ref, found.step, self._steps), file=sys.stderr)
            return _NotHeld.LEFT_OUT
        return None

    def next_page(
        self,
        *,
        holds: int | None,
        errors: int | None,
        scanned: int | None,
        members: int | None,
        measure: Callable[[_moving.Hold | str], int] | None = None,
        budget: int | None = None,
        tell: Callable[[str, int | None], object] | None = None,
    ) -> _Page:
        """Decide the next page, then hold it under one owner, closed before this returns.

        The page closes after *holds* candidates to attempt (the attempts an earlier page left count first), *errors*
        errors or *scanned* candidates looked at (``None``: no such limit); the attempts are then made in order: the
        ``--job`` refs one at a time, the listed roots pooled over ``io.workers`` unless one of them has its parent
        here. Once the page has *errors* errors (deciding and holding together), its holds carry *members* job keys, or
        its errors and holds measure *budget* (each as *measure* says), checked between attempts (between sub-batches
        of ``io.workers`` roots when pooled), the rest of its attempts are left for the next page.

        :param holds: The most candidates attempted.
        :param errors: The most errors.
        :param scanned: The most candidates looked at.
        :param members: The job keys at which holding stops.
        :param measure: The size of a hold or an error in the page's answer.
        :param budget: The size at which holding stops.
        :param tell: Told the page's progress instead of the selection's own *tell*.
        :return: The page.
        """

        tell = tell or self._tell
        self.snapshot()  # taken already (step 2), or now: a page never runs without its listing
        if self.done:
            return _Page([], [], 0, True)
        found: list[str] = []
        changed: list[str] = []
        deferred: list[str] = []
        consumed: list[bytes] = []
        carried = self._pending if holds is None else self._pending[:holds]
        later = self._pending[len(carried) :]
        batch = list(carried)
        cursor, looked = self._cursor, 0
        while (
            not later
            and cursor < self.candidates
            and (holds is None or len(batch) < holds)
            and (errors is None or len(found) < errors)
            and (scanned is None or looked < scanned)
        ):
            ref, explicit = self._candidate(cursor)
            cursor += 1
            looked += 1
            consumed.append(uuid.UUID(ref.job_id).bytes)
            tell("list", looked)
            if ref.job_key in self.moving:
                continue  # it went with an earlier hold: nothing read
            decided = self._decide(ref, explicit, deferred, changed)
            if isinstance(decided, _Attempt):
                batch.append(decided)
            elif decided is not None:
                found.append(decided)
        tell("hold", 0)
        # Committed before the owner's registration, from when on anything may have changed.
        self._cursor, self._pending = cursor, later
        self._deferred.update(deferred)
        for job_id in consumed:
            if job_id in self._ahead:
                self._ahead.discard(job_id)
            else:
                self._unconsumed.discard(job_id)
        made: list[_moving.Hold] = []
        size = 0 if measure is None else sum(measure(error) for error in found)
        held = handled = missed = 0

        def full() -> bool:
            return (
                (members is not None and held >= members)
                or (budget is not None and size >= budget)
                or (errors is not None and len(found) >= errors)
            )

        def hold_one(attempt: _Attempt) -> _moving.Hold | str | _NotHeld:
            ref = attempt.ref
            try:
                root = _kernel.claim(self.workspace, owner, ref)
                relocate = root is None and attempt.explicit and not attempt.relocated
                if relocate and (moved := self._relocate(ref)) is not None:
                    ref = moved
                    if (rechecked := self._recheck(ref)) is not None:
                        return rechecked
                    root = _kernel.claim(self.workspace, owner, ref)
                if root is None:
                    # A listed job that changed since the listing is left out, counted; a --job one is an error.
                    return f"{ref.job_key} moved before it could be claimed" if attempt.explicit else _NotHeld.CHANGED
                outcome = _eject_claimed(
                    self.workspace,
                    owner,
                    root,
                    None,
                    tree=self._tree,
                    locator=self._locator,
                    destination_workspace_id=self._destination_id,
                    workflows=self._workflows,
                )
            except _ERRORS as exc:
                return f"{ref.job_key}: {exc}"
            assert not isinstance(outcome, _moving.EjectReport)
            return outcome

        def record(_index: int, outcome: _moving.Hold | str | _NotHeld) -> None:
            nonlocal handled, held, size, missed
            if outcome is _NotHeld.CHANGED:
                missed += 1
            elif isinstance(outcome, str):
                found.append(outcome)
                size += 0 if measure is None else measure(outcome)
            elif isinstance(outcome, _moving.Hold):
                made.append(outcome)
                self.moving.update(outcome.members)
                for key in outcome.members:
                    if (job_id := _key_id(key)) in self._unconsumed:
                        self._unconsumed.discard(job_id)
                        self._ahead.add(job_id)
                held += len(outcome.members)
                size += 0 if measure is None else measure(outcome)
            handled += 1
            self._attempted += 1
            tell("hold", handled)

        named = [attempt for attempt in batch if attempt.explicit]
        listed = [attempt for attempt in batch if not attempt.explicit]
        workers = self.workspace.io_workers
        pooled = workers > 1 and not any(attempt.serial for attempt in listed)
        step = workers if pooled else 1
        leftover: list[_Attempt] = []
        if not batch:
            return _Page([], found, len(changed), self.done)  # nothing to attempt: no owner
        try:
            registered = _owner(self.workspace, "job transfer")
        except BaseException:
            self._pending = [*batch, *self._pending]  # nothing attempted: left for a later page
            raise
        with registered as owner:
            for index, attempt in enumerate(named):
                if full():
                    leftover = [*named[index:], *listed]
                    break
                if attempt.ref.job_key not in self.moving:
                    record(index, hold_one(attempt))
                else:
                    self._attempted += 1  # it went with an earlier selected tree: retired, nothing claimed
            else:
                start = 0
                while start < len(listed):
                    if full():
                        leftover = listed[start:]
                        break
                    # No more attempts at once than errors the page may still take: the rest stays for later.
                    width = step if errors is None else max(min(step, errors - len(found)), 1)
                    batch_attempts, start = listed[start : start + width], start + width
                    # Pooled: disjoint trees, since no listed root has a parent here, so none is in another's tree.
                    run = [attempt for attempt in batch_attempts if attempt.ref.job_key not in self.moving]
                    self._attempted += len(batch_attempts) - len(run)  # retired: they went with an earlier tree
                    if pooled:
                        run_batch(owner, hold_one, run, workers, emit=record)
                        continue
                    for attempt in run:
                        if attempt.ref.job_key not in self.moving:
                            record(0, hold_one(attempt))
                        else:
                            self._attempted += 1
        self._pending = [*leftover, *self._pending]
        return _Page(made, found, len(changed) + missed, self.done)


def _gone(path: Path) -> bool:
    """Whether *path* no longer exists (a job that moved since it was listed)."""

    try:
        os.lstat(path)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False


def _held_pages(
    workspace: Workspace, names: list[str], destination: str | None, *, workflows: bool
) -> Iterator[tuple[list[_moving.Hold], bool]]:
    """The holds of a local SRC among *names* (its transfer ids, listed once) bound for *destination* (every one
    when ``None``), a page at a time, each read when its page is (a name gone since the listing is passed over).

    :param workspace: The source workspace.
    :param names: The transfer ids, in order.
    :param destination: The destination locator, or ``None``.
    :param workflows: Report the workflows each hold's members need.
    :yields: The pages, none of them empty, each with whether names are left after it.
    """

    index = 0
    while index < len(names):
        page: list[_moving.Hold] = []
        members = looked = 0
        while index < len(names) and len(page) < _PAGE_HOLDS and members < _PAGE_MEMBERS and looked < _PAGE_SCANNED:
            hold = _moving.held_one(workspace, names[index], workflows=workflows)
            index += 1
            looked += 1
            if hold is not None and destination in (None, hold.destination_locator):
                page.append(hold)
                members += len(hold.members)
        if page:
            yield page, index < len(names)


def hold_document(
    workspace: Workspace,
    holds: list[_moving.Hold],
    locator: str,
    errors: list[str],
) -> dict[str, Any]:
    """Return the document ``job transfer --hold --json`` prints.

    Besides the new *holds* and the *errors*, it lists the other holds of *workspace* already bound for *locator*
    (``held_before``), so that a client needs no ``transfer status`` call; their workflows are resolved for these
    only, not for every hold of the workspace.

    :param workspace: The source workspace.
    :param holds: The holds just made.
    :param locator: The destination they record.
    :param errors: The errors of the hold.
    :return: ``{"holds", "held_before", "errors"}``.
    """

    new = {hold.transfer_id for hold in holds}
    before = []
    for hold in _moving.held(workspace):
        if hold.destination_locator != locator or hold.transfer_id in new:
            continue
        try:
            read = read_manifest(Path(hold.path) / "bundle.json")
        except BundleError:
            read = None  # changed since it was listed: reported without workflows, as by an older SRC
        needed = None if read is None else _moving._needed_workflows(workspace, Path(hold.path), read[1])
        before.append(replace(hold, workflows=needed).as_mapping())
    return {"holds": [hold.as_mapping() for hold in holds], "held_before": before, "errors": errors}


def _hold_jobs(
    src: _End,
    dst: _End,
    context: CLIContext,
    arguments: argparse.Namespace,
    errors: list[str],
) -> tuple[list[_moving.Hold], list[_moving.Hold] | None]:
    """Step 1 of hold-everything-first (``job transfer --hold``, and a remote *src* on the adapter path): hold every
    selected job on *src*, recording *dst* and its id when known; a job that cannot move is an error.

    The result is the new holds, and for a remote *src* the other holds it reported bound for *dst*
    (``held_before``), or ``None`` (a local *src*, or a hold call that printed no document).
    """

    if src.workspace is not None:
        held = hold_selected(
            src.workspace,
            context,
            arguments,
            errors,
            locator=dst.locator,
            destination_id=dst.workspace_id,
            workflows=arguments.hold,
        )
        return held, None
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


def _checked_adoption(document: Any, bundles: list[str], *, exact: bool = False) -> dict[str, Any]:
    """The checked document of a remote ``job adopt --json --stdin-tar`` of *bundles* (or of the transfer session's
    ``adopt`` or ``adopt_staged`` op): as given, with ``entries`` by bundle added; :class:`ValueError` when it is no
    such document.

    A document with ``refused`` says that nothing was adopted and lists no bundle, and with ``sync_failed`` true
    that the far side's sync commit failed (a writeback error there, possibly another user's); any other names
    every bundle sent, each with a known status. With *exact* (a staged segment's adoption), it names exactly
    *bundles*, each once, and nothing else.
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
    if not all(isinstance(entry, dict) and isinstance(entry.get("bundle"), str) for entry in document["bundles"]):
        raise ValueError("an adoption entry without its bundle")
    entries = {entry["bundle"]: entry for entry in document["bundles"] if entry.get("status") in _ADOPT_STATUSES}
    if workspace_id is None or not all(bundle in entries for bundle in bundles):
        raise ValueError("not an adoption report of the bundles sent")
    if exact and (len(document["bundles"]) != len(bundles) or entries.keys() != set(bundles)):
        raise ValueError("not an adoption report of exactly the bundles staged")
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
    """Steps 2 and 3 for a chunk of a remote *src* that serves no transfer session, bound for a local *dst*: pull its
    held bundles there and adopt them (a local *src*'s chunks are staged instead, :func:`_stage_local`).

    The holds are pulled into a landing scratch, made durable (a sync barrier entered before its first byte,
    committed before any adoption). When the ``syncfs`` commit of a pull fails under ``io.sync_errors`` ``fail``, the
    landing is discarded and the chunk pulled once more into a fresh one under a fresh barrier; a second failure (or
    any other failed commit, such as a walk of strategy ``fsync``, never pulled again) fails the chunk with the holds
    kept at the source.

    The result is each hold's adoption entry by transfer id (``status`` as ``job adopt --json`` gives it); an
    error raised fails the whole chunk.
    """

    assert src.workspace is None and dst.workspace is not None
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


def _stage_local(settled: "_Deliveries", holds: list[_moving.Hold]) -> None:
    """Stage a chunk of a local SRC's holds in a local DST (plan §7.10, 1a, as amended), for its adoption once the
    visibility deadline has run, while the next pages are held: at most :data:`_STAGES` staged chunks wait (a page is
    one chunk unless its ids exceed the command-text budget), so the oldest is adopted first when that many do.

    On one filesystem without ``--keep`` the stage is the first scan over the original holds in SRC's
    ``transfers/outgoing/`` (adoption takes each in; nothing of them is ever discarded); otherwise the holds are
    copied into the stage's landing scratch and committed first (:func:`_land_copies`), and the stage counts their
    landed bytes against :data:`_STAGED_BYTES`. A chunk that cannot be staged fails, held on SRC, and stops the pair
    (nothing more is held for a DST that cannot take it). An interrupt while the stage is made abandons it (its
    owner closed once its writes finished, its copies discarded, the original holds untouched) before it propagates.

    :param settled: The batch's deliveries; its entries get the chunk's adoption entries once it is adopted.
    :param holds: The chunk's holds.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    assert src.workspace is not None and dst.workspace is not None
    pair = settled.pair
    workspace = dst.workspace

    def failed(exc: BaseException) -> None:
        settled.fail(holds, str(exc))
        settled.stopped = f"{dst.locator}: {exc}"  # nothing more is held for a DST that cannot take it

    try:
        # Keeping away copies releases the hold itself, and another filesystem cannot take it by a rename: the
        # adoption takes a durable copy of it, which counts against the staged bytes.
        copied = arguments.keep or _other_filesystem(holds, workspace)
        size = sum(_measure(Path(hold.path))[0] for hold in holds) if copied else 0
    except _ERRORS as exc:
        failed(exc)
        return
    _make_room(pair, size)
    if settled.stopped is not None:
        settled.fail(holds, f"not delivered, since the transfer stopped ({settled.stopped})")
        return
    stage: _moving.Stage | None = None
    try:
        require_cli_modifiable(workspace)
        stage = _moving.Stage(workspace, _owner(workspace, "job transfer"))
        if not copied:
            # One filesystem: adoption moves each hold in, and a refusal moves it back.
            sources = [Path(hold.path) for hold in holds]
        else:
            stage.landing = stage.owner.scratch("landing")
            sources = _land_copies(workspace, stage.owner, holds, stage.landing, arguments)
        stage.commit(sources)
        # Queued inside the guard: from here on an interrupt reaches the stage through the pair's queue.
        pending = _PendingStage(
            holds,
            settled,
            size,
            time.monotonic() + stage.remaining(),
            partial(_adopt_local_stage, settled, stage, holds, stop=True),
            partial(_abandon_local_stage, settled, stage, holds, dst.locator),
        )
        _stage_pending(pair, pending)
    except BaseException as exc:
        if stage is not None and stage.state != "abandoned" and not any(item.holds is holds for item in pair.pending):
            with contextlib.suppress(Exception):  # an owner that cannot close is left for recovery
                stage.abandon()
        if not isinstance(exc, _ERRORS):
            raise  # an interrupt: nothing of the chunk is staged, its holds stay
        failed(exc)
        return
    _adopt_matured(pair)


def _adopt_local_stage(settled: "_Deliveries", stage: _moving.Stage, holds: list[_moving.Hold], *, stop: bool) -> bool:
    """Adopt a stage in this machine's DST once its deadline has run (:meth:`~httk.workflow._moving.Stage.begin`,
    :func:`_adopt_local` for each bundle), recording each hold's adoption entry in *settled*; the stage closes
    either way. A failed adoption fails the holds (some may be adopted: ``--resume`` reports those already
    adopted), and with *stop* stops the pair. A DST that cannot be modified any more (sealed since the stage) adopts
    nothing: the stage is given up (an original hold stays where it is), its holds stay held and the pair stops.

    :param settled: The deliveries of the stage's batch.
    :param stage: The committed stage.
    :param holds: Its holds, in the order of its bundles.
    :param stop: Stop the pair when the adoption fails.
    :return: Whether the holds were adopted (each its own entry).
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    workspace, owner = stage.workspace, stage.owner
    ids = [hold.transfer_id for hold in holds]
    failure: Exception | None = None
    adopted: list[dict[str, Any]] = []
    try:
        require_cli_modifiable(workspace)  # as a far side's adopt_staged does, before anything is adopted
    except _ERRORS as exc:
        with contextlib.suppress(*_ERRORS):  # an owner that cannot close is left for recovery
            stage.abandon()
        settled.fail(
            holds,
            f"not adopted: {dst.locator} cannot take it any more ({exc}); nothing of it was adopted: deliver it "
            f"with `httk job transfer --resume {src.locator} {dst.locator}`",
        )
        settled.stopped = settled.stopped or f"{dst.locator}: {exc}"
        return False
    _say(arguments, f"adopting {len(ids)} bundle(s) in {dst.locator}")
    try:
        absent = stage.begin()
        sources = list(stage.bundles)
        serial = {index for index, source in enumerate(sources) if source in absent.serial}
        adopted = run_batch(
            owner,
            lambda source: _adopt_local(workspace, owner, source, absent.absent),
            sources,
            workspace.io_workers,
            serial=serial,
        )
    except _ERRORS as exc:
        failure = exc
    except (KeyboardInterrupt, SystemExit) as exc:
        stage.owner.__exit__(type(exc), exc, exc.__traceback__)  # quiescent jobs go back, the owner closes
        raise
    try:
        stage.close()  # what is left in its landing was never taken
    except _ERRORS as exc:
        failure = failure or exc  # its owner is left for recovery
    if failure is not None:
        settled.fail(holds, str(failure))
        if stop:
            settled.stopped = settled.stopped or f"{dst.locator}: {failure}"
        return False
    settled.entries.update(zip(ids, adopted, strict=True))
    return True


def _abandon_local_stage(
    settled: "_Deliveries", stage: _moving.Stage, holds: list[_moving.Hold], where: str, reason: str
) -> None:
    """Abandon a stage in this machine's DST that was never adopted, since the transfer stopped (*reason*): its
    landing (copies only) is discarded and its owner closed; the holds stay held."""

    with contextlib.suppress(*_ERRORS):  # an owner that cannot close is left for recovery, which discards its copies
        stage.abandon()
    _abandon_staged(settled, holds, where, reason, copied=stage.landing is not None)


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
            if "token" in document:
                # Staged, never adopted: the other workspace discards the stage when the session ends.
                raise RuntimeError(
                    f"{dst.locator} staged the segment in workspace {second}, not the expected workspace {first} "
                    f"that answered {pair[1]}; nothing of it was adopted, and its holds stay held on {src.locator}: "
                    f"check which workspace {dst.locator} names, then deliver it with `httk job transfer --resume "
                    f"{src.locator} {dst.locator}`"
                )
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


#: The threads that write a relay's small files (the default of ``io.workers``; a relay belongs to no workspace).
_RELAY_WORKERS = 8


def _extract_relay(landing: Path, stream: StreamReader) -> list[str]:
    """Extract a transfer session's download into the relay directory *landing* (created here, inside a temporary
    directory of this machine) by the untrusted-tar rules (:func:`httk.workflow._fs.extract_tar`), and validate its
    terminal before anything of it is used: the top-level directories it carried, sorted.

    A relay is no workspace landing: nothing is synchronized (the destination makes what it adopts durable), and a
    refused stream is not discarded here (its temporary directory goes with it).

    :param landing: The directory to extract into, which must not exist yet.
    :param stream: The download.
    :return: The names of the top-level directories.
    :raises httk.workflow._landing.StreamRefused: For a malformed, aborted or cut stream, a refused member (naming
        its bundle), a top-level entry that is no directory, or a local failure (a full disk).
    """

    names: set[str] = set()
    try:
        landing.mkdir()
        _fs.extract_tar(stream, landing, workers=_RELAY_WORKERS, top_level=names)
        stream.finish()  # the stream is whole before anything of it is used
        for name in names:
            found = _fs.lstat(_fs.loc(landing / name))
            if found is None or not stat.S_ISDIR(found.st_mode):
                raise _landing.StreamRefused(f"not a directory: {name!r}", bundle=name)
    except (tarfile.TarError, EOFError, OSError, ValueError, StreamAborted, TransferProtocolError) as exc:
        message = f"refused the stream, nothing of it was used: {exc}"
        bundle = _landing._offending_bundle(exc)
        raise _landing.StreamRefused(message, bundle=bundle, stream_aborted=isinstance(exc, StreamAborted)) from None
    return sorted(names)


def _receive_store(
    session: TransferSession, src: _End, workspace: Workspace | None, landing: Path, directories: list[str]
) -> None:
    """Fetch the store entries *directories* of a remote *src* into *landing* in the local *workspace* (``None``: a
    relay, see :func:`_extract_relay`) with one ``send`` request on SRC's transfer *session* (not synchronized: the
    installation copies them durably).

    :param session: SRC's transfer session.
    :param src: The remote source end.
    :param workspace: The local destination workspace, or ``None`` for a relay.
    :param landing: The directory they land in.
    :param directories: The store directory names.
    :raises RuntimeError: When they did not arrive whole (:class:`~httk.workflow.adapters.SessionFailed` when the
        session failed).
    """

    def download(stream: BinaryIO) -> None:
        assert isinstance(stream, StreamReader)
        if workspace is None:
            _extract_relay(landing, stream)
        else:
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
    source: TransferSession | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str | None]]:
    """In one remote ``httk`` start, install SRC's store entries ``<store>/<directory>/{install.json,package}``
    in a remote *dst* (one ``deliver`` to ``workflow install --stdin-tar --build``, a remote *src*'s entries relayed
    through a local directory) and build the installations *rebuild* names (``--also-build``); with no
    directories, only build them (one ``invoke``). With *dst*'s transfer *session*, one ``install`` request on it
    instead (the entries uploaded: a local *src*'s from its store, a remote one's from a relay), in the far side's
    ``httk`` start of the transfer. A remote *src*'s entries are fetched into the relay with one ``send`` request on
    its transfer session *source* when there is one, else with the adapter's ``pull``.

    The result is the install rows by directory, and each *rebuild* id's build failure (``None`` when built); a
    :class:`RuntimeError` says that *dst* gave no usable answer.
    """

    label = f"transfer from {src.locator}"
    also = [item for workflow_id in rebuild for item in ("--also-build", workflow_id)]
    files = [f"{directory}/{name}" for directory in directories for name in ("install.json", "package")]
    argv = [*_WORKFLOW_COMMAND, "install", "--json", "--workspace", dst.plain, *also]
    if session is not None:
        _say(arguments, f"installing {len(directories)} and building {len(rebuild)} workflow(s) in {dst.locator}")
        if src.workspace is None and directories:
            assert store is not None
            with tempfile.TemporaryDirectory(prefix="httk-transfer-") as relay:
                _fetch_store(src, source, store, directories, Path(relay) / "workflows", context, arguments)
                code, out, err = _install_over_session(session, dst, f"{relay}/workflows", directories, rebuild, label)
        else:
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
                _fetch_store(src, source, store, directories, Path(relay) / "workflows", context, arguments)
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


def _fetch_store(
    src: _End,
    source: TransferSession | None,
    store: str,
    directories: list[str],
    target: Path,
    context: CLIContext,
    arguments: argparse.Namespace,
) -> None:
    """Fetch a remote *src*'s store entries ``<store>/<directory>/{install.json,package}`` into the local relay
    directory *target*: with one ``send`` request on its transfer session *source* when there is one (see
    :func:`_receive_store`), else with the adapter's ``pull`` (a call per 64 KiB of names).

    :param src: The remote source end.
    :param source: SRC's transfer session, or ``None``.
    :param store: SRC's store directory.
    :param directories: The store directory names.
    :param target: The relay directory they land in.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    """

    if source is not None:
        _receive_store(source, src, None, target, directories)
        return
    files = [f"{directory}/{name}" for directory in directories for name in ("install.json", "package")]
    for chunk in _chunks(files):
        _adapter(src, context, arguments, "pull", store, str(target), chunk)


def _ship_workflows(
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
    have: dict[str, tuple[object, bool]] | None = None,
    session: TransferSession | None = None,
    source: TransferSession | None = None,
) -> dict[str, dict[str, Any]]:
    """Install in *dst*, under the same ids, the workflows the holds need that it lacks and *src* has.

    The result maps every needed id to ``{"status", "reason"?, "build"?}``: ``present`` or ``mismatch`` (*dst*
    already has it, with the same or another tree, and keeps it), ``missing`` (installed in neither),
    ``installed``, ``refused``, ``failed`` or ``unbuilt``; ``build`` marks one a local *dst*'s
    :func:`_build_shipped` builds. A remote *dst* reported what it has (*have*) with the adoption (or, over a push's
    transfer *session*, before it), and builds in the same start that installs (on the session when there is one);
    a remote *src*'s entries reach it through a relay, fetched over SRC's transfer session *source* when there is
    one. A local *dst* fetches the entries from a remote *src* over a pull's transfer *session* when there is one.
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
            rows, failures = _install_remote(src, dst, base, sorted(ship), rebuild, context, arguments, session, source)
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


#: The most transfer ids one ``release`` request over a transfer session carries (its header stays far below a frame).
_RELEASE_IDS = 16384
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
    holds: list[_moving.Hold],
    measure: Callable[[_moving.Hold], tuple[int, int]] | None = None,
    sizes: dict[str, int] | None = None,
) -> list[list[_moving.Hold]]:
    """Split *holds*, in order, into the segments of a push: each closed once it reaches :data:`_SEGMENT_BUNDLES`,
    :data:`_SEGMENT_BYTES` or :data:`_SEGMENT_MEMBERS`, and before a hold that would take it beyond the bytes or
    the members (so a hold beyond either alone is a segment of its own). Each hold's bytes and members are what
    *measure* says, by default its local directory's (:func:`_measure`); *sizes*, when given, gets each hold's bytes
    by transfer id (a staged segment declares them without measuring again)."""

    segments: list[list[_moving.Hold]] = []
    current: list[_moving.Hold] = []
    size = members = 0
    for hold in holds:
        hold_size, hold_members = _measure(Path(hold.path)) if measure is None else measure(hold)
        if sizes is not None:
            sizes[hold.transfer_id] = hold_size
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
#: ... and a pull of its source's (paged holds: a far side without them takes the adapter operations).
_PULL_OPS = frozenset({"holds_page", "select", "hold_page", "send", "release"})


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

    if _session_disabled(end, context):
        return None
    assert end.binding is not None
    target = resolve_remote(end.binding.remote, project=context.cwd)
    session = sessions.enter_context(TransferSession(target.bundle, needs=needs, timeout=arguments.adapter_timeout))
    try:
        session.open(interactive=sys.stdin.isatty())
    except SessionUnsupported as exc:
        fallback = "one adapter call a delivery" if needs == _PUSH_OPS else "the adapter operations"
        _say(arguments, f"{end.locator} serves no transfer session ({exc}); using {fallback}")
        return None
    return session


def _session_disabled(end: _End, context: CLIContext) -> bool:
    """Whether the remote *end* takes the adapter operations without being asked: its setting ``transfer_session``
    is ``off``, or it is a ``mount-daemon`` remote."""

    assert end.binding is not None
    target = resolve_remote(end.binding.remote, project=context.cwd)
    return (
        remote_settings(target.bundle).get("transfer_session") == "off"
        or read_metadata(target.bundle).get("kind") == "mount-daemon"
    )


class _SourceSession:
    """The transfer session of a pull from a remote SRC: opened at the command's first request to SRC, closed with
    the command's other sessions.

    :param src: The remote source end.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :param sessions: Closes the command's transfer sessions at its end.
    """

    def __init__(
        self, src: _End, context: CLIContext, arguments: argparse.Namespace, sessions: contextlib.ExitStack
    ) -> None:
        self._open = partial(_push_session, src, context, arguments, sessions, _PULL_OPS)
        self._opened = False
        self._session: TransferSession | None = None
        self._failure: str | None = None
        #: A landing's sync commit failed in this session: every later fetch of it, for any destination, goes with an
        #: fsync of every file.
        self.fsync = False

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

    :param session: DST's open transfer session.
    :param src: The source end.
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
    response, aborted = _upload_holds(session, "adopt", header, holds, src, dst, arguments)
    return _checked_upload(response, aborted, ids, src, dst, arguments), str(response.get("stderr", ""))


def _upload_holds(
    session: TransferSession,
    op: str,
    header: dict[str, Any],
    holds: list[_moving.Hold],
    src: _End,
    dst: _End,
    arguments: argparse.Namespace,
) -> tuple[dict[str, Any], str]:
    """One *op* request on *session* uploading *holds* as one tar archive, each as ``<transfer_id>/...``, written as
    it is read: the response, and (when this side's upload failed: a hold changed under the writer) why, for the
    message; the far side's answer to the aborted stream is the response then.

    :param session: DST's open transfer session.
    :param op: The op (``adopt`` or ``stage``).
    :param header: Its header.
    :param holds: The holds to upload.
    :param src: The source end.
    :param dst: The remote destination end.
    :param arguments: The command's arguments.
    :return: The response, and the reason of this side's failed upload (empty when it did not fail).
    :raises RuntimeError: When the session failed: whether anything was adopted is unknown.
    """

    def upload(stream: BinaryIO) -> None:
        with tarfile.open(fileobj=stream, mode="w|", bufsize=DATA_LIMIT) as archive:
            for hold in holds:
                archive.add(hold.path, arcname=hold.transfer_id)

    try:
        return session.request(op, header, upload=upload), ""
    except RequestAborted as exc:
        return exc.response, f"; this side's upload failed: {exc}"
    except SessionFailed as exc:
        raise _unknown_adoption(f"the transfer session to {dst.locator} failed: {exc}", src, dst, arguments) from None


def _checked_upload(
    response: dict[str, Any], aborted: str, ids: list[str], src: _End, dst: _End, arguments: argparse.Namespace
) -> dict[str, Any]:
    """The checked adoption document of an ``adopt`` request's *response* (or the refusal of a ``stage`` request) of
    the bundles *ids*; after this side's own *aborted* upload only a ``refused`` document is a usable one.

    :param response: The response.
    :param aborted: Why this side's upload failed, or empty.
    :param ids: The transfer ids uploaded.
    :param src: The source end.
    :param dst: The remote destination end.
    :param arguments: The command's arguments.
    :return: The checked document (:func:`_checked_adoption`).
    :raises RuntimeError: When the answer is unusable: whether anything was adopted is unknown.
    """

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
    return document


class _Capacity(Exception):
    """DST refused a ``stage`` request at its capacity, before it used the upload: adopt a staged segment first."""


def _session_stage(
    session: TransferSession,
    src: _End,
    dst: _End,
    arguments: argparse.Namespace,
    holds: list[_moving.Hold],
    size: int,
    fsync: bool,
) -> tuple[dict[str, Any], str]:
    """One ``stage`` request of *holds* (*size* landed bytes) on *session* (plan §7.10): ``{"token", "workspace_id",
    "matures_in", "bytes"}`` (the bytes that landed) once DST holds the segment committed and its first scan taken, or the checked refusal document
    (see :func:`_checked_upload`); and the far side's standard error.

    :param session: DST's open transfer session.
    :param src: The source end.
    :param dst: The remote destination end.
    :param arguments: The command's arguments.
    :param holds: The segment's holds.
    :param size: Their landed regular-file bytes.
    :param fsync: Ask for ``--sync fsync``.
    :return: The stage (or refusal) document and the far side's standard error.
    :raises _Capacity: When DST refused it for its capacity (nothing of it used).
    :raises RuntimeError: When the answer is unusable or the session failed: the outcome is unknown (DST may hold
        the segment staged; it never adopts it unasked).
    """

    ids = [hold.transfer_id for hold in holds]
    header: dict[str, Any] = {"workspace": dst.plain, "transfer_ids": ids, "bytes": size}
    if fsync:
        header["sync"] = "fsync"
    response, aborted = _upload_holds(session, "stage", header, holds, src, dst, arguments)
    if response.get("ok") is False and response.get("capacity") is True and "document" not in response:
        raise _Capacity(str(response.get("error")))
    if response.get("ok") is not True:
        return _checked_upload(response, aborted, ids, src, dst, arguments), str(response.get("stderr", ""))
    token, matures, landed = response.get("token"), response.get("matures_in"), response.get("bytes")
    try:
        workspace_id = canonical_uuid(str(response.get("workspace_id")), "workspace_id")
        if aborted:
            raise ValueError("a stage of an aborted upload")
        if not isinstance(token, str) or not token or response.get("transfer_ids") != ids:
            raise ValueError("a stage of other bundles")
        if isinstance(matures, bool) or not isinstance(matures, (int, float)) or not 0 <= matures < math.inf:
            raise ValueError(f"a stage maturing in {matures!r} seconds")
        if isinstance(landed, bool) or not isinstance(landed, int) or landed < 0:
            raise ValueError(f"a stage of {landed!r} landed bytes")
    except (ValueError, TypeError, WorkflowError) as exc:
        detail = f"{dst.locator} gave no usable adoption report (the stage of a segment: {exc}){aborted}"
        raise _unknown_adoption(detail, src, dst, arguments) from None
    document = {"token": token, "workspace_id": workspace_id, "matures_in": float(matures), "bytes": landed}
    return document, str(response.get("stderr", ""))


def _adopt_staged(
    session: TransferSession,
    settled: "_Deliveries",
    holds: list[_moving.Hold],
    staged: dict[str, Any],
    err: str,
    *,
    release: bool,
) -> None:
    """Adopt the segment of *holds* DST staged (*staged*: its token and workspace id; *err* its ``stage``'s standard
    error) with one ``adopt_staged`` request, and settle it: the answer must be the adoption of exactly those
    bundles in the pinned workspace, else the outcome is unknown (the holds stay held, the pair stops). A refusal
    (DST answered without adopting anything: an unknown token, a workspace sealed since) keeps the holds held and
    stops the pair too. The adopted holds are released at once when *release*; a source session that failed in that
    release stops the pair before anything more is adopted.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    ids = [hold.transfer_id for hold in holds]
    header = {"workspace": dst.plain, "token": staged["token"], "transfer_ids": ids}
    try:
        try:
            response = session.request("adopt_staged", header)
        except SessionFailed as exc:
            detail = f"the transfer session to {dst.locator} failed while it adopted a staged segment: {exc}"
            raise _unknown_adoption(detail, src, dst, arguments) from None
        if response.get("ok") is False and "document" not in response:
            # Refused before anything was touched (the far side's rule): the stage stays there, unadopted.
            error = str(response.get("error"))
            refusal = f"{dst.locator} refused to adopt a staged segment: {error}"
            said = "" if "nothing was adopted" in error else "; nothing of it was adopted"
            settled.fail(
                holds,
                f"{refusal}{said}: deliver it with `httk job transfer --resume {src.locator} {dst.locator}`",
            )
            settled.stopped = settled.stopped or refusal
            return
        try:
            if response.get("ok") is not True:
                raise ValueError(str(response.get("error", "not adopted")))
            document = _checked_adoption(response.get("document"), ids, exact=True)
            if "refused" in document:
                raise ValueError("a refusal after the stage")
        except (ValueError, TypeError, WorkflowError) as exc:
            detail = f"{dst.locator} gave no usable adoption report of a staged segment ({exc})"
            raise _unknown_adoption(detail, src, dst, arguments) from None
        if (answered := document["workspace_id"]) != staged["workspace_id"]:
            # The jobs arrived, but in another workspace than the one the stage was made in: nothing is released.
            raise RuntimeError(
                f"{dst.locator} adopted the jobs into workspace {answered}, not the expected workspace "
                f"{staged['workspace_id']} that answered their stage; the holds are kept on {src.locator}: check with "
                f"`httk job show` on {dst.locator}, then `httk job transfer --resume {src.locator} {dst.locator}` "
                "reports them already adopted there and releases the holds"
            )
    except RuntimeError as exc:
        settled.fail(holds, str(exc))
        settled.stopped = settled.stopped or str(exc).split(";", 1)[0]
        return
    done = settled.settle(holds, document)
    _relay(dst, arguments, err + str(response.get("stderr", "")))
    if release:
        settled.release(done)
        if settled.session is not None and not settled.session.is_open:
            # SRC failed while it released: nothing more is adopted (the queued stages are abandoned, held on SRC).
            settled.stopped = settled.stopped or f"{src.locator}: the transfer session failed while releasing"


def _abandon_staged(
    settled: "_Deliveries", holds: list[_moving.Hold], where: str, reason: str, *, copied: bool = True
) -> None:
    """Settle the holds of a segment staged in *where* and never adopted, since the transfer stopped (*reason*): they
    stay held (they were sent, so they are never taken back), for ``--resume``. Without *copied* the stage was the
    original holds in SRC (one filesystem, no ``--keep``): nothing of them was copied."""

    src, dst = settled.src, settled.dst
    staged = f"it was staged in {where} and discarded there" if copied else "nothing of it was copied"
    message = (
        f"not adopted, since the transfer stopped ({reason}); {staged}, and it is delivered again by "
        f"`httk job transfer --resume {src.locator} {dst.locator}`"
    )
    settled.fail(holds, message)


def _session_workflows(
    session: TransferSession,
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
    source: TransferSession | None = None,
) -> dict[str, dict[str, Any]]:
    """Before a push's first segment: ask *dst* (one ``workflows`` request) which of the workflows the holds need it
    has, and install the missing ones and build the unbuilt ones there (one ``install`` request), so that every
    segment is runnable as soon as it is adopted. The result is :func:`_ship_workflows`'s; a failure is reported
    in it and never stops the push. A remote *src*'s store entries travel through a relay (see
    :func:`_install_remote`).

    :param session: DST's open transfer session.
    :param src: The source end.
    :param dst: The remote destination end.
    :param holds: Every hold the push delivers.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :param source: A remote *src*'s transfer session, if any.
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
    return _ship_workflows(src, dst, holds, context, arguments, have, session, source)


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
    :param pair: The pair whose page the batch is: what lives for the whole command (the pinned ``workspace_id``,
        the destination's id, the sticky fsync, the stop and the staged segments) is the pair's; by default the
        batch's own.
    """

    def __init__(
        self,
        src: _End,
        dst: _End,
        outcomes: dict[str, dict[str, Any]],
        fresh: frozenset[str],
        context: CLIContext,
        arguments: argparse.Namespace,
        pair: "_Pair | None" = None,
    ) -> None:
        self.src, self.dst, self.outcomes, self.fresh = src, dst, outcomes, fresh
        self.context, self.arguments = context, arguments
        #: The pair: the pinned ``workspace_id`` (``answered``), ``destination_id``, ``fsync``, ``stopped`` and the
        #: staged segments not yet adopted.
        self.pair = _Pair(dst) if pair is None else pair
        #: The batch's outcomes are concluded once its deliveries ended and its last staged segment was settled.
        self.page = _Concluding()
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
        #: The refusal of a whole segment that stopped the session: the segments never sent are settled by it.
        self.refusal: dict[str, Any] | None = None
        #: A pull's transfer session to the remote SRC, which then releases over it.
        self.session: TransferSession | None = None
        #: *fresh* names this run's holds of a remote SRC too (remote → remote over a session), so that a refused hold
        #: an earlier run left gets the conditional remedy.
        self.fresh_known = False

    @property
    def answered(self) -> str | None:
        """The ``workspace_id`` of the first answer: every later delivery of the command must report it."""

        return self.pair.answered

    @answered.setter
    def answered(self, value: str | None) -> None:
        self.pair.answered = value

    @property
    def destination_id(self) -> str | None:
        """The adopting workspace's id, for the releases' away copies."""

        return self.pair.destination_id

    @destination_id.setter
    def destination_id(self, value: str | None) -> None:
        self.pair.destination_id = value

    @property
    def fsync(self) -> bool:
        """A sync commit failed in this session: later segments go with ``--sync fsync`` directly."""

        return self.pair.fsync

    @fsync.setter
    def fsync(self, value: bool) -> None:
        self.pair.fsync = value

    @property
    def stopped(self) -> str | None:
        """Why the session stopped: an unknown outcome, after which nothing more is sent."""

        return self.pair.stopped

    @stopped.setter
    def stopped(self, value: str | None) -> None:
        self.pair.stopped = value

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
        SRC; a hold that predates the run, or one of a remote SRC, stays held, reported ``failed`` (one that predates
        the run with the conditional remedy, when that is known of a remote SRC's).
        """

        src, dst = self.src, self.dst
        ids = [hold.transfer_id for hold in holds]
        self.answered = self.answered or document.get("workspace_id")
        if "refused" in document:
            # Definitively nothing of this delivery was adopted.
            reason = f"{dst.locator}: {document['refused']}"
            if src.workspace is None:
                message = f"refused before anything was adopted: {reason}; it stays held on {src.locator}"
                message += f" (`httk job transfer --resume {src.locator} {dst.locator}` delivers it again)"
                for hold in holds:
                    earlier = self.fresh_known and hold.transfer_id not in self.fresh
                    self.outcomes[hold.transfer_id].update(
                        status="failed", message=self.earlier(hold, reason, document) if earlier else message
                    )
                return []
            if back := [hold for hold in holds if hold.transfer_id in self.fresh]:
                for transfer_id, update in _take_back(src, back, reason).items():
                    self.outcomes[transfer_id].update(update)
            for hold in holds:
                if hold.transfer_id not in self.fresh:
                    message = self.earlier(hold, reason, document)
                    self.outcomes[hold.transfer_id].update(status="failed", message=message)
            return []
        self.entries.update(document["entries"])
        self.destination_id = self.destination_id or document["workspace_id"]
        self.reports.append(document.get("workflows"))
        self.shipped_for += holds
        return [key for key in ids if document["entries"][key]["status"] in _DELIVERED]

    def earlier(self, hold: _moving.Hold, reason: str, document: dict[str, Any]) -> str:
        """The outcome message of a refused *hold* that predates the run: it stays held, with the conditional remedy
        (an earlier run's delivery of it may have been adopted, its answer lost)."""

        src, dst = self.src, self.dst
        remedy = _release_remedy(
            self.arguments, hold.transfer_id, self.destination_id or document.get("workspace_id"), src, dst
        )
        # A remote SRC's hold is taken back by hand on its own host, where its path is.
        where = "" if src.workspace is not None else f" on the host of {src.locator}"
        workspace = src.locator if src.workspace is not None else src.plain
        return (
            f"refused before anything was adopted: {reason}; it stays held on {src.locator}, because "
            f"an earlier delivery may already have adopted it: check with `httk job show` on "
            f"{dst.locator}, then deliver again with `httk job transfer --resume {src.locator} "
            f"{dst.locator}` or, if the jobs are there, release with {remedy}, or, if they are not "
            f"there, take it back{where} with `httk job adopt --workspace {workspace} {hold.path}`"
        )

    def release(self, transfer_ids: list[str]) -> None:
        """Release the delivered *transfer_ids* on SRC (step 4), recording which were released."""

        self.handled.update(transfer_ids)
        if self.pair.interrupted and self.session is not None and not self.session.is_open:
            # Interrupted, and SRC's session with it: the jobs are adopted, their holds stay for --resume.
            self.unreleased += [(transfer_id, "the transfer was interrupted") for transfer_id in transfer_ids]
            return
        # A session's request carries the ids in its header, not on a command line: a bounded number a request.
        chunks = (
            _chunks(transfer_ids, flag="--release")
            if self.session is None
            else [transfer_ids[start : start + _RELEASE_IDS] for start in range(0, len(transfer_ids), _RELEASE_IDS)]
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


#: The ops a destination's transfer session serves when it stages segments (plan §7.10): a push then stages each
#: segment and adopts it once its deadline has run, while later segments are copied; without them, one ``adopt``.
_STAGE_OPS = frozenset({"stage", "adopt_staged"})


def _deliver_segment(
    session: TransferSession,
    settled: "_Deliveries",
    holds: list[_moving.Hold],
    base: Path | None,
    size: int | None,
    token: str,
    fsync: bool,
) -> tuple[dict[str, Any], str]:
    """One delivery of a segment for :func:`_adopt_with_retry` (:func:`_session_adopt`, or with *size*, its landed
    bytes, :func:`_session_stage`; the bundles uploaded from *base*, a remote SRC's relay, when given), recording in
    *settled* that the session now goes with ``--sync fsync``."""

    settled.fsync = settled.fsync or fsync  # a retry after a failed commit: the rest go so too
    if base is not None:
        holds = [replace(hold, path=str(base / hold.transfer_id)) for hold in holds]
    if size is not None:
        return _session_stage(session, settled.src, settled.dst, settled.arguments, holds, size, fsync)
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

    When DST serves ``stage`` and ``adopt_staged`` (decided when the session opened; plan §7.10) a segment is staged
    instead (``stage``: received, committed and the first scan of its settle taken, answered at once) and adopted
    with ``adopt_staged`` once its visibility deadline has run, between later stages and, for the rest, when the
    pair ends (at most :data:`_STAGES` staged segments and :data:`_STAGED_BYTES` bytes wait; DST refusing one at its
    capacity gets the oldest adopted first). A staged segment's outcome is final only after its adoption and release
    (its refusals are the ``stage`` request's, ruled as above); a stop abandons the staged segments not yet adopted,
    their holds kept for ``--resume`` (they were sent: never taken back), and a lost ``adopt_staged`` answer is an
    unknown outcome of its segment.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    sizes: dict[str, int] = {}
    segments = [segment for delivery in deliveries for segment in _segments(delivery, sizes=sizes)]
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
        _push_segment(session, settled, segment, sizes=sizes)


def _push_segment(
    session: TransferSession,
    settled: _Deliveries,
    holds: list[_moving.Hold],
    base: Path | None = None,
    *,
    release: bool = True,
    sizes: dict[str, int] | None = None,
) -> None:
    """Deliver one segment over DST's transfer *session* by the rules of :func:`_push_over_session`: adopted (with
    the retry of a failed commit, from the same bundles), its offending bundle refused and the rest sent once more,
    or refused or unknown (*settled* says whether that stopped the session); its adopted holds released at once
    (unless not *release*: then the batch's one release after its last segment does). A staged segment is queued on
    the pair for its ``adopt_staged`` (after the oldest were adopted to make room), and those whose deadline has run
    are adopted after it.

    :param session: DST's open transfer session.
    :param settled: The batch's deliveries, updated in place.
    :param holds: The segment's holds.
    :param base: The local directory holding the bundles as ``<transfer_id>/`` (a remote SRC's relay); by default
        each hold's own path.
    :param release: Release the adopted holds right after the segment (its adoption, when staged).
    :param sizes: The holds' landed bytes by transfer id, measured by :func:`_segments`; a hold not in it (a relay's)
        is measured here.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    pair = settled.pair
    # Decided by the session's ops, at its opening: never after a segment was staged.
    staging = _STAGE_OPS <= session.ops
    resent = False
    while holds:
        size: int | None = None
        if staging:
            size = sum(
                sizes[hold.transfer_id]
                if sizes is not None and hold.transfer_id in sizes
                else _measure(Path(hold.path) if base is None else base / hold.transfer_id)[0]
                for hold in holds
            )
            _make_room(pair, size)
            if settled.stopped is not None:
                # An adoption of an earlier segment stopped the pair: this one is never sent.
                settled.fail(
                    holds,
                    f"not delivered, since the transfer session stopped ({settled.stopped}); deliver it with "
                    f"`httk job transfer --resume {src.locator} {dst.locator}`",
                )
                break
        deliver = partial(_deliver_segment, session, settled, holds, base, size)
        try:
            document, err = _adopt_with_retry(
                deliver, src, dst, arguments, len(holds), _fs.fresh_token(), settled.answered, fsync=settled.fsync
            )
        except _Capacity as exc:
            if not pair.pending:
                # A DST with a smaller cap than this side's segments: the transfer cannot stage there.
                remote = dst.binding.remote if dst.binding is not None else dst.locator
                settled.fail(
                    holds,
                    f"{dst.locator} refused to stage it at its capacity, with nothing staged: {exc}; it stays held: "
                    f"deliver it without staging, with `httk remote configure --set transfer_session=off {remote}` "
                    f"for this run, then `httk job transfer --resume {src.locator} {dst.locator}`",
                )
                settled.stopped = f"{dst.locator}: a stage refused at its capacity with nothing staged"
                break
            _adopt_oldest(pair)  # DST counts otherwise: one adopted, the segment goes once more
            continue
        except _ERRORS as exc:
            settled.fail(holds, str(exc))
            settled.stopped = str(exc).split(";", 1)[0]
            break
        if "token" in document:
            # Staged: adopted once its deadline has run, while later segments are copied.
            settled.answered = settled.answered or document["workspace_id"]
            mature_at = time.monotonic() + document["matures_in"]
            where = f"{dst.locator}"
            stage = _PendingStage(
                holds,
                settled,
                document["bytes"],  # what landed there, as DST counts it
                mature_at,
                partial(_adopt_staged, session, settled, holds, document, err, release=release),
                partial(_abandon_staged, settled, holds, where),
            )
            _stage_pending(pair, stage)
            _adopt_matured(pair)
            break
        refused = "refused" in document
        if refused and base is not None and document.get("stream_aborted"):
            # The upload of this side's own relay failed (it could not be read): a local failure naming no hold.
            settled.settle(holds, document)
            settled.stopped = f"this machine's relay could not be read while it was sent to {dst.locator}"
            break
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
            if release:
                settled.release(done)
        elif not any(document.get(key) for key in ("bundle", "sync_failed", "stream_aborted")):
            # Refused as a whole, by the far side's statement (an unknown or sealed workspace, a quota), whatever
            # this side's upload did: every later segment would be too.
            settled.stopped, settled.refusal = f"{dst.locator}: {document['refused']}", document
        break


def _receive_part(
    session: TransferSession,
    settled: _Deliveries,
    holds: list[_moving.Hold],
    landing: Path,
    fsync: bool,
    *,
    relay: bool = False,
) -> tuple[str, Any]:
    """One ``send`` request of a pull's segment (or of what is left of it) on *session*, received into *landing* in
    the local DST (or, with *relay*, in a relay on the way to a remote DST): what happened, and what it is about.

    The download is extracted into *landing* (:func:`httk.workflow._landing.receive_tar`, the trust boundary), its
    terminal validated, and the landing made durable (a sync barrier entered before its first byte; with *fsync*
    every file synchronized as it is written instead) before the far side's answer is read. A relay is extracted by
    the same rules and its terminal validated, but nothing is synchronized (:func:`_extract_relay`). SRC may carry
    only a prefix of *holds* (it closes a request at :data:`_SEGMENT_BYTES` or :data:`_SEGMENT_MEMBERS`); its answer
    must name exactly what arrived.

    :param session: SRC's transfer session.
    :param settled: The batch's deliveries.
    :param holds: The holds to fetch, in order.
    :param landing: The landing directory, which must not exist yet.
    :param fsync: Synchronize every file as it is written.
    :param relay: *landing* is a relay (see above).
    :return: ``("received", sent)`` (the transfer ids that arrived, a prefix of *holds*, in *landing*);
        ``("sync", reason)`` (the landing's ``syncfs`` commit failed under ``io.sync_errors`` ``fail``);
        ``("refused", (ids, reason, gone))`` (SRC could not send those holds, *gone* the ones it no longer has, or
        this side refused one's member); ``("failed", reason)`` (this side refused the stream naming no hold);
        ``("systemic", reason)`` (SRC refused the request naming no hold); ``("unknown", reason)`` (the session
        failed, or SRC answered something else).
        Except for ``received``, nothing of *landing* is left to use.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    workspace = None if relay else dst.workspace
    assert relay or workspace is not None
    where = "this machine's relay" if relay else dst.locator
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
        if workspace is None:
            received[:] = _extract_relay(landing, stream)
            return
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
            return "refused", ([cause.bundle], f"refused in {where}: {cause}", [])
        return "failed", f"the copy from {src.locator} was refused in {where}: {cause}"
    sent = response.get("sent")
    if not isinstance(sent, list) or not sent or sent != ids[: len(sent)] or sorted(sent) != received:
        session.close()  # a far side that answers something else is not asked anything more
        return "unknown", (
            f"{src.locator} answered a send of {len(ids)} bundle(s) with {sent!r}, not the prefix of them that arrived"
        )
    return "received", sent


def _pull_part(
    source: "_SourceSession", settled: _Deliveries, holds: list[_moving.Hold], fsync: bool
) -> tuple[str, Any]:
    """Fetch a prefix of *holds* from a remote SRC over a pull's transfer session (*source*) into the landing of a
    new stage in the local DST (plan §7.10, 1a): its own owner and ``landing`` scratch, the landing made durable, and
    only once SRC's answer is validated too (the exact prefix it sent), the first scan of its settle; the stage is
    then queued on the pair (inside the same guard, so an interrupt always reaches it). Anything else discards the
    landing and closes the stage's owner.

    :param source: The pull's transfer session to SRC.
    :param settled: The batch's deliveries.
    :param holds: The holds to fetch, in order.
    :param fsync: Synchronize every landed file as it is written.
    :return: ``("staged", sent)``: the transfer ids that arrived, staged and queued; or what :func:`_receive_part`
        says went wrong (nothing staged); ``("failed", reason)`` also when no landing could be made before the request
        (the owner or its scratch), a local refusal naming no hold.
    :raises WorkflowError: Or another error of the CLI's kinds, when the first scan failed (nothing adopted).
    """

    session = source.get()
    assert session is not None

    dst = settled.dst
    assert dst.workspace is not None
    workspace = dst.workspace
    unusable = f"no landing could be made in {dst.locator}"
    stage: _moving.Stage | None = None
    try:
        require_cli_modifiable(workspace)
        stage = _moving.Stage(workspace, _owner(workspace, "job transfer"))
        stage.landing = stage.owner.scratch("landing")
    except BaseException as exc:
        if stage is not None:
            with contextlib.suppress(Exception):  # an owner that cannot close is left for recovery
                stage.abandon()
        if not isinstance(exc, _ERRORS):
            raise  # an interrupt: nothing was asked of SRC
        return "failed", f"{unusable}: {exc}"  # before the request: nothing was asked of SRC
    landing = stage.landing / "outgoing"
    queued = False
    try:
        outcome = _receive_part(session, settled, holds, landing, fsync)
        if outcome[0] == "received":
            sent: list[str] = outcome[1]
            # Only after the download's terminal and SRC's answer (the exact prefix sent) were validated.
            stage.commit([landing / transfer_id for transfer_id in sent])
            part = holds[: len(sent)]
            pending = _PendingStage(
                part,
                settled,
                # The landed regular-file bytes (hard-link copies counted), against the pair's staged bytes.
                sum(_measure(landing / transfer_id)[0] for transfer_id in sent),
                time.monotonic() + stage.remaining(),
                partial(_adopt_pulled, source, settled, stage, part),
                partial(_abandon_local_stage, settled, stage, part, settled.dst.locator),
            )
            _stage_pending(settled.pair, pending)
            queued = True
            return "staged", sent
    except BaseException:
        if not queued:
            with contextlib.suppress(Exception):
                stage.abandon()
        raise
    stage.abandon()
    return outcome


def _adopt_pulled(
    source: "_SourceSession", settled: _Deliveries, stage: _moving.Stage, holds: list[_moving.Hold]
) -> None:
    """Adopt a pull's stage in the local DST once its deadline has run (:func:`_adopt_local_stage`) and release its
    adopted holds on SRC over the pull's transfer session; a session that failed while releasing stops the pull."""

    src = settled.src
    session = source.get()
    assert session is not None
    if not _adopt_local_stage(settled, stage, holds, stop=False):
        return
    ids = [hold.transfer_id for hold in holds]
    settled.release([key for key in ids if settled.entries[key]["status"] in _DELIVERED])
    if not session.is_open:
        settled.stopped = settled.stopped or f"{src.locator}: the transfer session failed while releasing"


def _pull_over_session(source: _SourceSession, holds: list[_moving.Hold], settled: _Deliveries) -> None:
    """Pull the batch *holds* from a remote SRC into a local DST over the pull's transfer session, segment by segment.

    Segments close at :data:`_SEGMENT_BUNDLES`; SRC closes each ``send`` request at :data:`_SEGMENT_BYTES` of landed
    bytes or :data:`_SEGMENT_MEMBERS` archive members (it measures the bundles), so a segment may take several
    requests, each a prefix of what is left of it. Each request has its own landing, staged in the local DST
    (:func:`_pull_part`, plan §7.10: durable, SRC's answer validated, the first scan of its settle taken) and adopted
    once the visibility deadline has run from that scan, between later requests (at most :data:`_STAGES` stages and
    :data:`_STAGED_BYTES` landed bytes wait, the incoming request counted at :data:`_SEGMENT_BYTES` (it may carry one
    bundle more, which then exceeds the bytes until it is adopted); the pair adopts the
    rest at its end, across pages); the holds a stage adopted (or found adopted already) are released on SRC right
    after its adoption, over the session. A stop abandons the stages not yet adopted (their landings discarded,
    their holds kept on SRC). Per segment:

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

    A failed adoption fails the holds of its stage (held on SRC; some may be adopted, which ``--resume`` reports as
    already adopted).

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
            # One fetch reserved: SRC closes it at _SEGMENT_BYTES, or one bundle beyond (never split; documented).
            _make_room(settled.pair, _SEGMENT_BYTES)
            if settled.stopped is not None:
                settled.fail(pending, f"not delivered, since {settled.stopped}; deliver it with {resume}")
                break
            try:
                kind, detail = _pull_part(source, settled, pending, source.fsync)
            except _ERRORS as exc:
                settled.fail(pending, str(exc))
                break
            if kind == "staged":
                pending = pending[len(detail) :]
                _adopt_matured(settled.pair)
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
    (another actor released or took them), which the message says instead of their staying held there. A hold an
    earlier run left, when that is known (remote → remote), gets the conditional remedy: an earlier delivery of it
    may have been adopted."""

    src, dst = settled.src, settled.dst
    for hold in holds:
        if hold.transfer_id in gone:
            message = (
                f"{reason}; it is gone from {src.locator} (released or taken by another actor): check with "
                f"`httk job show` on {src.locator} and on {dst.locator} where its jobs are"
            )
            settled.fail([hold], message, complete=True)
        elif settled.fresh_known and hold.transfer_id not in settled.fresh:
            settled.fail([hold], settled.earlier(hold, reason, {}), complete=True)
        else:
            settled.fail([hold], reason)


def _pull_relay(settled: _Deliveries, holds: list[_moving.Hold], landing: Path) -> tuple[str, Any]:
    """Fetch *holds* of a remote SRC that serves no transfer session into the relay directory *landing* with the
    adapter's ``pull`` (a call per 64 KiB of ids).

    :param settled: The batch's deliveries.
    :param holds: The holds, in order.
    :param landing: The relay directory.
    :return: ``("received", ids)``, every one; or ``("failed", reason)``, naming no hold.
    """

    src, context, arguments = settled.src, settled.context, settled.arguments
    ids = [hold.transfer_id for hold in holds]
    outgoing = str(PurePosixPath(holds[0].path).parent)
    try:
        for chunk in _chunks(ids):  # a pull names its files on the far side's command line
            _adapter(src, context, arguments, "pull", outgoing, str(landing), chunk)
    except _ERRORS as exc:
        return "failed", f"copying from {src.locator} into this machine's relay failed: {exc}"
    return "received", ids


def _make_relay() -> "tempfile.TemporaryDirectory[str] | str":
    """A new relay, a temporary directory of this machine (the default temporary location: ``TMPDIR``), or why it
    could not be made (a local refusal naming no hold)."""

    try:
        return tempfile.TemporaryDirectory(prefix="httk-transfer-")
    except OSError as exc:
        return f"this machine's relay could not be made: {exc}"


def _remove_relay(relay: "tempfile.TemporaryDirectory[str]") -> str | None:
    """Remove *relay*: ``None``, or why it could not be removed (a local failure, which stops the pair)."""

    try:
        relay.cleanup()
    except OSError as exc:
        return f"this machine's relay could not be removed: {exc}"
    return None


def _measure_relayed(base: Path, hold: _moving.Hold) -> tuple[int, int]:
    """The regular-file bytes and tar members of *hold*'s copy in the relay *base* (:func:`_measure`)."""

    return _measure(base / hold.transfer_id)


def _relay_over_session(
    session: TransferSession, holds: list[_moving.Hold], settled: _Deliveries, source: TransferSession | None
) -> None:
    """Deliver the batch *holds* of a remote SRC to a remote DST over DST's transfer *session*, segment by segment,
    each through a local temporary relay (plan §7.9).

    Segments close at :data:`_SEGMENT_BUNDLES`. Each part of a segment is fetched into its own relay, a temporary
    directory of this machine (``TMPDIR``) removed once the part is delivered (staged, when DST stages: the relay is
    neither a stage nor a receipt; plan §7.10): over SRC's transfer session *source*
    with one ``send`` request (SRC closes it at :data:`_SEGMENT_BYTES` or :data:`_SEGMENT_MEMBERS`, so a segment may
    take several), extracted by the untrusted-tar rules and its terminal validated before the relay is used; without
    one, with the adapter's ``pull``. The part is then delivered in the segments of a push (:func:`_segments`, by the
    bytes measured in the relay, so that no staged segment exceeds what DST admits) by the rules of a push
    (:func:`_push_segment`: a
    failed commit's retry goes from the same relay, an offending bundle fails and the rest go once more from it) and
    its adopted holds are released on SRC once adopted, over *source* (without it, by the batch's one adapter
    release after the last segment, as before sessions). A remote SRC never takes a hold back: every refusal leaves
    its holds held
    there. Per segment, as in a pull:

    - holds SRC cannot send (gone, or changed while read), or whose member the relay refuses, fail and the rest is
      fetched once more without them; after a second such refusal the rest of the segment is not fetched;
    - a refusal naming no hold (by SRC, or a local one: a relay that cannot be made, written, read or removed, a full
      disk), a failed SRC session or an unusable answer stops the transfer to DST, that part and every later hold
      held on SRC (what earlier parts settled stays settled);
    - so do an unknown outcome at DST, a refusal of a whole segment and another ``workspace_id``
      (:func:`_push_segment`).

    :param session: DST's open transfer session.
    :param holds: The batch, in order.
    :param settled: The batch's deliveries, updated in place.
    :param source: SRC's open transfer session, or ``None`` when SRC takes the adapter operations.
    """

    src, dst, arguments = settled.src, settled.dst, settled.arguments
    resume = f"`httk job transfer --resume {src.locator} {dst.locator}`"
    segments = _segments(holds, measure=lambda _hold: (0, 0))
    for number, segment in enumerate(segments, 1):
        which = f" (segment {number} of {len(segments)})" if len(segments) > 1 else ""
        pending: list[_moving.Hold] = segment
        resent = False
        if settled.stopped is None:
            _say(arguments, f"copying {len(segment)} bundle(s) from {src.locator}{which}")
        while pending and settled.stopped is None:
            relay = _make_relay()
            detail: Any
            if isinstance(relay, str):
                kind, detail = "failed", relay
            else:
                try:
                    landing = Path(relay.name) / "outgoing"
                    if source is not None:
                        kind, detail = _receive_part(source, settled, pending, landing, False, relay=True)
                    else:
                        kind, detail = _pull_relay(settled, pending, landing)
                    if kind == "received":
                        part, pending = pending[: len(detail)], pending[len(detail) :]
                        _say(arguments, f"delivering {len(part)} bundle(s) to {dst.locator}{which}")
                        # A relayed part may hold more than a push's segment (an adapter SRC's 256 bundles of any
                        # size, a send's last bundle beyond its limit): staged in the push's segments, each its own
                        # stage, adoption and release, measured once, in the relay.
                        sizes: dict[str, int] = {}
                        relayed = partial(_measure_relayed, landing)
                        for piece in _segments(part, measure=relayed, sizes=sizes):
                            if settled.stopped is not None:
                                settled.fail(
                                    piece,
                                    f"not delivered, since the transfer stopped ({settled.stopped}); deliver it "
                                    f"with {resume}",
                                )
                                continue
                            # Without SRC's session, its holds are released by the batch's one adapter release at the
                            # end.
                            _push_segment(session, settled, piece, landing, release=source is not None, sizes=sizes)
                finally:
                    removed = _remove_relay(relay)
                if removed is not None and kind != "failed":
                    settled.stopped = settled.stopped or removed  # what this part settled stays settled
            if kind == "received":
                if source is not None and not source.is_open:
                    settled.stopped = settled.stopped or f"{src.locator}: the transfer session failed"
            elif kind == "refused":
                offenders, reason, gone = detail
                _fail_offenders(settled, [hold for hold in pending if hold.transfer_id in offenders], reason, gone)
                pending = [hold for hold in pending if hold.transfer_id not in offenders]
                if resent and pending:
                    message = (
                        f"not fetched: another hold of its segment was refused twice ({offenders[0]}: {reason}); "
                        f"it stays held on {src.locator}: --resume retries it"
                    )
                    settled.fail(pending, message, complete=True)
                    pending = []
                resent = True
            else:
                # SRC refused naming no hold, the session failed, SRC answered something else, or the relay could not
                # be made or written (a full disk): nothing of this part reached DST.
                settled.stopped = str(detail)
                settled.fail(pending, f"{detail}; nothing of it was adopted in {dst.locator}; deliver it with {resume}")
                pending = []
        if pending:
            settled.fail(
                pending, f"not delivered, since the transfer stopped ({settled.stopped}); deliver it with {resume}"
            )


def _deliver_relayed(
    source: TransferSession, settled: _Deliveries, holds: list[_moving.Hold], built_for: list[str]
) -> None:
    """Deliver the batch *holds* of a remote SRC to a remote DST that serves no transfer session (plan §7.9, (S, A)):
    fetched over SRC's transfer session *source* into one relay, a ``send`` request a part of at most
    :data:`_SEGMENT_BUNDLES` holds (SRC closes each at :data:`_SEGMENT_BYTES` or :data:`_SEGMENT_MEMBERS`), then
    what arrived delivered with one adapter ``deliver`` (:func:`_deliver_adoption`: a failed commit's retry goes from
    the same relay, nothing is fetched again), asking about *built_for*, and settled in *settled*. Holds SRC cannot
    send, or whose member the relay refuses, fail and the rest of their part is fetched once more without them (after
    a second such refusal the rest of the part is not fetched); a refusal naming no hold, a local failure of the
    relay, a failed SRC session or an unusable answer stops the transfer to DST with nothing delivered: every hold
    stays held on SRC. An unknown outcome of the delivery, or its refusal, stops the transfer to DST too.

    :param source: SRC's open transfer session.
    :param settled: The batch's deliveries, updated in place.
    :param holds: The batch, in order.
    :param built_for: The workflow ids DST reports with the adoption.
    """

    src, dst, context, arguments = settled.src, settled.dst, settled.context, settled.arguments
    resume = f"`httk job transfer --resume {src.locator} {dst.locator}`"

    def stop(reason: str) -> None:
        settled.stopped = settled.stopped or reason
        message = f"{reason}; nothing of it was delivered to {dst.locator}; deliver it with {resume}"
        settled.fail([hold for hold in holds if hold.transfer_id not in settled.entries], message)

    _say(arguments, f"copying {len(holds)} bundle(s) from {src.locator}")
    relay = _make_relay()
    if isinstance(relay, str):
        stop(relay)
        return
    document: dict[str, Any] | None = None
    arrived: list[_moving.Hold] = []
    try:
        base = Path(relay.name)
        outgoing = base / "outgoing"
        number = 0
        for window in _segments(holds, measure=lambda _hold: (0, 0)):
            pending, resent = window, False
            while pending:
                number += 1
                part = base / f"part-{number}"
                kind, detail = _receive_part(source, settled, pending, part, False, relay=True)
                if kind == "received":
                    try:
                        for transfer_id in detail:
                            _fs.move_owned(_fs.loc(part / transfer_id), _fs.loc(outgoing / transfer_id), durable=False)
                        _fs.discard(_fs.loc(part), trash_dir=base, durable=False)
                    except _ERRORS as exc:
                        stop(f"this machine's relay failed: {exc}")
                        return
                    arrived, pending = [*arrived, *pending[: len(detail)]], pending[len(detail) :]
                elif kind == "refused":
                    offenders, reason, gone = detail
                    _fail_offenders(settled, [hold for hold in pending if hold.transfer_id in offenders], reason, gone)
                    pending = [hold for hold in pending if hold.transfer_id not in offenders]
                    if resent and pending:
                        message = (
                            f"not fetched: another hold of its part was refused twice ({offenders[0]}: {reason}); "
                            f"it stays held on {src.locator}: --resume retries it"
                        )
                        settled.fail(pending, message, complete=True)
                        pending = []
                    resent = True
                else:
                    stop(str(detail))
                    return
        if not arrived:
            return
        ids = [hold.transfer_id for hold in arrived]
        _say(arguments, f"delivering {len(ids)} bundle(s) to {dst.locator}")
        try:
            document = _deliver_adoption(
                src, dst, context, arguments, str(outgoing), ids, _fs.fresh_token(), built_for, settled.answered
            )
        except _ERRORS as exc:
            settled.fail(arrived, str(exc))
            settled.stopped = settled.stopped or str(exc).split(";", 1)[0]
            return
    finally:
        removed = _remove_relay(relay)
        if removed is not None:
            settled.stopped = settled.stopped or removed
    settled.settle(arrived, document)
    if "refused" in document:
        settled.stopped = settled.stopped or f"{dst.locator}: {document['refused']}"


def _contained(settled: _Deliveries, holds: list[_moving.Hold], deliver: Callable[[], object]) -> None:
    """Run *deliver*, the remote → remote delivery of *holds*, containing an unexpected error to the pair: it stops,
    and the holds it had not settled are reported held on SRC unless adopted (what it settled stays settled).

    :param settled: The batch's deliveries.
    :param holds: The holds *deliver* delivers.
    :param deliver: The delivery.
    """

    try:
        deliver()
    except _ERRORS as exc:
        src, dst = settled.src, settled.dst
        settled.stopped = settled.stopped or f"{dst.locator}: the transfer failed while it was driven: {exc}"
        left = [
            hold
            for hold in holds
            if hold.transfer_id not in settled.entries and "status" not in settled.outcomes[hold.transfer_id]
        ]
        message = (
            f"the transfer to {dst.locator} failed while it was driven ({exc}); it stays held on {src.locator} unless "
            f"it was adopted: `httk job transfer --resume {src.locator} {dst.locator}` delivers or releases it"
        )
        settled.fail(left, message, complete=True)


class _Pair:
    """One (SRC, DST) pair of a command and what lives for the whole command, whatever the number of its pages
    (plan §7.8): how it is driven, its transfer session, the pinned DST ``workspace_id``, the sticky fsync, the stop
    and the workflow attempts.

    :param dst: The destination end.
    """

    def __init__(self, dst: _End) -> None:
        self.dst = dst
        #: ``"page"``: each page is driven before the next is held (a local DST, a push's or a pull's transfer
        #: session); ``"adapter"``: its holds are gathered and driven once, as before sessions; ``None`` until the
        #: pair's first use.
        self.mode: str | None = None
        #: A push's transfer session to DST.
        self.session: TransferSession | None = None
        #: The ``workspace_id`` of the first answer, which every later delivery must report.
        self.answered: str | None = None
        #: The adopting workspace's id, for the releases' away copies.
        self.destination_id: str | None = dst.workspace_id
        #: A push's sync commit failed: every later segment goes with ``--sync fsync``.
        self.fsync = False
        #: Why the pair stopped: it holds and drives nothing more.
        self.stopped: str | None = None
        #: Holds of this pair a stop left undriven.
        self.undriven = 0
        #: The workflows attempted for the pair (installed and built, or failed), by id.
        self.shipping: dict[str, dict[str, Any]] = {}
        #: An adapter pair's holds, and those of them this run created.
        self.held: list[_moving.Hold] = []
        self.fresh: set[str] = set()
        #: The segments staged in DST (by its transfer session, or in this machine's DST) and not yet adopted, in
        #: staging order, across pages (plan §7.10).
        self.pending: collections.deque[_PendingStage] = collections.deque()
        #: The command was interrupted: a release goes only over a session that is still usable.
        self.interrupted = False


class _Concluding:
    """When a batch's (a page's) outcomes are concluded: once its deliveries ended (:meth:`close`) and none of its
    staged segments waits for adoption (:meth:`settled`), possibly while a later page is driven."""

    def __init__(self) -> None:
        self._open = 0
        self._conclude: Callable[[], object] | None = None
        self._delivering = True

    def staged(self) -> None:
        """Count one more staged segment of the batch."""

        self._open += 1

    def settled(self) -> None:
        """Count one staged segment of the batch as adopted or abandoned (its outcomes settled)."""

        self._open -= 1
        self._maybe()

    def close(self, conclude: Callable[[], object]) -> None:
        """End the batch's deliveries; *conclude* runs once nothing of it is staged any more (perhaps at once).

        :param conclude: Concludes the batch's outcomes.
        """

        self._conclude, self._delivering = conclude, False
        self._maybe()

    def _maybe(self) -> None:
        if not self._delivering and self._open == 0 and (conclude := self._conclude) is not None:
            self._conclude = None
            conclude()


@dataclass
class _PendingStage:
    """A segment staged and not yet adopted (plan §7.10): its holds, its batch's deliveries, the bytes it holds and
    when it matures by this machine's clock (advisory: the receiver waits out its own deadline).

    :param holds: The segment's holds.
    :param settled: The deliveries of the batch it belongs to.
    :param size: Its landed regular-file bytes, as staged.
    :param mature_at: This machine's :func:`time.monotonic` time from which its adoption need not wait.
    :param adopt: Adopts it and settles its outcomes (an unknown outcome stops the pair); never raises the CLI's
        errors.
    :param abandon: Settles its holds as left held, since the pair stopped (given why).
    """

    holds: list[_moving.Hold]
    settled: "_Deliveries"
    size: int
    mature_at: float
    adopt: Callable[[], object]
    abandon: Callable[[str], object]


#: A pair keeps at most this many segments staged and not yet adopted (plan §7.10, 1a; a transfer session's far side
#: enforces the same), ...
_STAGES = 8
#: ... holding at most this many landed regular-file bytes, the incoming segment included; one segment larger than
#: this alone is staged only when nothing else is, and adopted before another is staged.
_STAGED_BYTES = 32 << 30


def _stage_pending(pair: _Pair, stage: _PendingStage) -> None:
    """Queue *stage*, the newest of *pair*, for its adoption."""

    pair.pending.append(stage)
    stage.settled.page.staged()


def _adopt_oldest(pair: _Pair) -> None:
    """Adopt *pair*'s oldest staged segment (waiting out what is left of its deadline); a stop that follows abandons
    the others."""

    stage = pair.pending.popleft()
    try:
        stage.adopt()
    except (KeyboardInterrupt, SystemExit):
        # An interrupt: its batch is not concluded (no release) while it unwinds; see _Transfer.interruptible.
        pair.interrupted = True
        raise
    except Exception as exc:
        # Anything else escaping the adoption or the release that follows it: the outcome of the holds not released
        # is unknown (the adoption may have begun), and the pair stops; the batch still concludes, with a status for
        # every hold.
        settled = stage.settled
        src, dst = settled.src, settled.dst
        settled.fail(
            [hold for hold in stage.holds if hold.transfer_id not in settled.handled],
            f"the transfer to {dst.locator} failed while its staged segment was adopted ({exc}); it stays held on "
            f"{src.locator} unless it was adopted: `httk job transfer --resume {src.locator} {dst.locator}` delivers "
            "or releases it",
            complete=True,
        )
        pair.stopped = pair.stopped or f"{dst.locator}: the transfer failed while a staged segment was adopted: {exc}"
    stage.settled.page.settled()
    if pair.stopped is not None:
        _abandon_pending(pair)


def _abandon_pending(pair: _Pair) -> None:
    """Abandon every segment *pair* staged and has not adopted, since it stopped: their holds stay held (they were
    sent, so they are never taken back)."""

    while pair.pending:
        stage = pair.pending.popleft()
        try:
            stage.abandon(pair.stopped or "the transfer stopped")
        finally:
            stage.settled.page.settled()


def _make_room(pair: _Pair, size: int) -> None:
    """Adopt *pair*'s oldest staged segments until one more, of *size* bytes, fits: fewer than :data:`_STAGES`
    staged and at most :data:`_STAGED_BYTES` bytes with it (or nothing staged)."""

    while (
        pair.pending
        and pair.stopped is None
        and (len(pair.pending) >= _STAGES or sum(stage.size for stage in pair.pending) + size > _STAGED_BYTES)
    ):
        _adopt_oldest(pair)
    if pair.stopped is not None:
        _abandon_pending(pair)


def _adopt_matured(pair: _Pair) -> None:
    """Adopt *pair*'s staged segments whose deadline has run, oldest first."""

    while pair.pending and pair.stopped is None and pair.pending[0].mature_at <= time.monotonic():
        _adopt_oldest(pair)


def _flush(pair: _Pair) -> None:
    """Adopt every segment *pair* staged, in staging order, or abandon them once it stopped."""

    while pair.pending and pair.stopped is None:
        _adopt_oldest(pair)
    _abandon_pending(pair)


def _unattempted(holds: list[_moving.Hold], attempted: Collection[str]) -> list[_moving.Hold]:
    """*holds* with only the workflows not in *attempted* (a pair's workflow attempts)."""

    return [
        hold
        if hold.workflows is None
        else replace(hold, workflows=tuple(entry for entry in hold.workflows if entry[0] not in attempted))
        for hold in holds
    ]


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
    pair: _Pair | None = None,
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

    From a local *src* to a remote *dst*, the deliveries go over the push's transfer session when there is one: the
    workflows first (:func:`_session_workflows`), then segment by segment (:func:`_push_over_session`); a *dst* that
    serves no session gets one adapter ``deliver`` a delivery instead. From a remote *src* to a local *dst*, the
    pull's transfer session to *src* (*source*) carries the workflows and then the holds, segment by segment
    (:func:`_pull_over_session`); without one, the adapter's ``pull`` copies them a chunk at a time and ``invoke``
    releases them. From a remote *src* to a remote *dst* (plan §7.9), every copy goes through a local temporary
    relay: with *dst*'s transfer session (the *pair*'s), the workflows first and then segment by segment
    (:func:`_relay_over_session`), each fetched over *src*'s transfer session (*source*) when it serves one, else
    with the adapter's ``pull``; without one, *src*'s session fetches the batch into one relay for one adapter
    ``deliver`` (:func:`_deliver_relayed`), or, serving none either, the adapter operations relay it as before
    sessions. A remote *src* never takes a hold back.

    With *pair*, the batch is one page of a paged transfer (plan §7.8), or an adapter pair's gathered holds: the
    pair's session, pinned ``workspace_id``, sticky fsync and workflow attempts carry over from its earlier pages
    (only the workflows not attempted yet are installed and built, a local *dst*'s before the page's first
    adoption), and a stop of this page (a session failure, an unknown outcome, a refusal naming no hold, another
    ``workspace_id``, and from a local *src* to a local *dst* a chunk that could not be delivered) stops the pair.

    Staged segments (plan §7.10: a push or relay to a *dst* whose session serves ``stage``, a pull's landings, a
    local *src*'s chunks for a local *dst*) are adopted once their visibility deadline has run, possibly while a
    later page is driven (:class:`_Pair`'s queue, at most :data:`_STAGES` staged segments a pair, a page a segment for
    a local *dst* of a local *src*): the
    returned outcomes are concluded (:func:`_conclude`) once every segment of the batch was adopted or abandoned. A
    stop abandons the pair's staged segments, their holds kept. Without *pair*, every one is adopted before this
    returns.

    :param src: The end holding the jobs.
    :param dst: The destination every hold of the batch records.
    :param holds: The batch.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :param fresh: The transfer ids of the holds this run created in a local *src* (in a remote one too, with a
        *pair* to a remote *dst*).
    :param sessions: Closes the command's transfer sessions at its end.
    :param source: The command's transfer session to a remote *src*, if any.
    :param pair: The pair whose page this is.
    :return: One outcome per hold, in the order of *holds* (concluded in place, later, when a stage waits).
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
    # A pull from a remote SRC over its transfer session (opened already, by the first request to SRC).
    pulling = source.get() if source is not None and src.workspace is None and dst.workspace is not None else None
    # Remote to remote: SRC's transfer session, which fetches into the relays and releases.
    relaying = source.get() if source is not None and src.workspace is None and dst.workspace is None else None
    # The workflows a pair attempted on an earlier page are not attempted again.
    unattempted = ready if pair is None else _unattempted(ready, pair.shipping)
    shipping: dict[str, dict[str, Any]] = {}
    # The session failed while the workflows were installed: none of this batch's counts as attempted.
    unshipped = False
    # The holds whose workflows are shipped: a local DST's all, a remote DST's those of an adopting delivery.
    shipped_for = ready
    if workflows and dst.workspace is not None:
        # Once a batch, before the first adoption: a job adopted after its workflow can start at once.
        shipping = _ship_workflows(src, dst, unattempted, context, arguments, session=pulling)
        unshipped = pulling is not None and not pulling.is_open
        if pair is not None and not unshipped:
            _build_shipped(dst, shipping, arguments)  # a page's adopted jobs are runnable at once
    entries: dict[str, dict[str, Any]] = {}
    destination_id = dst.workspace_id
    report: object = None
    by_id = {hold.transfer_id: hold for hold in ready}
    settled = _Deliveries(src, dst, outcomes, fresh, context, arguments, pair)
    settled.session = pulling or relaying
    settled.fresh_known = pair is not None and src.workspace is None and dst.workspace is None
    session: TransferSession | None = None
    # The holds whose workflows were installed before the first segment: reported whatever their segments do.
    stepped: list[_moving.Hold] | None = None
    if pulling is not None:
        assert source is not None
        _pull_over_session(source, ready, settled)
        entries = settled.entries
    elif dst.workspace is not None:
        if src.workspace is not None:
            entries = settled.entries  # filled as each chunk's stage is adopted
        for chunk in _chunks(list(by_id)):
            if settled.stopped is not None:
                message = f"not delivered, since the transfer stopped ({settled.stopped})"
                entries.update({transfer_id: {"status": "failed", "message": message} for transfer_id in chunk})
                continue
            if src.workspace is not None:
                _stage_local(settled, [by_id[transfer_id] for transfer_id in chunk])
                continue
            try:
                entries.update(_deliver(src, dst, [by_id[transfer_id] for transfer_id in chunk], context, arguments))
            except _ERRORS as exc:
                entries.update({transfer_id: {"status": "failed", "message": str(exc)} for transfer_id in chunk})
                if pair is not None and src.workspace is not None:
                    settled.stopped = f"{dst.locator}: {exc}"  # nothing more is held for a DST that cannot take it
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
        if pair is not None:
            session = pair.session  # opened (or found not served) before the pair's first hold
        elif src.workspace is not None:
            try:
                session = _push_session(dst, context, arguments, sessions)
            except _ERRORS as exc:
                # Nothing was sent: the holds stay, as after any failed delivery.
                for delivery in deliveries:
                    settled.fail(delivery, f"{dst.locator}: the transfer session did not open: {exc}")
                deliveries = []
        if session is not None and workflows and deliveries:
            # Before the first segment, so that each adopted segment can start at once.
            stepped = [hold for delivery in deliveries for hold in delivery]
            try:
                attempt = stepped if pair is None else _unattempted(stepped, pair.shipping)
                shipping = _session_workflows(session, src, dst, attempt, context, arguments, relaying)
                failure = None if session.is_open else "while installing the workflows"
                failed = dst.locator
                if failure is None and relaying is not None and not relaying.is_open:
                    failure, failed = "while its workflows were fetched", src.locator
            except SessionFailed as exc:
                failure, failed = str(exc), dst.locator
            if failure is not None:
                message = (
                    f"{failed}: the transfer session failed before anything was adopted ({failure}); deliver "
                    f"it with `httk job transfer --resume {src.locator} {dst.locator}`"
                )
                for delivery in deliveries:
                    settled.fail(delivery, message)
                deliveries = []
                settled.stopped = f"{failed}: the transfer session failed ({failure})"
                unshipped = True
        if session is not None and src.workspace is None:
            for delivery in deliveries:
                _contained(settled, delivery, partial(_relay_over_session, session, delivery, settled, relaying))
        elif session is not None:
            _push_over_session(session, deliveries, settled)
        for delivery in deliveries if session is None else ():
            built_for = sorted({entry[0] for hold in delivery for entry in hold.workflows or ()}) if workflows else []
            if relaying is not None:
                _contained(settled, delivery, partial(_deliver_relayed, relaying, settled, delivery, built_for))
                continue
            try:
                document = _deliver_remote(
                    src, dst, delivery, context, arguments, _fs.fresh_token(), built_for, expected=settled.answered
                )
            except _ERRORS as exc:
                settled.fail(delivery, str(exc))
                continue
            settled.settle(delivery, document)
    # A remote DST's adoption entries, its id and the holds whose workflows it reported are known once every segment
    # the batch staged was adopted.
    remote = dst.workspace is None and bool(ready)
    if pair is not None and not unshipped:
        # At once: a later page, driven before this one concludes, attempts none of these again.
        pair.shipping.update(shipping)
    if pair is None:
        _flush(settled.pair)  # no later page adopts what this batch staged
    elif pair.stopped is not None:
        _abandon_pending(pair)

    def conclude() -> None:
        _conclude(
            settled,
            by_id,
            paged=pair is not None,
            workflows=workflows,
            entries=settled.entries if remote else entries,
            destination_id=settled.destination_id if remote else destination_id,
            shipped_for=(settled.shipped_for if stepped is None else stepped) if remote else shipped_for,
            report=_joined_reports(settled.reports) if remote else report,
            session=session,
            shipping=shipping,
            unshipped=unshipped,
            relaying=relaying,
        )

    settled.page.close(conclude)  # at once, or once the last segment of the batch staged is adopted or abandoned
    return list(outcomes.values())


def _joined_reports(reports: list[object]) -> object:
    """The ``workflows`` reports of a batch's deliveries joined (each asked about its own holds' workflows), or
    ``None`` when none is a list."""

    if any(isinstance(rows, list) for rows in reports):
        return [row for rows in reports if isinstance(rows, list) for row in rows]
    return None


def _conclude(
    settled: _Deliveries,
    by_id: dict[str, _moving.Hold],
    *,
    paged: bool,
    workflows: bool,
    entries: dict[str, dict[str, Any]],
    destination_id: str | None,
    shipped_for: list[_moving.Hold],
    report: object,
    session: TransferSession | None,
    shipping: dict[str, dict[str, Any]],
    unshipped: bool,
    relaying: TransferSession | None,
) -> None:
    """Conclude the outcomes of a batch of :func:`_drive` once its deliveries ended and every segment it staged was
    adopted or abandoned: each hold's status from its adoption entry, the releases, the away copies kept, the
    workflows shipped and reported. The arguments are :func:`_drive`'s state of the batch (*paged*: it was given a
    pair).
    """

    src, dst, context, arguments, outcomes = (
        settled.src,
        settled.dst,
        settled.context,
        settled.arguments,
        settled.outcomes,
    )
    pair = settled.pair
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
        shipping = _ship_workflows(
            src, dst, shipped_for, context, arguments, _reported_workflows(report), source=relaying
        )
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
    if dst.workspace is not None and not paged:
        _build_shipped(dst, shipping, arguments)
    if paged:
        if not unshipped:
            pair.shipping.update(shipping)  # attempted, failures included: never attempted again for the pair
        shipping = {**pair.shipping, **shipping}
    for hold in shipped_for:
        # Reported only when every workflow of the hold was attempted (a failed session attempts none of the batch's).
        if shipping and hold.workflows is not None and all(entry[0] in shipping for entry in hold.workflows):
            outcomes[hold.transfer_id]["workflows"] = [
                {"id": workflow_id, **shipping[workflow_id]} for workflow_id, _, _ in hold.workflows
            ]


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
    changed: int | None = None,
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
    if changed:
        print(
            f"{changed} selected job(s) changed since they were listed: left out; run again to select them",
            file=sys.stderr,
        )
    for error in errors:
        print(error, file=sys.stderr)
    if json_output:
        document: dict[str, object] = {"transfers": outcomes, "errors": errors}
        if changed is not None:
            document["changed"] = changed  # a paged selection's
        print(json.dumps(document, indent=2, sort_keys=True))
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
    outcomes: list[dict[str, Any]] = []
    #: The listed jobs left out as changed since the listing, when the selection was paged.
    changed: int | None = None
    # Each remote end's transfer session, closed at the end: a remote DST's, a remote SRC's.
    with contextlib.ExitStack() as sessions:
        if selected:
            assert dst is not None
            if dst.workspace is not None:
                # Before anything is held: a destination that cannot take the jobs would only leave them held.
                require_cli_modifiable(dst.workspace)
            if src.workspace is None:
                for selector in arguments.jobs:
                    try:
                        canonical_uuid(selector, "JOB")
                    except (WorkflowError, ValueError) as exc:
                        raise ValueError(f"a remote source requires canonical job ids: {selector!r}") from exc
        if src.workspace is not None:
            selection = None
            if selected:
                assert dst is not None
                # Step 0: a bad selector stops the command here, before anything is held or driven.
                selection = _Selection(
                    src.workspace,
                    context.cwd,
                    arguments,
                    locator=dst.locator,
                    destination_id=dst.workspace_id,
                    workflows=not arguments.no_workflows,
                )
            run = _Transfer(src, dst, ends, context, arguments, sessions)
            with run.interruptible():
                changed = _transfer_local(run, selection)
            outcomes, errors = run.outcomes, run.errors
        else:
            # A remote SRC's session decides first (step 0 is its select); each DST's at its pair's first use.
            source = _SourceSession(src, context, arguments, sessions)
            try:
                session = source.get()
            except _ERRORS as exc:
                if not selected:
                    raise
                errors.append(f"{src.locator}: the transfer session did not open: {exc}; nothing was held")
            else:
                if session is not None:
                    run = _Transfer(src, dst, ends, context, arguments, sessions, source)
                    with run.interruptible():
                        changed = _transfer_pull(run, session, bool(selected))
                    outcomes, errors = run.outcomes, run.errors
                else:
                    _transfer_adapter(src, dst, ends, context, arguments, sessions, errors, outcomes)
    if arguments.state or arguments.step:
        under = f" under {arguments.placement}" if arguments.placement is not None else ""
        which = f"{' or '.join(arguments.state)} " if arguments.state else ""
        at = f" at step {' or '.join(arguments.step)}" if arguments.step else ""
        empty = f"no {which}job{at} to move in {src.locator}{under}"
        return _report(outcomes, errors, json_output=arguments.json, source=src.locator, empty=empty, changed=changed)
    return _report(outcomes, errors, json_output=arguments.json, source=src.locator, changed=changed)


def _holding(arguments: argparse.Namespace, src: _End) -> None:
    """Say what is being held on *src*."""

    what = [f"{len(arguments.jobs)} job(s)"] if arguments.jobs else []
    what += [f"every {state} job" for state in arguments.state]
    at = f" at step {' or '.join(arguments.step)}" if arguments.step else ""
    _say(arguments, f"holding {', '.join(what or ['every job'])}{at} on {src.locator}")


def _transfer_adapter(
    src: _End,
    dst: _End | None,
    ends: dict[str, _End],
    context: CLIContext,
    arguments: argparse.Namespace,
    sessions: contextlib.ExitStack,
    errors: list[str],
    outcomes: list[dict[str, Any]],
) -> None:
    """A transfer from a remote SRC on the adapter path, hold-everything-first as before sessions: the selection held
    with ``job transfer --hold`` there, then every hold bound for DST (with ``--resume`` and no DST: every hold)
    driven, each destination's as one batch.

    A remote DST that serves a transfer session (plan §7.9, (A, S)) gets it opened before anything is held for it
    (for ``--resume``, before its batch is driven; one that cannot open holds or drives nothing), and its batch is
    relayed segment by segment over it (:func:`_relay_over_session`, which fetches with the adapter's ``pull``).
    With a DST that serves none, nothing changes."""

    pairs: dict[str, _Pair | str | None] = {}

    def pair_of(end: _End) -> _Pair | str | None:
        """The pair of a remote *end* that serves a transfer session (decided at the first call), or why its session
        did not open; ``None`` for a local end or one that serves no session (nothing changes for those)."""

        if end.workspace is not None:
            return None
        if end.locator not in pairs:
            try:
                session = None if _session_disabled(end, context) else _push_session(end, context, arguments, sessions)
            except _ERRORS as exc:
                pairs[end.locator] = str(exc)
            else:
                pair = None
                if session is not None:
                    pair = _Pair(end)
                    pair.mode, pair.session = "page", session
                pairs[end.locator] = pair
        return pairs[end.locator]

    known: list[_moving.Hold] | None = None
    fresh: frozenset[str] = frozenset()
    if arguments.jobs or arguments.state or arguments.step:
        assert dst is not None
        if isinstance(failed := pair_of(dst), str):
            # Before anything is held: a destination that cannot take the jobs would only leave them held.
            errors.append(f"{dst.locator}: the transfer session did not open: {failed}; nothing was held")
            return
        _holding(arguments, src)
        held, before = _hold_jobs(src, dst, context, arguments, errors)
        fresh = frozenset(hold.transfer_id for hold in held)
        if before is not None:
            # SRC listed the other holds bound for DST with the hold: no transfer status call.
            known = sorted({hold.transfer_id: hold for hold in [*before, *held]}.values(), key=lambda h: h.transfer_id)
    batches: dict[str, list[_moving.Hold]] = {}
    if known is None:
        known = _holds(src, context, arguments, workflows=not arguments.no_workflows)
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
        pair = pair_of(ends[locator])
        if pair is None:
            outcomes.extend(_drive(src, ends[locator], holds, context, arguments, sessions=sessions))
            continue
        resume = f"`httk job transfer --resume {src.locator} {locator}`"
        if isinstance(pair, str):
            message = f"{locator}: the transfer session did not open: {pair}; it stays held on {src.locator}: {resume}"
            outcomes.extend(
                {
                    "transfer_id": hold.transfer_id,
                    "members": list(hold.members),
                    "destination": locator,
                    "status": "failed",
                    "message": message,
                }
                for hold in holds
            )
            continue
        try:
            outcomes.extend(_drive(src, pair.dst, holds, context, arguments, fresh=fresh, sessions=sessions, pair=pair))
            _flush(pair)  # its one batch: every segment it staged is adopted now
        except _ERRORS as exc:
            # Contained to this destination, as a paged transfer's pair is.
            pair.stopped = pair.stopped or str(exc)
            _abandon_pending(pair)
            message = (
                f"the transfer to {locator} failed while it was driven ({exc}); it stays held on {src.locator} "
                f"unless it was adopted: {resume} delivers or releases it"
            )
            outcomes.extend(
                {
                    "transfer_id": hold.transfer_id,
                    "members": list(hold.members),
                    "destination": locator,
                    "status": "failed",
                    "message": message,
                }
                for hold in holds
            )
        if pair.stopped is not None:
            errors.append(
                f"the transfer to {locator} stopped: {pair.stopped}; {resume} drives the holds left on {src.locator}"
            )


class _Transfer:
    """One paged transfer (plan §7.8): a local SRC's (push, local → local), or a remote SRC's over its transfer
    session (pull, and remote → remote, plan §7.9); its pairs of ends, outcomes and errors.

    :param src: The source end.
    :param dst: The destination end; ``None`` for ``--resume`` without DST (each hold goes where it records).
    :param ends: The ends resolved so far, by locator.
    :param context: The CLI invocation.
    :param arguments: The command's arguments.
    :param sessions: Closes the command's transfer sessions at its end.
    :param source: A pull's transfer session to SRC.
    """

    def __init__(
        self,
        src: _End,
        dst: _End | None,
        ends: dict[str, _End],
        context: CLIContext,
        arguments: argparse.Namespace,
        sessions: contextlib.ExitStack,
        source: _SourceSession | None = None,
    ) -> None:
        self.src, self.dst, self.ends = src, dst, ends
        self.context, self.arguments, self.sessions, self.source = context, arguments, sessions, source
        self.pairs: dict[str, _Pair] = {}
        self.outcomes: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._unreachable: dict[str, str] = {}
        #: Why every pair stopped (SRC failed), when one did.
        self.failure: str | None = None
        #: Step 1 stopped before SRC's holds were all listed: those not listed stay held there.
        self.unlisted = False

    def pair(self, dst: _End) -> _Pair:
        """Return the pair of *dst*, made at its first use.

        :param dst: The destination end.
        :return: The pair.
        """

        return self.pairs.setdefault(dst.locator, _Pair(dst))

    def route(self, holds: list[_moving.Hold]) -> list[tuple[_Pair, list[_moving.Hold]]]:
        """Group *holds* by the destination each records, each group with its pair; a hold that records none, or
        one whose destination cannot be resolved, is an error.

        :param holds: The holds, in order.
        :return: The groups, in order of their first hold.
        """

        groups: dict[str, list[_moving.Hold]] = {}
        for hold in holds:
            locator = hold.destination_locator
            if self.dst is not None and locator != self.dst.locator:
                continue
            if locator is None:
                self.errors.append(f"{hold.transfer_id}: the hold records no destination; adopt or release it by hand")
                continue
            if locator not in self.ends and locator not in self._unreachable:
                try:
                    self.ends[locator] = _end(locator, self.context, self.arguments)
                except _ERRORS as exc:
                    self._unreachable[locator] = str(exc)
            if locator in self._unreachable:
                reason = self._unreachable[locator]
                self.errors.append(f"{hold.transfer_id}: cannot reach its destination {locator}: {reason}")
                continue
            groups.setdefault(locator, []).append(hold)
        return [(self.pair(self.ends[locator]), group) for locator, group in groups.items()]

    def start(self, pair: _Pair) -> None:
        """Decide how *pair* is driven, at its first use (before its first hold is made or driven): a remote DST's
        transfer session is opened here (one that cannot open stops the pair: nothing of it is held), or found not
        served, and the pair's holds are then gathered for the adapter operations (remote → remote: for one adapter
        delivery relayed over SRC's session, plan §7.9)."""

        if pair.mode is not None:
            return
        pair.mode = "page"
        if pair.dst.workspace is not None:
            return
        if _session_disabled(pair.dst, self.context):
            # A DST that serves no session by its setting: the adapter operations, as before sessions (nothing
            # opened, nothing said).
            pair.mode = "adapter"
            return
        try:
            pair.session = _push_session(pair.dst, self.context, self.arguments, self.sessions)
        except _ERRORS as exc:
            pair.stopped = f"{pair.dst.locator}: the transfer session did not open: {exc}"
            return
        if pair.session is None:
            pair.mode = "adapter"

    def drive(self, pair: _Pair, holds: list[_moving.Hold], fresh: frozenset[str] = frozenset()) -> None:
        """Drive one page of *pair* (an adapter pair's holds are gathered, driven once by :meth:`finish`). An error
        escaping the drive stops the pair, the page's holds reported as left held unless adopted.

        :param pair: The pair.
        :param holds: The page's holds bound for it.
        :param fresh: Those this run created.
        """

        if pair.stopped is None:
            self.start(pair)  # not for a stopped pair: no session is opened for it
        if pair.stopped is not None:
            self.outcomes += self._undriven(pair, holds)
            return
        if pair.mode == "adapter":
            pair.held += holds
            pair.fresh.update(fresh)
            return
        try:
            self.outcomes += _drive(
                self.src,
                pair.dst,
                holds,
                self.context,
                self.arguments,
                fresh=fresh,
                sessions=self.sessions,
                source=self.source,
                pair=pair,
            )
        except _ERRORS as exc:
            pair.stopped = str(exc)
            message = (
                f"the transfer to {pair.dst.locator} failed while it was driven ({exc}); it stays held on "
                f"{self.src.locator} unless it was adopted: `httk job transfer --resume {self.src.locator} "
                f"{pair.dst.locator}` delivers or releases it"
            )
            self.outcomes += [self._outcome(pair, hold, message) for hold in holds]
            _abandon_pending(pair)

    def _outcome(self, pair: _Pair, hold: _moving.Hold, message: str) -> dict[str, Any]:
        return {
            "transfer_id": hold.transfer_id,
            "members": list(hold.members),
            "destination": pair.dst.locator,
            "status": "failed",
            "message": message,
        }

    def _undriven(self, pair: _Pair, holds: list[_moving.Hold]) -> list[dict[str, Any]]:
        message = (
            f"not delivered, since the transfer to {pair.dst.locator} stopped ({pair.stopped}); it stays held on "
            f"{self.src.locator}"
        )
        return [self._outcome(pair, hold, message) for hold in holds]

    @contextlib.contextmanager
    def interruptible(self) -> Iterator[None]:
        """On an interrupt (or another escaping exception), abandon every pair's staged segments (their owners
        closed, their landings discarded; their holds stay) before it propagates. The batches that then end conclude:
        the holds already adopted are released only over a session that is still usable, else reported with the
        ``--resume`` remedy (a stage interrupted in its adoption concludes nothing while the interrupt unwinds)."""

        try:
            yield
        except BaseException:
            for pair in self.pairs.values():
                pair.stopped = pair.stopped or "the transfer was interrupted"
                pair.interrupted = True
                with contextlib.suppress(Exception):
                    _abandon_pending(pair)
            raise

    def stop_all(self, reason: str) -> None:
        """Stop every pair at once (SRC failed): nothing more is driven or held for any.

        :param reason: Why.
        """

        self.failure = self.failure or reason
        if self.dst is not None:
            self.pair(self.dst)
        for pair in self.pairs.values():
            pair.stopped = pair.stopped or reason
            _abandon_pending(pair)  # their holds stay held: a pull's could not be released on SRC

    def finish(self, selection: str | None = None) -> None:
        """Drive each adapter pair's gathered holds once, as before sessions, and report every stopped pair: its
        reason, what of the selection was not attempted (*selection*, for DST's pair) and the ``--resume`` that drives
        what it left held; a failure of SRC that stopped no pair is reported on its own.

        :param selection: What of the selection was not attempted, when there was one and DST's pair stopped.
        """

        src = self.src
        unlisted = f"the holds not yet listed stay held on {src.locator}; " if self.unlisted else ""
        for pair in list(self.pairs.values()):
            # What each pair staged and has not adopted yet, before anything is reported (plan §7.10).
            try:
                _flush(pair)
            except Exception as exc:  # a stage's own failures are settled inside: this is the queue's, reported
                pair.stopped = pair.stopped or f"the transfer failed while its staged segments were adopted: {exc}"
                with contextlib.suppress(Exception):
                    _abandon_pending(pair)
            if self.source is not None and (session := self.source.get()) is not None and not session.is_open:
                self.stop_all(f"the transfer session to {src.locator} failed")
        for pair in list(self.pairs.values()):
            if pair.held and pair.stopped is None and self.source is not None:
                session = self.source.get()
                if session is not None and not session.is_open:
                    # SRC's session failed while an earlier pair was driven: every pair stops.
                    self.stop_all(f"the transfer session to {src.locator} failed")
            if pair.held and pair.stopped is None:
                holds = sorted(pair.held, key=lambda hold: hold.transfer_id)
                try:
                    self.outcomes += _drive(
                        src,
                        pair.dst,
                        holds,
                        self.context,
                        self.arguments,
                        fresh=frozenset(pair.fresh),
                        sessions=self.sessions,
                        source=self.source,
                        pair=pair,
                    )
                    _flush(pair)
                except _ERRORS as exc:
                    pair.stopped = str(exc)
                    message = f"the transfer failed ({exc}); it stays held on {src.locator} unless it was adopted"
                    self.outcomes += [self._outcome(pair, hold, message) for hold in holds]
                    _abandon_pending(pair)
            elif pair.held:
                self.outcomes += self._undriven(pair, pair.held)
            if pair.stopped is None:
                continue
            message = f"the transfer to {pair.dst.locator} stopped: {pair.stopped}; {unlisted}"
            if selection is not None and self.dst is not None and pair.dst.locator == self.dst.locator:
                message += f"{selection}; "
            message += (
                f"`httk job transfer --resume {src.locator} {pair.dst.locator}` drives the holds left on {src.locator}"
            )
            if selection is not None:
                message += " (it does not select the rest: run the transfer again for that)"
            self.errors.append(message)
        if self.failure is not None and not self.pairs:
            self.errors.append(
                f"{self.failure}; the holds on {src.locator} stay held there: `httk job transfer --resume "
                f"{src.locator}` drives them"
            )


def _not_attempted(count: int | None, *, bound: bool = False, carried: int = 0, src: str = "SRC") -> str:
    """What of a selection a stop left unattempted: *count* jobs (at most, with *bound*); with no count, the whole
    selection, or (with *bound*: a first page whose answer was lost) an unknown part of it; and the *carried* jobs
    held on *src* but not reported yet."""

    if count is None and bound:
        text = "the selection may have been partly held; how much was attempted is not known"
    elif count is None:
        text = "the selection was not attempted"
    else:
        text = f"{'at most ' if bound else ''}{count} selected job(s) were not attempted"
    return text + (f"; {carried} held job(s) not yet reported stay held on {src}" if carried else "")


def _transfer_local(run: _Transfer, selection: "_Selection | None") -> int | None:
    """A transfer from a local SRC, paged (plan §7.8): the holds left bound for DST (every hold for ``--resume``
    without DST) driven first, a page at a time from one listing of their names (step 1); then, with a *selection*,
    its snapshot (step 2) and its pages, each held and driven before the next is held, until it is exhausted or the
    pair stops (step 3). A DST that serves no transfer session gets the selection held as before sessions
    (:func:`hold_selected`), and every hold driven once. An error between pages stops the pair (reading SRC's
    holds: every pair); the pages driven before are reported.

    :param run: The transfer.
    :param selection: The selection, resolved (step 0); ``None`` for ``--resume``.
    :return: The listed jobs left out as changed since the listing; ``None`` without a selection or when it was held
        as before sessions.
    """

    src, dst, arguments = run.src, run.dst, run.arguments
    assert src.workspace is not None
    # Listed once: a page drives exactly the holds made for it, never one of a later listing.
    names = _moving.held_names(src.workspace)
    destination = None if dst is None else dst.locator
    pages = _held_pages(src.workspace, names, destination, workflows=not arguments.no_workflows)
    try:
        for earlier, more in pages:
            for pair, holds in run.route(earlier):
                run.drive(pair, holds)  # every hold of this step predates the run
            if dst is not None and run.pair(dst).stopped is not None:
                run.unlisted = more  # nothing more is listed for a stopped pair
                break
    except _ERRORS as exc:
        run.stop_all(f"reading the holds on {src.locator} failed: {exc}")
        run.unlisted = True
    del names
    if selection is None:
        run.finish()
        return None
    assert dst is not None
    pair = run.pair(dst)
    changed: int | None = 0
    try:
        if pair.mode is None and dst.workspace is None and _session_disabled(dst, run.context):
            run.start(pair)  # known without asking DST: held as before sessions, nothing listed twice
        if pair.mode != "adapter" and pair.stopped is None:
            selection.snapshot()  # not after a stop: nothing of the selection is attempted then
            if pair.stopped is None and not selection.done:
                run.start(pair)  # before the first hold: a push that cannot open its session holds nothing
        if pair.stopped is None:
            _holding(arguments, src)
        if pair.stopped is None and pair.mode == "adapter":
            # As before sessions: everything held first, delivered once.
            held = hold_selected(
                src.workspace,
                run.context,
                arguments,
                run.errors,
                locator=dst.locator,
                destination_id=dst.workspace_id,
                workflows=not arguments.no_workflows,
            )
            pair.held += held
            pair.fresh.update(hold.transfer_id for hold in held)
            changed = None
        while changed is not None and pair.stopped is None and not selection.done:
            page = selection.next_page(
                holds=_PAGE_HOLDS, errors=_PAGE_ERRORS, scanned=_PAGE_SCANNED, members=_PAGE_MEMBERS
            )
            run.errors.extend(page.errors)
            changed += page.changed
            if page.holds:
                run.drive(pair, page.holds, fresh=frozenset(hold.transfer_id for hold in page.holds))
    except _ERRORS as exc:
        pair.stopped = pair.stopped or str(exc)
    if pair.mode == "adapter":
        changed = None  # held as before sessions, or not at all
    if changed is not None and pair.stopped is None and (note := selection.note()) is not None:
        print(note, file=sys.stderr)
    # Without its listing nothing was attempted: no count.
    count = selection.remaining() if selection.snapshotted else None
    run.finish(_not_attempted(count) if pair.stopped is not None else None)
    return changed


def _reported_holds(value: object) -> tuple[list[_moving.Hold], list[tuple[str, int]]]:
    """The holds a page of a remote SRC reported, and those it named by id only (``oversized``: the transfer id and
    member count); :class:`ValueError` (or :class:`TypeError`) for anything else."""

    if not isinstance(value, list):
        raise ValueError("no holds")
    holds: list[_moving.Hold] = []
    oversized: list[tuple[str, int]] = []
    for item in value:
        if isinstance(item, dict) and item.get("oversized") is True:
            transfer_id, count = item.get("transfer_id"), item.get("member_count")
            if not isinstance(transfer_id, str) or not _TRANSFER_ID.fullmatch(transfer_id):
                raise ValueError(f"a malformed oversized hold: {item!r}")
            if not isinstance(count, int) or isinstance(count, bool):
                raise ValueError(f"a malformed oversized hold: {item!r}")
            oversized.append((transfer_id, count))
        else:
            holds.append(_moving.Hold.from_mapping(item))
    return holds, oversized


def _transfer_pull(run: _Transfer, session: TransferSession, selected: bool) -> int | None:
    """A transfer from a remote SRC over its transfer *session* (pull), paged (plan §7.8): with a selection, SRC
    first checks and resolves it (``select``, step 0: a bad one drives nothing); the holds left on SRC bound for DST
    (every hold for ``--resume`` without DST) are driven a page at a time (``holds_page``, step 1); then SRC takes
    the selection's listing and holds it page by page (``hold_page``), each page driven before the next is asked for.
    A failure of the session (or an answer that cannot be used) stops every pair at once.

    :param run: The transfer.
    :param session: The pull's open transfer session to SRC.
    :param selected: Whether the command selects jobs (not ``--resume``).
    :return: The listed jobs left out as changed since the listing; ``None`` without a selection.
    """

    src, dst, arguments = run.src, run.dst, run.arguments
    if selected:
        assert dst is not None
        refusal, failed = _pull_select(run, session)
        if refusal is not None:
            run.errors.append(refusal)  # nothing changed: nothing held or driven
            return None
        if failed is not None:
            run.stop_all(failed)
            run.finish(_not_attempted(None))
            return None
    failed = None
    after: str | None = None
    try:
        while True:
            header: dict[str, Any] = {
                "workspace": src.plain,
                "destination": None if dst is None else dst.locator,
                "after": after,
                "limit_holds": _PAGE_HOLDS,
                "limit_members": _PAGE_MEMBERS,
            }
            try:
                response = session.request("holds_page", header)
            except SessionFailed as exc:
                failed = f"the transfer session to {src.locator} failed while listing its holds: {exc}"
                break
            if response.get("ok") is not True:
                failed = f"transfer status failed on {src.locator}: {response.get('error')}"
                break
            try:
                holds, oversized = _reported_holds(response.get("holds"))
                following = response.get("next")
                if following is not None and not (
                    isinstance(following, str)
                    and _TRANSFER_ID.fullmatch(following)
                    and (after is None or following > after)
                ):
                    raise ValueError(f"a cursor that does not advance: {following!r}")
            except (ValueError, TypeError, WorkflowError) as exc:
                session.close()  # a far side that answers something else is not asked anything more
                failed = f"{src.locator} answered a page of its holds unusably: {exc}"
                break
            _oversized(run, oversized)
            for pair, group in run.route(holds):
                run.drive(pair, group)  # every hold of this step predates the run
                if not session.is_open:
                    # Every pair stops at once: the groups not yet driven do nothing.
                    run.stop_all(failed := f"the transfer session to {src.locator} failed")
            if failed is not None or following is None:
                break
            after = following
            if dst is not None and run.pair(dst).stopped is not None:
                run.unlisted = True  # as a local SRC: nothing more is listed for a stopped pair
                break
    except _ERRORS as exc:
        failed = f"listing the holds on {src.locator} failed: {exc}"
    if failed is not None:
        run.unlisted = True
        run.stop_all(failed)
    if not selected:
        run.finish()
        return None
    assert dst is not None
    pair = run.pair(dst)
    if pair.stopped is not None:
        run.finish(_not_attempted(None))
        return 0
    run.start(pair)
    _holding(arguments, src)
    left, bound, carried, changed, failed = _pull_pages(run, session, pair)
    if failed is not None:
        run.stop_all(failed)
    stopped = _not_attempted(left, bound=bound, carried=carried, src=src.locator)
    run.finish(stopped if pair.stopped is not None else None)
    return changed


def _pull_select(run: _Transfer, session: TransferSession) -> tuple[str | None, str | None]:
    """Step 0 of a pull: ``select`` the selection on SRC, which checks and resolves it and holds nothing.

    :param run: The transfer.
    :param session: The pull's open transfer session to SRC.
    :return: The refusal (nothing changed) or the session's failure, either ``None``.
    """

    src, dst, arguments = run.src, run.dst, run.arguments
    assert dst is not None
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
        response = session.request("select", header)
    except SessionFailed as exc:
        return None, f"the transfer session to {src.locator} failed while selecting: {exc}"
    sys.stderr.write(str(response.get("stderr", "")))
    if response.get("ok") is not True:
        return f"job transfer --hold on {src.locator} was refused: {response.get('error')}", None
    return None, None


def _pull_pages(run: _Transfer, session: TransferSession, pair: _Pair) -> tuple[int | None, bool, int, int, str | None]:
    """Steps 2 and 3 of a pull: ``hold_page`` until the selection is done or the pair stops, each page driven before
    the next is asked for. Every answer that is not ``done`` must have ``consumed`` more than the one before, or SRC
    is taken as out of step: the session closes and the pair stops, as when it failed. A refusal of the first page
    (SRC answered it, holding nothing) leaves the selection not attempted.

    :param run: The transfer.
    :param session: The pull's open transfer session to SRC.
    :param pair: DST's pair.
    :return: How many selected jobs SRC had not attempted at its last answer (``None`` before any), whether that is
        an upper bound only (a later answer was lost: its page may have held some), the jobs SRC held but had not
        reported at its last answer, the listed jobs left out as changed, and why SRC's session stopped, if it did.
    """

    src = run.src
    left: int | None = None
    carried = consumed = 0
    changed = 0
    limits = {"limit_candidates": _PAGE_SCANNED, "limit_holds": _PAGE_HOLDS, "limit_members": _PAGE_MEMBERS}
    # A page may be held before its answer is lost: never "nothing was held".
    lost = f"holding a page on {src.locator} failed, and the jobs it may have held stay held there"
    while pair.stopped is None:
        try:
            response = session.request("hold_page", limits)
        except SessionFailed as exc:
            return left, True, carried, changed, f"{lost} (the transfer session failed: {exc})"
        sys.stderr.write(str(response.get("stderr", "")))  # the far side's notes, such as jobs left out by --step
        if response.get("ok") is not True and response.get("fatal") is not True and left is None:
            # A refusal of the first page: SRC changed nothing (an error before the page's owner), held nothing.
            return None, False, carried, changed, f"holding on {src.locator} was refused: {response.get('error')}"
        if response.get("ok") is not True:
            return left, True, carried, changed, f"{lost} ({response.get('error')})"
        try:
            holds, oversized = _reported_holds(response.get("holds"))
            errors, page_changed, done = response.get("errors"), response.get("changed"), response.get("done")
            remaining, note = response.get("remaining"), response.get("note")
            counts = [page_changed, remaining, response.get("consumed"), response.get("carried")]
            if not isinstance(errors, list) or not isinstance(done, bool):
                raise ValueError("no hold page")
            if not all(isinstance(count, int) and not isinstance(count, bool) for count in counts):
                raise ValueError("no hold page")
            if not (note is None or isinstance(note, str)):
                raise ValueError("no hold page")
            assert isinstance(page_changed, int) and isinstance(remaining, int)
            previous, consumed, carried = consumed, int(response["consumed"]), int(response["carried"])
            if not done and consumed <= previous:
                raise ValueError(f"a page that made no progress (consumed {consumed} after {previous})")
        except (ValueError, TypeError, WorkflowError) as exc:
            session.close()  # a far side that answers something else is not asked anything more
            return left, True, carried, changed, f"{lost} (an unusable answer: {exc})"
        run.errors.extend(str(error) for error in errors)
        changed += page_changed
        left = remaining
        _oversized(run, oversized)
        if holds:
            run.drive(pair, holds, fresh=frozenset(hold.transfer_id for hold in holds))  # made by this run
        if not session.is_open:
            return left, False, carried, changed, f"the transfer session to {src.locator} failed"
        if done:
            if note is not None and pair.stopped is None:
                print(note, file=sys.stderr)
            break
    return left, False, carried, changed, None


def _oversized(run: _Transfer, holds: list[tuple[str, int]]) -> None:
    """Fail the holds a remote SRC named by id only, their documents too large for one answer: each stays held."""

    src, dst = run.src, run.dst
    for transfer_id, count in holds:
        resume = f"`httk job transfer --resume {src.locator}{'' if dst is None else ' ' + dst.locator}`"
        assert src.binding is not None
        message = (
            f"its {count} job(s) are too many for one answer of the transfer session; it stays held on {src.locator}: "
            f"set `httk remote configure --set transfer_session=off {src.binding.remote}`, then {resume} drives it"
        )
        run.outcomes.append(
            {
                "transfer_id": transfer_id,
                "members": [],
                "destination": None if dst is None else dst.locator,
                "status": "failed",
                "message": message,
            }
        )


def status_document(workspace: Workspace) -> dict[str, Any]:
    """Return the document ``transfer status --json`` prints for a local *workspace*: every hold, with the workflows
    its members need.

    :param workspace: The workspace.
    :return: ``{"format", "format_version", "workspace", "holds"}``.
    """

    now = time.time()
    listed = [hold.as_mapping(now) for hold in _moving.held(workspace, workflows=True)]
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
From a local SRC, and from a remote SRC over its transfer session, the
selection is held page by page (up to 256 jobs a page), each page copied,
adopted and released before the next is held, over one connection to a remote
end however many pages there are; elsewhere (a remote end without a session,
remote to remote) every job is held first and the holds travel as one batch.
A listed job that changed since the listing is left out and counted
(`changed` in --json), not an error. A failure that stops a destination (a
failed session, an unknown outcome, a refusal naming no bundle) holds nothing
more for it: the command exits 1, says how many selected jobs were not
attempted, and names the --resume that drives what it left held; run the
transfer again to select the rest. Progress goes to stderr unless -q is given.

Workflows travel too: before adopting a page that needs one not yet attempted,
the transfer installs in DST, under the same ids, the workflows the jobs need
(each job's effective workflow and its calls) that DST lacks and SRC has,
copying only their sources, and builds those with [workflow.build] there
(without a session, a remote DST builds them after the adoption). An
installation DST already has is never replaced, even with another tree
(reported). --no-workflows ships none.

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
