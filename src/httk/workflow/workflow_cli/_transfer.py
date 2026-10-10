"""Remote and transfer command groups.

``job transfer`` moves jobs between workspaces as hold + copy + adopt + release
on :mod:`httk.workflow._moving`, locally or through the remote adapters'
``invoke``, ``push`` and ``pull``; ``transfer status`` lists held transfers.
"""

import argparse
import json
import os
import re
import shlex
import sys
import tempfile
import time
from collections.abc import Sequence
from contextlib import redirect_stdout
from copy import copy
from dataclasses import dataclass
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any, cast

from httk.core.cli import CLIContext

from .. import _fs, _kernel, _moving
from .._bundles import BundleError
from .._util import write_json_atomic
from ..adapters import (
    REMOTE_JOB_ADOPT_COMMAND,
    REMOTE_JOB_TRANSFER_COMMAND,
    REMOTE_TRANSFER_STATUS_COMMAND,
    add_remote,
    import_v1_remote,
    list_remotes,
    metadata_path,
    probe_remote_workspace,
    read_credentials,
    read_metadata,
    resolve_remote,
    run_adapter,
    split_settings,
    store_credentials,
)
from ..errors import ResolutionMiss, WorkflowError
from ..hygiene import describe_remote, remove_remote
from ..introspection import iter_jobs, read_job, read_state, resolve_job_selectors
from ..models import WORKSPACE_DIRECTORY, canonical_uuid, normalize_placement
from ..registry import LOCAL_REMOTE, WorkspaceBinding, default_workspace, list_workspaces, resolve_workspace
from ..seals import require_cli_modifiable
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _durable,
    _field,
    _group,
    _leaf,
    _required,
    _run_remote_workspace,
    _settings,
    add_durability_arguments,
    confirm,
    remote_workspace_output,
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
                "prelude= (shell run before each remote command, e.g. module loads and a "
                "venv activation), or interactive_auth= (yes|per-call lets ssh prompt in the "
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


def _workspace_reference(value: str, context: CLIContext) -> Path | WorkspaceBinding:
    """Resolve a registered name (``REMOTE:WS`` included) or a workspace directory: a local root or a remote binding."""

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
        return candidate
    if binding.remote == LOCAL_REMOTE and binding.path is not None:
        return Path(binding.path)
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
    root: str | None = None
    workspace_id: str | None = None

    @property
    def plain(self) -> str:
        assert self.binding is not None
        return self.binding.name.split(":", 1)[1]


def _end(value: str | None, context: CLIContext, arguments: argparse.Namespace, *, probe: bool = False) -> _End:
    """Resolve one end; *value* ``None`` is the enclosing or default workspace. *probe* learns a remote's root and id."""

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
    root = workspace_id = None
    if probe:
        target = resolve_remote(reference.remote, project=context.cwd)
        workspace_id, root = probe_remote_workspace(
            target, reference.name.split(":", 1)[1], timeout=arguments.adapter_timeout
        )
    return _End(reference.name, binding=reference, root=root, workspace_id=workspace_id)


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


def eject_once(
    workspace: Workspace,
    owner: _kernel.Owner,
    ref: _kernel.JobRef,
    destination: Path | None,
    *,
    tree: bool,
    locator: str | None = None,
    destination_workspace_id: str | None = None,
) -> _moving.EjectReport | _moving.Hold | str:
    """Claim and eject one job, or hold it when *destination* is ``None``: the report, or why it cannot move now.

    :param workspace: The workspace.
    :param owner: The ejecting owner.
    :param ref: The job.
    :param destination: Where the bundle goes; ``None`` holds it in ``transfers/outgoing/``.
    :param tree: Move the job's descendants too.
    :param locator: The transfer destination a hold records.
    :param destination_workspace_id: The destination workspace's id a hold records.
    :return: The report (the hold, when held), or the reason; a job that cannot move stays where it is (as after
        any raised error).
    """

    root = _kernel.claim(workspace, owner, ref)
    if root is None:
        return f"{ref.job_key} moved before it could be claimed"
    try:
        if destination is not None:
            return _moving.eject(workspace, owner, root, destination=destination, tree=tree)
        return _moving.hold(
            workspace,
            owner,
            root,
            tree=tree,
            destination_locator=locator,
            destination_workspace_id=destination_workspace_id,
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


def _holds(src: _End, context: CLIContext, arguments: argparse.Namespace) -> list[_moving.Hold]:
    """The holds of *src*."""

    if src.workspace is not None:
        return _moving.held(src.workspace)
    code, out, err = _remote(src, context, arguments, [*REMOTE_TRANSFER_STATUS_COMMAND, "--json", src.plain])
    if code:
        raise RuntimeError(f"transfer status failed on {src.locator}: {err.strip()}")
    document = json.loads(out)
    if not isinstance(document, dict) or not isinstance(document.get("holds"), list):
        raise ValueError(f"{src.locator} returned an invalid transfer status")
    return [_moving.Hold.from_mapping(hold) for hold in document["holds"]]


def _parent_here(ref: _kernel.JobRef, present: set[str]) -> bool:
    """Whether *ref* is a non-detached child of a job in *present*: a member of its root's tree, as ejection rules."""

    job, _ = read_job(ref)
    if job is None or job.parent is None or str(job.parent.get("job_id")) not in present:
        return False
    doc, _ = read_state(ref)
    return doc is None or doc.detached is None


def _hold_jobs(
    src: _End, dst: _End, context: CLIContext, arguments: argparse.Namespace, errors: list[str]
) -> list[_moving.Hold]:
    """Step 1: hold every selected job on *src*, recording *dst* and its id; a job that cannot move is an error."""

    holds: list[_moving.Hold] = []
    if src.workspace is not None:
        workspace = src.workspace
        require_cli_modifiable(workspace)
        refs = resolve_job_selectors(workspace, context.cwd, arguments.jobs)
        children: list[_kernel.JobRef] = []
        if arguments.kind:
            seen = {ref.job_id for ref in refs}
            # Listed in full first: holding moves jobs out of the directories the listing walks.
            listed = list(iter_jobs(workspace, arguments.kind, placement_prefix=arguments.placement))
            present = {ref.job_id for ref in iter_jobs(workspace)}
            for ref in listed:
                if ref.job_id not in seen:
                    seen.add(ref.job_id)
                    # A child whose parent is here moves only in its root's tree, never on its own.
                    (children if _parent_here(ref, present) else refs).append(ref)
        moving: set[str] = set()
        with _owner(workspace, "job transfer") as owner:
            for ref in refs:
                if ref.job_key in moving:
                    continue  # it went with an earlier selected tree
                try:
                    outcome = eject_once(
                        workspace,
                        owner,
                        ref,
                        None,
                        tree=arguments.tree,
                        locator=dst.locator,
                        destination_workspace_id=dst.workspace_id,
                    )
                except _ERRORS as exc:
                    outcome = f"{ref.job_key}: {exc}"
                if isinstance(outcome, str):
                    errors.append(outcome)
                else:
                    assert isinstance(outcome, _moving.Hold)
                    holds.append(outcome)
                    moving.update(outcome.members)
        if left := sum(ref.job_key not in moving for ref in children):
            print(
                f"{left} selected job(s) have a parent in {src.locator} and move only with their root "
                "(use --tree, or detach them)",
                file=sys.stderr,
            )
        return holds
    for selector in arguments.jobs:
        try:
            canonical_uuid(selector, "JOB")
        except (WorkflowError, ValueError) as exc:
            raise ValueError(f"a remote source requires canonical job ids: {selector!r}") from exc
    argv = [*REMOTE_JOB_TRANSFER_COMMAND, "--hold", "--json", *(["--tree"] if arguments.tree else [])]
    argv += [*(["--destination-id", dst.workspace_id] if dst.workspace_id is not None else [])]
    # --kind names no ids, so it goes in one call: the last, as a --job of a later call that --kind already held
    # would no longer resolve there.
    kinds = [item for state in arguments.kind for item in ("--kind", state)]
    kinds += ["--placement", arguments.placement] if arguments.placement is not None else []
    chunks = _chunks(arguments.jobs, flag="--job") or [[]]
    for index, chunk in enumerate(chunks):
        selected = [item for selector in chunk for item in ("--job", selector)]
        selected += kinds if index == len(chunks) - 1 else []
        # Holds may exist after any failure here: the pass over the source's holds, or a later --resume, drives them.
        try:
            code, out, err = _remote(src, context, arguments, [*argv, *selected, "--", src.plain, dst.locator])
        except _ERRORS as exc:
            errors.append(
                f"job transfer --hold on {src.locator} failed: {exc}; jobs it already held are driven by this run's "
                f"pass over {src.locator}'s holds or by `httk job transfer --resume {src.locator} {dst.locator}`"
            )
            continue
        try:
            document = json.loads(out)
            if not isinstance(document, dict) or not all(
                isinstance(document.get(key), list) for key in ("holds", "errors")
            ):
                raise ValueError("no hold document")
            held = [_moving.Hold.from_mapping(hold) for hold in document["holds"]]
        except (ValueError, TypeError):
            message = f"job transfer --hold on {src.locator} printed no hold document"
            message += f": {err.strip()}" if code and err.strip() else ""
            message += f": {out[:200]!r}" if out else ""
            if "unrecognized arguments" in err:
                message += f" ({src.locator} may run an older httk-workflow; upgrade it)"
            errors.append(message)
            continue
        holds += held
        errors.extend(str(error) for error in document["errors"])
        sys.stderr.write(err)  # the far side's notes, such as children left with their root
    return holds


#: The statuses of a ``job adopt --json`` entry.
_ADOPT_STATUSES = ("adopted", "already_adopted", "refused", "taken", "failed")


def _adopt_local(workspace: Workspace, owner: _kernel.Owner, source: Path, absent: frozenset[str]) -> dict[str, Any]:
    """Adopt one bundle of a batch (*absent* settled for it) into a local *workspace*: a ``job adopt --json`` entry."""

    try:
        report = _moving.adopt(workspace, owner, source, untrusted=False, absent=absent)
    except BundleError as exc:
        return {"status": "refused", "message": str(exc)}
    except _ERRORS as exc:
        return {"status": "failed", "message": str(exc)}
    if report is None:
        return {"status": "taken"}
    return {"status": "already_adopted" if report.already_adopted else "adopted", **adopt_document(report)}


def _adoption(out: str, bundles: list[str]) -> list[dict[str, Any]]:
    """The entries of a remote ``job adopt --json`` of *bundles*, checked; :class:`ValueError` when malformed."""

    document = json.loads(out)
    entries = document.get("bundles") if isinstance(document, dict) else None
    if (
        not isinstance(entries, list)
        or len(entries) != len(bundles)
        or not all(
            isinstance(entry, dict) and entry.get("bundle") == bundle and entry.get("status") in _ADOPT_STATUSES
            for entry, bundle in zip(entries, bundles, strict=False)
        )
    ):
        raise ValueError("not an adoption report of the bundles sent")
    return entries


def _deliver(
    src: _End,
    dst: _End,
    holds: list[_moving.Hold],
    context: CLIContext,
    arguments: argparse.Namespace,
    token: str,
) -> dict[str, dict[str, Any]]:
    """Steps 2 and 3 for a chunk: one copy of its held bundles to *dst* and one adoption there.

    A remote *dst* receives them in ``transfers/incoming/<token>/``. The result is each hold's adoption entry by
    transfer id (``status`` as ``job adopt --json`` gives it); an error raised fails the whole chunk.
    """

    ids = [hold.transfer_id for hold in holds]
    # One workspace's holds share its transfers/outgoing/.
    outgoing = str(PurePosixPath(holds[0].path).parent)
    if dst.workspace is not None:
        require_cli_modifiable(dst.workspace)
        failure: Exception | None = None
        entries: dict[str, dict[str, Any]] = {}
        with _owner(dst.workspace, "job transfer") as owner:
            # Errors are kept, not raised, inside: an owner left by an error stays open for recovery.
            scratch = None
            try:
                if src.workspace is not None:
                    # One filesystem: adoption moves each hold in, and a refusal moves it back. Across: it copies.
                    sources = [Path(hold.path) for hold in holds]
                else:
                    scratch = owner.scratch("landing")
                    _say(arguments, f"copying {len(ids)} bundle(s) from {src.locator}")
                    _adapter(src, context, arguments, "pull", outgoing, str(scratch / "outgoing"), ids)
                    sources = [scratch / "outgoing" / transfer_id for transfer_id in ids]
                _say(arguments, f"adopting {len(ids)} bundle(s) in {dst.locator}")
                absent = _moving.settle_absent(dst.workspace, sources)
                for transfer_id, source in zip(ids, sources, strict=True):
                    entries[transfer_id] = _adopt_local(dst.workspace, owner, source, absent)
            except _ERRORS as exc:
                failure = exc
            finally:
                if scratch is not None:
                    owner.discard_scratch(scratch)
        if failure is not None:
            raise failure
        return entries
    assert dst.root is not None
    landing = PurePosixPath(dst.root, WORKSPACE_DIRECTORY, "transfers", "incoming", token)
    _say(arguments, f"copying {len(ids)} bundle(s) to {dst.locator}")
    if src.workspace is not None:
        _adapter(dst, context, arguments, "push", outgoing, str(landing), ids)
    else:
        with tempfile.TemporaryDirectory(prefix="httk-transfer-") as relay:
            _adapter(src, context, arguments, "pull", outgoing, f"{relay}/outgoing", ids)
            _adapter(dst, context, arguments, "push", f"{relay}/outgoing", str(landing), ids)
    _say(arguments, f"adopting {len(ids)} bundle(s) in {dst.locator}")
    # Bare transfer ids, adopted from within the landing, keep the command short; the workspace goes by its
    # probed root, so that resolving it does not depend on the landing as the working directory.
    argv = [*REMOTE_JOB_ADOPT_COMMAND, "--json", "--by-path", "--workspace", dst.root, *ids]
    code, out, err = _remote(dst, context, arguments, argv, cwd=str(landing))
    try:
        adopted = _adoption(out, ids)
    except ValueError:
        # Refused copies in the landing were discarded there: each hold stays the only bundle.
        if code:
            raise RuntimeError(err.strip() or f"job adopt failed on {dst.locator}") from None
        raise ValueError(
            f"{dst.locator} returned an unrecognised adoption report (it may run an older httk-workflow; upgrade "
            "it, check whether the jobs arrived with `httk job show`, and release with `httk job transfer --release`)"
        ) from None
    return dict(zip(ids, adopted, strict=True))


def _release(src: _End, transfer_ids: list[str], context: CLIContext, arguments: argparse.Namespace) -> list[str]:
    """Step 4: discard the holds on *src*; one already gone (adopted in on one filesystem) is fine.

    The result lists the ids a remote *src* did not confirm (an older remote takes only the last ``--release``).
    """

    _say(arguments, f"releasing {len(transfer_ids)} hold(s) on {src.locator}")
    if src.workspace is not None:
        with _owner(src.workspace, "job transfer") as owner:
            for transfer_id in transfer_ids:
                _moving.release_hold(src.workspace, owner, transfer_id)
        return []
    releases = [item for transfer_id in transfer_ids for item in ("--release", transfer_id)]
    code, out, err = _remote(src, context, arguments, [*REMOTE_JOB_TRANSFER_COMMAND, *releases, src.plain])
    if code not in (0, 1):
        raise RuntimeError(err.strip() or f"job transfer --release failed on {src.locator}")
    # Confirmed: "released T" on stdout, or "T: no such hold ..." on stderr (gone already).
    confirmed = {line.removeprefix("released ") for line in out.splitlines() if line.startswith("released ")}
    confirmed |= {line.split(":", 1)[0] for line in err.splitlines() if ": no such hold" in line}
    return [transfer_id for transfer_id in transfer_ids if transfer_id not in confirmed]


def _drive(
    src: _End, dst: _End, holds: list[_moving.Hold], context: CLIContext, arguments: argparse.Namespace
) -> list[dict[str, Any]]:
    """Copy, adopt and release a batch of holds bound for *dst*: one outcome per hold, in order.

    A hold that was not adopted stays held on *src*.
    """

    outcomes = {
        hold.transfer_id: {"transfer_id": hold.transfer_id, "members": list(hold.members), "destination": dst.locator}
        for hold in holds
    }
    ready = []
    for hold in holds:
        if hold.destination_workspace_id is not None and dst.workspace_id != hold.destination_workspace_id:
            message = (
                f"the hold's destination is not this workspace: {dst.locator} is {dst.workspace_id}, the hold is "
                f"bound for {hold.destination_workspace_id}; it stays held on {src.locator}"
            )
            outcomes[hold.transfer_id].update(status="refused", message=message)
        else:
            ready.append(hold)
    entries: dict[str, dict[str, Any]] = {}
    token = _fs.fresh_token()  # one landing for every chunk
    by_id = {hold.transfer_id: hold for hold in ready}
    for chunk in _chunks(list(by_id)):
        try:
            entries.update(_deliver(src, dst, [by_id[transfer_id] for transfer_id in chunk], context, arguments, token))
        except _ERRORS as exc:
            entries.update({transfer_id: {"status": "failed", "message": str(exc)} for transfer_id in chunk})
    delivered = []
    for transfer_id, entry in entries.items():
        outcome, status = outcomes[transfer_id], entry["status"]
        if status in _DELIVERED:
            outcome.update({key: value for key, value in entry.items() if key not in ("bundle", "message")})
            delivered.append(transfer_id)
        elif status == "refused":
            message = f"bundle refused: {entry.get('message')}; it stays held on {src.locator}"
            outcome.update(status="refused", message=message)
        elif status == "taken":
            outcome.update(status="taken", message="another actor took the held bundle first")
        else:
            outcome.update(status="failed", message=f"{entry.get('message')}; it stays held on {src.locator}")
    unreleased: list[tuple[str, str]] = []
    for chunk in _chunks(delivered, flag="--release"):
        try:
            unconfirmed = _release(src, chunk, context, arguments)
            unreleased += [(transfer_id, "not confirmed by the source") for transfer_id in unconfirmed]
        except _ERRORS as exc:
            unreleased += [(transfer_id, str(exc)) for transfer_id in chunk]
    for transfer_id, reason in unreleased:
        outcomes[transfer_id]["message"] = (
            f"the jobs are in {dst.locator}, but the hold was not released: {reason}; "
            f"run `httk job transfer --resume` or `httk job transfer --release {transfer_id}`"
        )
    return list(outcomes.values())


def _report(
    outcomes: list[dict[str, Any]], errors: list[str], *, json_output: bool, empty: str = "no held transfers to drive"
) -> int:
    missing = sorted({workflow for outcome in outcomes for workflow in outcome.get("missing_workflows", ())})
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
    for outcome in outcomes:
        if "message" in outcome:
            print(f"{outcome['transfer_id']}: {outcome['message']}", file=sys.stderr)
    failed = errors or any(outcome["status"] not in _DELIVERED or "message" in outcome for outcome in outcomes)
    return 1 if failed else 0


def _handle_release(arguments: argparse.Namespace, context: CLIContext) -> int:
    if arguments.destination is not None or arguments.jobs or arguments.kind or arguments.resume or arguments.hold:
        raise ValueError("--release takes transfer ids and at most the workspace holding them")
    for transfer_id in arguments.release:
        if not _TRANSFER_ID.fullmatch(transfer_id):
            raise ValueError(f"not a transfer id: {transfer_id!r}")
    src = _end(arguments.source, context, arguments)
    if src.workspace is None:
        releases = [item for transfer_id in arguments.release for item in ("--release", transfer_id)]
        code, out, err = _remote(src, context, arguments, [*REMOTE_JOB_TRANSFER_COMMAND, *releases, src.plain])
        sys.stdout.write(out)
        sys.stderr.write(err)
        return code
    with _owner(src.workspace, "job transfer --release") as owner:
        released = [_moving.release_hold(src.workspace, owner, transfer_id) for transfer_id in arguments.release]
    for transfer_id, done in zip(arguments.release, released, strict=True):
        if done:
            print(f"released {transfer_id}")
        else:
            print(f"{transfer_id}: no such hold in {src.locator}", file=sys.stderr)
    return 0 if all(released) else 1


def _handle_hold(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Hold the selected jobs of a local SRC for a transfer to the DST locator, recorded as given."""

    selected = arguments.jobs or arguments.kind
    if arguments.resume or arguments.source is None or arguments.destination is None or not selected:
        raise ValueError("--hold takes --job or --kind, SRC and the destination to record")
    src = _end(arguments.source, context, arguments)
    if src.workspace is None:
        raise ValueError(f"--hold holds jobs in a workspace on this machine, not in {src.locator}")
    destination_id = arguments.destination_id
    if destination_id is not None:
        destination_id = canonical_uuid(destination_id, "--destination-id")
    errors: list[str] = []
    dst = _End(arguments.destination, workspace_id=destination_id)
    holds = _hold_jobs(src, dst, context, arguments, errors)
    if arguments.json:
        print(json.dumps({"holds": [hold.as_mapping() for hold in holds], "errors": errors}, indent=2, sort_keys=True))
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

    if arguments.placement is not None:
        if not arguments.kind:
            raise ValueError("--placement narrows --kind; it needs --kind")
        if not normalize_placement(arguments.placement).parts:
            raise ValueError("--placement needs a placement prefix; leave it out to select under every placement")
    if arguments.release:
        return _handle_release(arguments, context)
    if arguments.hold:
        return _handle_hold(arguments, context)
    if arguments.destination_id is not None:
        raise ValueError("--destination-id is recorded by --hold; it needs --hold")
    selected = arguments.jobs or arguments.kind
    if not arguments.resume and (arguments.source is None or arguments.destination is None or not selected):
        raise ValueError("name jobs with --job or --kind, and SRC and DST; or re-drive held transfers with --resume")
    if arguments.resume and selected:
        raise ValueError("--resume re-drives held transfers and takes no --job or --kind")
    src = _end(arguments.source, context, arguments)
    ends: dict[str, _End] = {}
    dst = None
    if arguments.destination is not None:
        dst = _end(arguments.destination, context, arguments, probe=True)
        if dst.locator == src.locator:
            raise ValueError(f"SRC and DST are the same workspace: {src.locator}")
        ends[dst.locator] = dst
    errors: list[str] = []
    if selected:
        assert dst is not None
        if dst.workspace is not None:
            # Before anything is held: a destination that cannot take the jobs would only leave them held.
            require_cli_modifiable(dst.workspace)
        what = [f"{len(arguments.jobs)} job(s)"] if arguments.jobs else []
        what += [f"every {state} job" for state in arguments.kind]
        _say(arguments, f"holding {', '.join(what)} on {src.locator}")
        _hold_jobs(src, dst, context, arguments, errors)
    # Every run re-drives the holds bound for its destination (with --resume and no DST: every hold), each
    # destination's holds as one batch.
    batches: dict[str, list[_moving.Hold]] = {}
    for hold in _holds(src, context, arguments):
        locator = hold.destination_locator
        if arguments.destination is not None and locator not in ends:
            continue
        if locator is None:
            errors.append(f"{hold.transfer_id}: the hold records no destination; adopt or release it by hand")
            continue
        if locator not in ends:
            try:
                ends[locator] = _end(locator, context, arguments, probe=True)
            except _ERRORS as exc:
                errors.append(f"{hold.transfer_id}: cannot reach its destination {locator}: {exc}")
                continue
        batches.setdefault(locator, []).append(hold)
    outcomes = []
    for locator, holds in batches.items():
        outcomes.extend(_drive(src, ends[locator], holds, context, arguments))
    if arguments.kind:
        under = f" under {arguments.placement}" if arguments.placement is not None else ""
        empty = f"no {' or '.join(arguments.kind)} job to move in {src.locator}{under}"
        return _report(outcomes, errors, json_output=arguments.json, empty=empty)
    return _report(outcomes, errors, json_output=arguments.json)


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
    now = time.time()
    holds = _holds(src, context, arguments)
    if arguments.json:
        listed = [hold.as_mapping(now) for hold in holds]
        document = {"format": TRANSFER_STATUS_FORMAT, "format_version": 1, "workspace": src.locator, "holds": listed}
        print(json.dumps(document, indent=2, sort_keys=True))
        return 0
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
Move jobs between two workspaces: `job transfer --job JOB [--tree] SRC DST`,
or every job in a state: `job transfer --kind succeeded [--placement PREFIX]
[--tree] SRC DST` (--kind and --job combine; both repeat). --kind never
splits a tree: it leaves out jobs whose parent is in SRC, which move only in
their root's tree. Without --tree a selected job that has children present is
refused: --tree moves a finished campaign's roots with their children (failed,
cancelled or paused ones too).

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

Recovery: delivery is at least once. Every run, and `job transfer --resume
[SRC] [DST]`, first re-drives the holds bound for DST (with --resume and no DST,
every hold, to the destination it records); DST reports "already adopted" for
a bundle it already has, and the hold is released. `transfer status` and
`job why` list held bundles. A transfer abandoned for good is taken back into
SRC with `job adopt SRC/.httk-workspace/transfers/outgoing/<transfer id>`,
which is safe only after checking that DST does not have the jobs.
`job transfer --release T [--release T ...] [WS]` discards holds whose jobs
DST has; `job transfer --hold (--job JOB | --kind STATE) ... SRC DST` only
holds, recording DST as given (the first leg a client runs on a remote SRC)."""


def build_transfer_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``job transfer`` verb: move jobs between two workspace names or directories."""

    transfer = _leaf(
        subparsers,
        "transfer",
        summary="move jobs between two registered workspace names or workspace directories",
        description=_TRANSFER_DESCRIPTION,
        handler=handle_transfer,
    )
    transfer.add_argument(
        "source", metavar="SRC", nargs="?", help="the workspace the jobs leave (with --release: the one holding it)"
    )
    transfer.add_argument("destination", metavar="DST", nargs="?", help="the workspace the jobs arrive in")
    transfer.add_argument(
        "--job",
        action="append",
        default=[],
        dest="jobs",
        metavar="JOB",
        help="a job UUID, key, unique prefix or path in SRC; a remote SRC requires canonical job UUIDs (repeatable)",
    )
    transfer.add_argument(
        "--kind",
        action="append",
        default=[],
        metavar="STATE",
        choices=_kernel.UNOWNED_STATES,
        help="select every job of SRC in this state (repeatable; combinable with --job)",
    )
    transfer.add_argument("--placement", metavar="PREFIX", help="with --kind: only jobs under this placement prefix")
    transfer.add_argument("--tree", action="store_true", help="move each job's descendants with it")
    transfer.add_argument("--resume", action="store_true", help="re-drive the held transfers of SRC (to DST)")
    transfer.add_argument(
        "--release",
        action="append",
        default=[],
        metavar="TRANSFER_ID",
        help="discard a hold whose jobs DST already has (repeatable)",
    )
    transfer.add_argument(
        "--hold",
        action="store_true",
        help="only hold the jobs in SRC, recording DST as given (the first leg, as a client drives it remotely)",
    )
    transfer.add_argument(
        "--destination-id",
        metavar="WORKSPACE_ID",
        help="with --hold: the destination workspace's id, recorded so that the transfer adopts only there",
    )
    transfer.add_argument("-q", "--quiet", action="store_true", help="print no progress lines to stderr")
    _add_adapter_timeout(transfer)
    transfer.add_argument("--json", action="store_true", help="print the outcomes as one JSON document")
    add_durability_arguments(transfer)


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
