"""Remote and transfer command groups.

``job transfer`` moves jobs between workspaces as hold + copy + adopt + release
on :mod:`httk.workflow._moving`, locally or through the remote adapters'
``invoke``, ``push`` and ``pull``; ``transfer status`` lists held transfers.
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from copy import copy
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.cli import CLIContext

from .. import _fs, _kernel, _moving
from .._bundles import BundleError
from .._util import write_json_atomic
from ..adapters import (
    REMOTE_JOB_ADOPT_COMMAND,
    REMOTE_JOB_EJECT_COMMAND,
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
from ..introspection import resolve_job_selectors
from ..models import WORKSPACE_DIRECTORY, canonical_uuid
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

    @property
    def plain(self) -> str:
        assert self.binding is not None
        return self.binding.name.split(":", 1)[1]


def _end(value: str | None, context: CLIContext, arguments: argparse.Namespace, *, probe: bool = False) -> _End:
    """Resolve one end; *value* ``None`` is the enclosing or default workspace. *probe* learns a remote's root."""

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
        return _End(str(workspace.root), workspace=workspace)
    root = None
    if probe:
        target = resolve_remote(reference.remote, project=context.cwd)
        _, root = probe_remote_workspace(target, reference.name.split(":", 1)[1], timeout=arguments.adapter_timeout)
    return _End(reference.name, binding=reference, root=root)


def _remote(end: _End, context: CLIContext, arguments: argparse.Namespace, argv: Sequence[str]) -> tuple[int, str, str]:
    assert end.binding is not None
    return remote_workspace_output(end.binding, context, argv, timeout=arguments.adapter_timeout)


def _adapter(
    end: _End, context: CLIContext, arguments: argparse.Namespace, operation: str, source: str, destination: str
) -> None:
    assert end.binding is not None
    target = resolve_remote(end.binding.remote, project=context.cwd)
    run_adapter(
        target.bundle,
        operation,
        {"source": source, "destination": destination},
        timeout=arguments.adapter_timeout,
    )


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
) -> _moving.EjectReport | str:
    """Claim and eject one job, or hold it when *destination* is ``None``: the report, or why it cannot move now.

    :param workspace: The workspace.
    :param owner: The ejecting owner.
    :param ref: The job.
    :param destination: Where the bundle goes; ``None`` holds it in ``transfers/outgoing/``.
    :param tree: Move the job's descendants too.
    :param locator: The transfer destination a hold records.
    :return: The report, or the reason; a job that cannot move stays where it is (as after any raised error).
    """

    root = _kernel.claim(workspace, owner, ref)
    if root is None:
        return f"{ref.job_key} moved before it could be claimed"
    try:
        if destination is not None:
            return _moving.eject(workspace, owner, root, destination=destination, tree=tree)
        return _moving.hold(workspace, owner, root, tree=tree, destination_locator=locator)
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


def _hold_record(hold: _moving.Hold, now: float) -> dict[str, object]:
    manifest = hold.manifest
    return {
        "transfer_id": hold.transfer_id,
        "path": str(hold.path),
        "root": manifest.members[0].job_key,
        "members": [member.job_key for member in manifest.members],
        "destination": manifest.destination_locator,
        "created_at": manifest.created_at,
        "age_seconds": max(0, int(now - datetime.fromisoformat(manifest.created_at).timestamp())),
    }


def _checked_hold(value: object) -> dict[str, Any]:
    """Validate one hold a remote reported: its transfer id, a path ending in it, and its members."""

    if not isinstance(value, dict):
        raise ValueError("the remote reported a malformed hold")
    transfer_id, path, members = value.get("transfer_id"), value.get("path"), value.get("members")
    if (
        not isinstance(transfer_id, str)
        or not _TRANSFER_ID.fullmatch(transfer_id)
        or not isinstance(path, str)
        or PurePosixPath(path).name != transfer_id
        or not isinstance(members, list)
        or not all(isinstance(member, str) for member in members)
    ):
        raise ValueError(f"the remote reported a malformed hold: {value!r}")
    destination = value.get("destination")
    return {**value, "destination": destination if isinstance(destination, str) else None}


def _holds(src: _End, context: CLIContext, arguments: argparse.Namespace) -> list[dict[str, Any]]:
    """The holds of *src*, as ``transfer status --json`` lists them."""

    if src.workspace is not None:
        now = time.time()
        return [_hold_record(hold, now) for hold in _moving.held(src.workspace)]
    code, out, err = _remote(src, context, arguments, [*REMOTE_TRANSFER_STATUS_COMMAND, "--json", src.plain])
    if code:
        raise RuntimeError(f"transfer status failed on {src.locator}: {err.strip()}")
    document = json.loads(out)
    if not isinstance(document, dict) or not isinstance(document.get("holds"), list):
        raise ValueError(f"{src.locator} returned an invalid transfer status")
    return [_checked_hold(hold) for hold in document["holds"]]


def _hold_jobs(
    src: _End, dst: _End, context: CLIContext, arguments: argparse.Namespace, errors: list[str]
) -> list[dict[str, Any]]:
    """Step 1: hold every selected job on *src*, recording *dst*; a job that cannot move is an error."""

    holds: list[dict[str, Any]] = []
    if src.workspace is not None:
        workspace = src.workspace
        require_cli_modifiable(workspace)
        refs = resolve_job_selectors(workspace, context.cwd, arguments.jobs)
        with _owner(workspace, "job transfer") as owner:
            for ref in refs:
                if any(ref.job_key in hold["members"] for hold in holds):
                    continue  # it went with an earlier selected tree
                try:
                    outcome = eject_once(workspace, owner, ref, None, tree=arguments.tree, locator=dst.locator)
                except _ERRORS as exc:
                    outcome = f"{ref.job_key}: {exc}"
                if isinstance(outcome, str):
                    errors.append(outcome)
                else:
                    holds.append(
                        {
                            "transfer_id": outcome.transfer_id,
                            "path": str(outcome.destination),
                            "members": list(outcome.members),
                        }
                    )
        return holds
    for selector in arguments.jobs:
        try:
            canonical_uuid(selector, "JOB")
        except (WorkflowError, ValueError) as exc:
            raise ValueError(f"a remote source requires canonical job ids: {selector!r}") from exc
    for selector in arguments.jobs:
        argv = [*REMOTE_JOB_EJECT_COMMAND, "--hold", "--json", "--workspace", src.plain]
        argv += [*(["--tree"] if arguments.tree else []), "--", selector, dst.locator]
        code, out, err = _remote(src, context, arguments, argv)
        if code:
            errors.append(f"{selector}: {err.strip() or f'job eject failed on {src.locator}'}")
            continue
        try:
            holds.append(_checked_hold(json.loads(out)))
        except ValueError:
            # The hold may exist: the pass over the source's holds, or a later --resume, drives it.
            errors.append(f"{selector}: job eject --hold on {src.locator} printed no hold document: {out[:200]!r}")
    return holds


class _Refused(Exception):
    """The destination refused the bundle; the message says why."""


def _adopt_into(
    src: _End,
    workspace: Workspace,
    owner: _kernel.Owner,
    hold: Mapping[str, Any],
    context: CLIContext,
    arguments: argparse.Namespace,
) -> dict[str, Any] | None | Exception:
    """Adopt a hold into a local *workspace*; an error is returned, so that the owner closes cleanly."""

    transfer_id, path = str(hold["transfer_id"]), str(hold["path"])
    landing = None
    try:
        if src.workspace is not None:
            # One filesystem: adoption moves the hold in, and a refusal moves it back. Across: it copies.
            source = Path(path)
        else:
            landing = owner.scratch("landing")
            source = landing / transfer_id
            _adapter(src, context, arguments, "pull", path, str(source))
        report = _moving.adopt(workspace, owner, source, untrusted=False)
    except BundleError as exc:
        return _Refused(str(exc))
    except _ERRORS as exc:
        return exc
    finally:
        if landing is not None:
            owner.discard_scratch(landing)
    return None if report is None else adopt_document(report)


def _deliver(
    src: _End, dst: _End, hold: Mapping[str, Any], context: CLIContext, arguments: argparse.Namespace
) -> dict[str, Any] | None:
    """Steps 2 and 3: copy the held bundle to *dst* and adopt it there; ``None`` when another actor took it."""

    transfer_id, path = str(hold["transfer_id"]), str(hold["path"])
    if dst.workspace is not None:
        require_cli_modifiable(dst.workspace)
        with _owner(dst.workspace, "job transfer") as owner:
            result = _adopt_into(src, dst.workspace, owner, hold, context, arguments)
        if isinstance(result, Exception):
            raise result
        return result
    assert dst.root is not None
    incoming = PurePosixPath(
        dst.root, WORKSPACE_DIRECTORY, "transfers", "incoming", f"{transfer_id}.{_fs.fresh_token()}"
    )
    if src.workspace is not None:
        _adapter(dst, context, arguments, "push", path, str(incoming))
    else:
        with tempfile.TemporaryDirectory(prefix="httk-transfer-") as relay:
            _adapter(src, context, arguments, "pull", path, f"{relay}/{transfer_id}")
            _adapter(dst, context, arguments, "push", f"{relay}/{transfer_id}", str(incoming))
    code, out, err = _remote(
        dst, context, arguments, [*REMOTE_JOB_ADOPT_COMMAND, "--json", "--workspace", dst.plain, str(incoming)]
    )
    if code:
        raise _Refused(f"{err.strip()} (the refused copy stays at {incoming} on {dst.locator})")
    document = json.loads(out)
    if not isinstance(document, dict) or not isinstance(document.get("already_adopted"), bool):
        raise ValueError(f"{dst.locator} returned an invalid adoption report")
    return document


def _release(src: _End, transfer_id: str, context: CLIContext, arguments: argparse.Namespace) -> None:
    """Step 4: discard the hold on *src*; one already gone (adopted in on one filesystem) is fine."""

    if src.workspace is not None:
        with _owner(src.workspace, "job transfer") as owner:
            _moving.release_hold(src.workspace, owner, transfer_id)
        return
    code, _, err = _remote(src, context, arguments, [*REMOTE_JOB_TRANSFER_COMMAND, "--release", transfer_id, src.plain])
    if code not in (0, 1):
        raise RuntimeError(err.strip() or f"job transfer --release failed on {src.locator}")


def _drive(
    src: _End, dst: _End, hold: Mapping[str, Any], context: CLIContext, arguments: argparse.Namespace
) -> dict[str, Any]:
    """Copy, adopt and release one hold: the outcome. A hold that was not adopted stays held on *src*."""

    transfer_id = str(hold["transfer_id"])
    outcome: dict[str, Any] = {"transfer_id": transfer_id, "members": hold["members"], "destination": dst.locator}
    try:
        document = _deliver(src, dst, hold, context, arguments)
    except _Refused as exc:
        return {**outcome, "status": "refused", "message": f"bundle refused: {exc}; it stays held on {src.locator}"}
    except _ERRORS as exc:
        return {**outcome, "status": "failed", "message": f"{exc}; it stays held on {src.locator}"}
    if document is None:
        return {**outcome, "status": "taken", "message": "another actor took the held bundle first"}
    outcome.update(document, status="already_adopted" if document["already_adopted"] else "adopted")
    try:
        _release(src, transfer_id, context, arguments)
    except _ERRORS as exc:
        outcome["message"] = (
            f"the jobs are in {dst.locator}, but the hold was not released: {exc}; "
            f"run `httk job transfer --resume` or `httk job transfer --release {transfer_id}`"
        )
    return outcome


def _report(outcomes: list[dict[str, Any]], errors: list[str], *, json_output: bool) -> int:
    missing = sorted({workflow for outcome in outcomes for workflow in outcome.get("missing_workflows", ())})
    if missing:
        print(f"warning: these workflows are not installed, so their jobs wait: {', '.join(missing)}", file=sys.stderr)
    for error in errors:
        print(error, file=sys.stderr)
    if json_output:
        print(json.dumps({"transfers": outcomes, "errors": errors}, indent=2, sort_keys=True))
    else:
        if not outcomes and not errors:
            print("no held transfers to drive")
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
    if arguments.destination is not None or arguments.jobs or arguments.resume:
        raise ValueError("--release takes one transfer id and at most the workspace holding it")
    if not _TRANSFER_ID.fullmatch(arguments.release):
        raise ValueError(f"not a transfer id: {arguments.release!r}")
    src = _end(arguments.source, context, arguments)
    if src.workspace is None:
        code, out, err = _remote(
            src, context, arguments, [*REMOTE_JOB_TRANSFER_COMMAND, "--release", arguments.release, src.plain]
        )
        sys.stdout.write(out)
        sys.stderr.write(err)
        return code
    with _owner(src.workspace, "job transfer --release") as owner:
        released = _moving.release_hold(src.workspace, owner, arguments.release)
    if not released:
        print(f"{arguments.release}: no such hold in {src.locator}", file=sys.stderr)
        return 1
    print(f"released {arguments.release}")
    return 0


def handle_transfer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move jobs between two workspaces: hold, copy, adopt, then release (see the verb's help).

    :param arguments: The parsed arguments.
    :param context: The CLI invocation.
    :return: 0 when every transfer was adopted (or already was), else 1.
    """

    if arguments.release is not None:
        return _handle_release(arguments, context)
    if not arguments.resume and (arguments.source is None or arguments.destination is None or not arguments.jobs):
        raise ValueError("name jobs with --job, and SRC and DST; or re-drive held transfers with --resume")
    if arguments.resume and arguments.jobs:
        raise ValueError("--resume re-drives held transfers and takes no --job")
    src = _end(arguments.source, context, arguments)
    ends: dict[str, _End] = {}
    dst = None
    if arguments.destination is not None:
        dst = _end(arguments.destination, context, arguments, probe=True)
        if dst.locator == src.locator:
            raise ValueError(f"SRC and DST are the same workspace: {src.locator}")
        ends[dst.locator] = dst
    errors: list[str] = []
    if arguments.jobs:
        assert dst is not None
        _hold_jobs(src, dst, context, arguments, errors)
    outcomes = []
    # Every run re-drives the holds bound for its destination (with --resume and no DST: every hold).
    for hold in _holds(src, context, arguments):
        locator = hold["destination"]
        if arguments.destination is not None and locator not in ends:
            continue
        if locator is None:
            errors.append(f"{hold['transfer_id']}: the hold records no destination; adopt or release it by hand")
            continue
        if locator not in ends:
            try:
                ends[locator] = _end(locator, context, arguments, probe=True)
            except _ERRORS as exc:
                errors.append(f"{hold['transfer_id']}: cannot reach its destination {locator}: {exc}")
                continue
        outcomes.append(_drive(src, ends[locator], hold, context, arguments))
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
    holds = _holds(src, context, arguments)
    if arguments.json:
        document = {"format": TRANSFER_STATUS_FORMAT, "format_version": 1, "workspace": src.locator, "holds": holds}
        print(json.dumps(document, indent=2, sort_keys=True))
        return 0
    if not holds:
        print(f"no held transfers in {src.locator}")
    for hold in holds:
        age = hold["age_seconds"] // 3600
        destination = hold["destination"] or "-"
        print(f"{hold['transfer_id']}\t{hold['root']}\t{len(hold['members'])} job(s)\tto {destination}\t{age}h old")
    return 0


_TRANSFER_DESCRIPTION = """\
Move jobs between two workspaces: `job transfer --job JOB [--tree] SRC DST`.

Each of SRC and DST is tried first as a registered workspace name (REMOTE:WS
for a workspace on a remote), then as a workspace directory; `./NAME`
addresses a directory unambiguously. Any combination of local and remote ends
works; a remote-to-remote transfer is relayed through this client.

A transfer holds the jobs on SRC (an eject into SRC's own
.httk-workspace/transfers/outgoing/<transfer id>, where the jobs are in
neither workspace), copies the held bundle to DST, adopts it there into the
states it left, and only then releases the hold. A bundle DST refuses (say,
because one of its jobs is already there) stays held on SRC and is reported.

Recovery: delivery is at least once. Every run, and `job transfer --resume
[SRC] [DST]`, first re-drives the holds bound for DST (with --resume and no DST,
every hold, to the destination it records); DST reports "already adopted" for
a bundle it already has, and the hold is released. `transfer status` and
`job why` list held bundles. A transfer abandoned for good is taken back into
SRC with `job adopt SRC/.httk-workspace/transfers/outgoing/<transfer id>`,
which is safe only after checking that DST does not have the jobs.
`job transfer --release T [WS]` discards a hold whose jobs DST has."""


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
    transfer.add_argument("--tree", action="store_true", help="move each job's descendants with it")
    transfer.add_argument("--resume", action="store_true", help="re-drive the held transfers of SRC (to DST)")
    transfer.add_argument("--release", metavar="TRANSFER_ID", help="discard one hold whose jobs DST already has")
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
