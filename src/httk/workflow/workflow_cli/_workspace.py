"""Workspace command group."""

import argparse
import errno
import json
import os
import sys
from contextlib import redirect_stdout
from copy import copy
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from httk.core.cli import CLIContext
from httk.core.identity import configured_operator_identity

from .. import _death, _kernel, _store
from ..adapters import (
    REMOTE_STATUS_COMMAND,
    REMOTE_WORKSPACE_DELETE_COMMAND,
    REMOTE_WORKSPACE_FSCK_COMMAND,
    REMOTE_WORKSPACE_GC_COMMAND,
    REMOTE_WORKSPACE_INIT_COMMAND,
    REMOTE_WORKSPACE_LIST_COMMAND,
    REMOTE_WORKSPACE_MOVE_COMMAND,
    REMOTE_WORKSPACE_SETTINGS_COMMAND,
    REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND,
    resolve_remote,
    run_adapter,
    seed_application_settings,
)
from ..configuration import machine_names
from ..fsck import check_workspace
from ..gc import GC_CATEGORIES, collect_garbage, iter_report_rows
from ..introspection import JOB_STATES, count_jobs, probe_liveness
from ..manifests import require_quiescent_workspace
from ..models import (
    POLICY_KEYS,
    RETENTION_KEYS,
    WORKSPACE_DIRECTORY,
    WorkspacePolicy,
)
from ..projects import discover_project, read_project_section, require_project, write_project_section
from ..registry import (
    LOCAL_REMOTE,
    _update_workspace_path,
    adopt_workspace,
    create_workspace,
    delete_workspace,
    forget_workspace,
    list_workspaces,
    move_project_member,
    register_workspace,
    remove_local_workspace,
    resolve_workspace,
    valid_workspace_name,
)
from ..seals import (
    default_workspace_keys,
    is_workspace_sealed,
    read_seal,
    require_cli_modifiable,
    seal_workspace,
    unseal_workspace,
    verify_workspace_seal,
)
from ..workspace import Workspace, _validate_setting_key, _validate_settings
from ._common import (
    _ERRORS,
    _add_by_path_argument,
    _by_path,
    _durable,
    _group,
    _json_value,
    _leaf,
    _local_root,
    _pairs,
    _remote_workspace_read,
    _resolve_binding,
    add_durability_arguments,
    add_workspace_argument,
    confirm,
)
from ._seal import _default_trusted_keys

# ---------------------------------------------------------------------------
# workspace
# ---------------------------------------------------------------------------


def _workspace_batch(arguments: argparse.Namespace, context: CLIContext, handler: Any) -> int:
    """Run a workspace leaf for each parsed target, retaining the leaf's logic."""

    targets = arguments.workspace
    assert isinstance(targets, list)
    targets = targets or [None]
    multiple = len(targets) > 1
    results: list[object] = []
    failed = False
    for target in targets:
        item = copy(arguments)
        item.workspace = target
        label = target or "default"
        try:
            if getattr(arguments, "json", False):
                output = StringIO()
                with redirect_stdout(output):
                    code = handler(item, context)
                results.append(json.loads(output.getvalue()))
            else:
                if multiple:
                    print(f"== workspace {label} ==")
                code = handler(item, context)
            failed |= code != 0
        except _ERRORS as exc:
            print(f"workspace {label}: {exc}", file=sys.stderr)
            failed = True
    if getattr(arguments, "json", False):
        print(json.dumps(results, indent=2, sort_keys=True))
    return 1 if failed else 0


def _add_workspace_targets(parser: argparse.ArgumentParser, *, help_text: str, required: bool = False) -> None:
    parser.add_argument(
        "workspace",
        metavar="WORKSPACE",
        nargs="+" if required else "*",
        help=f"{help_text} (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )


def add_workspace_init_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace init`."""

    parser.add_argument("workspace", metavar="PATH", nargs="+", help="the local path, or REMOTE:PATH, to initialize")
    parser.add_argument("--name", metavar="NAME", help="the registry name (default: the path basename)")
    parser.add_argument(
        "--setting",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="seed one application setting at creation, e.g. vasp.command=... (repeatable)",
    )
    _add_by_path_argument(parser)
    add_durability_arguments(parser)


def _init_settings(arguments: argparse.Namespace) -> dict[str, object]:
    """Return the application settings ``workspace init --setting`` carried."""

    return {name: _json_value(text, f"setting {name}") for name, text in _pairs(arguments.setting, "a setting")}


def handle_workspace_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Initialize a local path or ask its owning machine to do so."""

    if isinstance(arguments.workspace, list):
        if arguments.name and len(arguments.workspace) != 1:
            raise ValueError("workspace init --name requires exactly one PATH")
        return _workspace_batch(arguments, context, handle_workspace_init)
    settings = _init_settings(arguments)
    if _by_path(arguments):
        workspace = Workspace.initialize(
            arguments.workspace,
            durable=_durable(arguments),
        )
        for key, value in settings.items():
            workspace.set_setting(key, value)
        print(workspace.root)
        return 0
    remote, separator, remote_path = arguments.workspace.partition(":")
    remote_form = bool(separator) and remote not in machine_names()
    if remote_form:
        if not remote_path:
            raise ValueError("remote workspace init requires a path after REMOTE:")
        target = resolve_remote(remote, project=context.cwd)
        name = arguments.name or Path(remote_path).name
        valid_workspace_name(name)
        argv = list(REMOTE_WORKSPACE_INIT_COMMAND)
        if arguments.name:
            argv += ["--name", name]
        merged = {**seed_application_settings(target.bundle), **settings}
        for key, value in merged.items():
            argv += ["--setting", f"{key}={json.dumps(value)}"]
        argv.append(remote_path)
        result = run_adapter(target.bundle, "invoke", {"argv": argv})
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace init failed: {result.get('stderr', '')}")
        print(name)
        return 0

    local_path = remote_path if separator and remote in machine_names() else arguments.workspace
    root = (Path(context.cwd) / local_path).resolve()
    name = arguments.name or root.name
    valid_workspace_name(name)
    format_path = root / WORKSPACE_DIRECTORY / "format.json"
    if format_path.exists():
        if settings:
            raise ValueError("existing workspace adopted unchanged; use `workspace settings set`")
        Workspace(root)
        created = register_workspace(name, root, durable=_durable(arguments))
    else:
        created = create_workspace(name, root, durable=_durable(arguments), settings=settings)
    print(created.name)
    project = discover_project(root)
    if project is not None:
        section = read_project_section(project, "workspace")
        if section.get("default") is None:
            section["default"] = created.name
            write_project_section(project, "workspace", section)
            print(f"recorded {created.name} as the default workspace of the project at {project}")
    return 0


def add_workspace_status_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace status`."""

    add_workspace_argument(parser, help_text="the workspace to summarize")
    parser.add_argument("--json", action="store_true", help="print the machine-readable status document")
    _add_by_path_argument(parser)
    add_durability_arguments(parser)


def _epoch_text(value: object) -> str | None:
    """Render an epoch second from ``owner.json`` as UTC ISO text, or ``None`` when absent or malformed."""

    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(value, UTC).isoformat(timespec="seconds")


def _owner_rows(workspace: Workspace, *, kind: str | None, tombstoning: bool) -> list[dict[str, object]]:
    """Describe the owners of a workspace, each with its liveness.

    :param workspace: The workspace.
    :param kind: List only the owners of this kind, or every owner when ``None``.
    :param tombstoning: Probe with :func:`httk.workflow._kernel.probe_owner`, which writes the tombstone of an
        owner it proves dead; otherwise probe read-only.
    :return: One row per owner, by owner id.
    """

    scheduler, here = _death.SchedulerQueries(), _death.process_identity()
    rows: list[dict[str, object]] = []
    for owner in _kernel.list_owners(workspace):
        record = owner.record or {}
        if kind is not None and record.get("kind") != kind:
            continue
        tombstone_by = None if owner.tombstone is None else owner.tombstone.get("by")
        if owner.tombstone is not None:
            liveness = "dead"
        elif owner.record is None:
            # Neither owner.json nor dead.json: a clean close in progress, or an unreadable record.
            liveness = "unknown"
        elif tombstoning:
            try:
                liveness = _kernel.probe_owner(workspace, owner.owner_id, scheduler=scheduler).value
            except ValueError:
                liveness = "alive"  # the owner is this very process
            tombstone_by = "probe" if liveness == "dead" else None
        else:
            verdict, _evidence = _death.probe(owner.path, visibility_deadline=0.0, scheduler=scheduler, here=here)
            liveness = verdict.value
        rows.append(
            {
                "owner_id": owner.owner_id,
                "kind": record.get("kind"),
                "label": record.get("label"),
                "hostname": record.get("hostname"),
                "pid": record.get("pid"),
                "started_at": record.get("started_at"),
                "end_time": _epoch_text(record.get("end_time")),
                "drain_start": _epoch_text(record.get("drain_start")),
                "liveness": liveness,
                "tombstone_by": tombstone_by,
            }
        )
    return rows


def _print_owner_rows(rows: list[dict[str, object]]) -> None:
    """Print one tab-separated line per owner row."""

    for row in rows:
        liveness = row["liveness"] if row["tombstone_by"] is None else f"dead (tombstone by {row['tombstone_by']})"
        line = (
            f"{row['owner_id']}\t{row['kind'] or '-'}\t{liveness}\t{row['hostname'] or '-'}\tpid={row['pid'] or '-'}\t"
            f"started={row['started_at'] or '-'}\tlabel={row['label'] or '-'}"
        )
        for key in ("end_time", "drain_start"):
            if row[key] is not None:
                line += f"\t{key}={row[key]}"
        print(line)


def handle_workspace_status(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Summarize one workspace: its job counts by state, its seal, and its owners with their liveness.

    The owner probe is the tombstone-writing one: an owner proven dead gets its
    ``dead.json`` here, and the next manager tick or ``workspace gc`` recovers it.
    A remote binding is summarized over its adapter, so ``workspace status NAME``
    reads a remote workspace exactly as it reads a local one.
    """

    if isinstance(arguments.workspace, list):
        if _by_path(arguments) and not arguments.workspace:
            raise ValueError("--by-path requires an explicit path")
        return _workspace_batch(arguments, context, handle_workspace_status)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(binding, context, REMOTE_STATUS_COMMAND, arguments, flags=("--json",))
    workspace = Workspace(root, durable=_durable(arguments))
    counts = {state: count for state in JOB_STATES if (count := count_jobs(workspace, state))}
    owners = _owner_rows(workspace, kind=None, tombstoning=True)
    sealed = is_workspace_sealed(workspace)
    if arguments.json:
        print(
            json.dumps(
                {
                    "format": "httk-workflow-status",
                    "format_version": 3,
                    "workspace_id": workspace.workspace_id,
                    "root": str(workspace.root),
                    "workspace_format_version": workspace.format["format_version"],
                    "core_profile": workspace.format["core_profile"],
                    "extensions": sorted(workspace.extensions),
                    "sealed": sealed,
                    "counts": counts,
                    "owners": owners,
                },
                indent=2,
            )
        )
        return 0
    print(f"workspace {workspace.workspace_id}")
    for state, count in counts.items():
        print(f"{state:12s} {count}")
    print(f"sealed: {'yes' if sealed else 'no'}")
    print(f"owners: {len(owners)}")
    _print_owner_rows(owners)
    return 0


def handle_workspace_owners(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the owners registered in one workspace (``workspace managers`` is an alias).

    Each owner is shown with its kind, label, host, process, start, allocation
    end and drain start, and its liveness from the read-only death proof; this
    listing never writes a tombstone (``workspace status`` and ``workspace gc``
    do).
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_owners)
    workspace = Workspace(_local_root(arguments, context, action="list its owners"))
    rows = _owner_rows(workspace, kind=arguments.kind, tombstoning=False)
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if not rows:
        print("no owner is registered in this workspace")
        return 0
    _print_owner_rows(rows)
    return 0


_ATTEST_LIMITATION = (
    "Attesting an owner that is still running can apply a request twice and run work twice: attest only after "
    "confirming that the owner process and every launch it started are gone"
)


def handle_workspace_attest_dead(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Attest, as the operator, that an owner and every launch it started have ended.

    This writes the owner's ``dead.json`` tombstone (``by: operator``); the owner's
    jobs are recovered at the next manager tick or ``httk workspace gc``. The
    read-only death proof runs first: an owner it finds alive (this very process
    included) is refused, and one whose death it cannot decide is refused
    unless ``--force`` is given.
    """

    workspace = Workspace(_local_root(arguments, context, action="attest an owner dead"))
    owner_id = arguments.owner
    owners = {owner.owner_id for owner in _kernel.list_owners(workspace)}
    if owner_id not in owners and not (workspace.jobs / _kernel.OWNED / owner_id).is_dir():
        raise ValueError(f"no owner {owner_id!r} is registered in this workspace, and no job is owned by it")
    verdict, evidence = probe_liveness(workspace, owner_id)
    detail = "; ".join(item.detail for item in evidence) or "no evidence"
    if verdict is _death.Liveness.ALIVE:
        raise ValueError(f"owner {owner_id} is alive ({detail}); it cannot be attested dead")
    if verdict is _death.Liveness.UNKNOWN:
        if not arguments.force:
            raise ValueError(
                f"the death of owner {owner_id} cannot be proven ({detail}); pass --force to attest it anyway. "
                f"{_ATTEST_LIMITATION}"
            )
        print(f"warning: attesting owner {owner_id} without proof ({detail}). {_ATTEST_LIMITATION}", file=sys.stderr)
    operator = arguments.operator
    if operator is None:
        identity = configured_operator_identity()
        operator = None if identity is None else identity.label
    _kernel.attest_dead(
        workspace,
        owner_id,
        by="operator",
        evidence=[{"subject": f"owner {owner_id}", "rule": "operator", "detail": arguments.reason}],
        operator=operator,
        reason=arguments.reason,
    )
    print(f"{owner_id}\tattested dead")
    print("its jobs are recovered at the next manager tick or `httk workspace gc`", file=sys.stderr)
    return 0


def handle_workspace_workflows(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the workflows installed in a workspace, with each one's id, short name, tree digest and summary.

    The summary comes from parsing the installed manifest; the runner is never
    executed. A manifest that fails to parse leaves the listing intact: its error
    text takes the summary column.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_workflows)
    workspace = Workspace(_local_root(arguments, context, action="list its workflows"))
    rows: list[dict[str, object]] = []
    for installed in _store.list_installed(workspace):
        row: dict[str, object] = {
            "workflow": installed.id,
            "name": installed.name,
            "tree_sha256": installed.record.get("tree_sha256"),
            "summary": None,
            "error": None,
        }
        try:
            row["summary"] = installed.provider().summary or None
        except _ERRORS as exc:
            row["error"] = str(exc)
        rows.append(row)
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if not rows:
        print("no workflows are installed in this workspace")
        return 0
    for row in rows:
        print(f"{row['workflow']}\t{row['name']}\t{row['error'] or row['summary'] or '-'}")
    return 0


def handle_workspace_seal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal a workspace: a signed snapshot of the seal digest of every job it holds now.

    Jobs without a seal (unfinished, failed, cancelled, opted out) are recorded
    as unsealed and listed, for information only. An already sealed workspace is
    an error.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_seal)
    workspace = Workspace(_local_root(arguments, context, action="seal it"))
    refs = [ref.strip() for ref in arguments.keys.split(",") if ref.strip()] if arguments.keys else None
    resolved = default_workspace_keys(workspace, refs)
    path, unsealed = seal_workspace(workspace, keys=resolved)
    for ref in unsealed:
        print(f"unsealed job\t{ref.job_id}\t{ref.state}", file=sys.stderr)
    roles = ",".join(str(signature.get("role")) for signature in read_seal(path).signatures)
    print(f"{workspace.root}\tsealed\t{roles}")
    if resolved.missing_roles:
        print(f"warning: no key resolved for seal role(s): {', '.join(resolved.missing_roles)}", file=sys.stderr)
    return 0


def handle_workspace_verify(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Verify a workspace seal and report every job that drifted since the snapshot; drift exits one."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_verify)
    workspace = Workspace(_local_root(arguments, context, action="verify its seal"))
    trusted = _default_trusted_keys(workspace.root, list(arguments.trusted_key))
    verification = verify_workspace_seal(workspace, trusted_keys=trusted)
    if arguments.json:
        print(json.dumps(verification.as_entry("workspace", workspace.workspace_id), indent=2, sort_keys=True))
    else:
        for discrepancy in verification.discrepancies:
            print(f"{discrepancy.kind}\t{discrepancy.path}")
        print(f"{workspace.root}\t{verification.verdict}\t{verification.reason or '-'}")
    return 0 if verification.valid else 1


def handle_workspace_unseal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove a workspace's seal, after confirmation.

    Refused while the enclosing project is still sealed, so a project seal that
    commits to this workspace can never be left dangling.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_unseal)
    workspace = Workspace(_local_root(arguments, context, action="unseal it"))
    if not confirm(f"Unseal the workspace {workspace.workspace_id}?", force=arguments.force):
        return 1
    unseal_workspace(workspace)
    print(f"{workspace.root}\tunsealed")
    return 0


def _policy_value(key: str, text: str) -> object:
    """Parse one command-line policy value as the JSON it denotes."""

    if key.startswith("retention.") and text == "keep":
        return "keep"
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

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_policy_show)
    root = _local_root(arguments, context, action="show its policy")
    return _print_policy(Workspace(root).policy, as_json=arguments.json)


def handle_workspace_policy_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one policy member of a workspace and print the result."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_policy_set)
    root = _local_root(arguments, context, action="change its policy")
    # A retention member is addressed directly so that setting one limit does
    # not require restating the whole object as JSON.
    workspace = Workspace(root)
    require_cli_modifiable(workspace)
    if arguments.key.startswith("retention."):
        member = arguments.key.split(".", 1)[1]
        retention = dict(workspace.policy.retention.as_mapping())
        retention[member] = _policy_value(arguments.key, arguments.value)
        policy = workspace.set_policy({"retention": retention})
    else:
        policy = workspace.set_policy({arguments.key: _policy_value(arguments.key, arguments.value)})
    return _print_policy(policy, as_json=arguments.json)


def handle_workspace_policy_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Restore one policy member's default without removing the required policy section."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_policy_unset)
    defaults = WorkspacePolicy()
    key = arguments.key
    if key in POLICY_KEYS:
        value = defaults.as_mapping()[key]
    elif key.startswith("retention.") and key.split(".", 1)[1] in RETENTION_KEYS:
        value = getattr(defaults.retention, key.split(".", 1)[1])
    else:
        raise ValueError(f"unknown policy key: {key}")
    item = copy(arguments)
    item.value = json.dumps(value)
    return handle_workspace_policy_set(item, context)


def handle_workspace_fsck(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Check a workspace's job tree, which no other actor may use meanwhile; ``--repair`` asks first."""

    if isinstance(arguments.workspace, list):
        if arguments.repair and not arguments.workspace:
            raise ValueError("workspace fsck repair requires at least one WORKSPACE")
        return _workspace_batch(arguments, context, handle_workspace_fsck)
    if arguments.repair:
        prompt = "Make sure no other operations are ongoing in this workspace. Continue?"
        if not confirm(prompt, force=arguments.yes, flag="--yes", declined="not repaired"):
            return 1
        arguments.yes = True  # answered here: a remote repair runs without asking again
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            REMOTE_WORKSPACE_FSCK_COMMAND,
            arguments,
            flags=("--repair", "--yes", "--json"),
        )
    report = check_workspace(Workspace(root), repair=arguments.repair)
    if arguments.json:
        print(json.dumps(report.as_mapping(), indent=2, sort_keys=True))
    else:
        for finding in report.findings:
            print(f"{finding.action}\t{finding.problem}\t{finding.job_key or '-'}\t{finding.entry}\t{finding.detail}")
        print(f"checked {report.jobs_checked} jobs, {len(report.findings)} findings")
    # A clean workspace and a fully repaired one both exit zero; anything an
    # operator still has to deal with exits one, as a check command should.
    return 1 if report.unresolved else 0


def handle_workspace_gc(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Collect what the workspace retention policy allows, as one CLI owner; dead owners are recovered first."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_gc)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            REMOTE_WORKSPACE_GC_COMMAND,
            arguments,
            flags=("--dry-run", "--json"),
            tail=[item for category in arguments.category or () for item in ("--category", category)],
        )
    workspace = Workspace(root)
    report = collect_garbage(workspace, dry_run=arguments.dry_run, categories=arguments.category or None)
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


def handle_workspace_exchange_enable(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Enable the exchange extension of a workspace, or report that it is enabled already."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_exchange_enable)
    from .._exchange import enable_exchange, exchange_directory

    workspace = Workspace(_local_root(arguments, context, action="enable its exchange"), durable=_durable(arguments))
    require_cli_modifiable(workspace)
    directory = exchange_directory(workspace)
    if not enable_exchange(workspace):
        print(f"{workspace.root}	exchange already enabled	{directory}")
        return 0
    print(f"{workspace.root}	exchange enabled	{directory}")
    print(
        "every manager on this workspace must now confine its attempts (manager.confine=bwrap); "
        f"clients write ejected jobs into {directory / 'inbox'} and adopt finished ones from {directory / 'outbox'}",
        file=sys.stderr,
    )
    return 0


def handle_workspace_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the registered workspaces and where each resolves to."""

    if arguments.remote is not None:
        remote_name, separator, empty = arguments.remote.partition(":")
        if not separator or empty:
            raise ValueError("workspace list expects REMOTE:")
        target = resolve_remote(remote_name, project=context.cwd)
        result = run_adapter(target.bundle, "invoke", {"argv": [*REMOTE_WORKSPACE_LIST_COMMAND, "--json"]})
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace list failed: {result.get('stderr', '')}")
        remote_rows = json.loads(str(result.get("stdout", "[]")))
        remote_display_rows = [dict(row) for row in remote_rows]
        for row in remote_display_rows:
            row["name"] = f"{remote_name}:{row['name']}"
        if arguments.json:
            print(json.dumps(remote_display_rows, indent=2, sort_keys=True))
        else:
            for row in remote_display_rows:
                print(f"{row['name']}\t{row.get('path', '')}")
        return 0

    rows: list[dict[str, object]] = []
    for binding in list_workspaces():
        assert binding.path is not None
        reachable = (Path(binding.path) / WORKSPACE_DIRECTORY / "format.json").is_file()
        rows.append(
            {
                "name": binding.name,
                "path": binding.path,
                "reachable": reachable,
            }
        )
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if not rows:
        print("no workspaces are registered; create one with `httk workspace init`")
        return 0
    for row in rows:
        mark = "?" if row["reachable"] is None else ("ok" if row["reachable"] else "missing")
        print(f"{row['name']}\t{row['path']}\t{mark}")
    return 0


def handle_workspace_forget(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Deregister one workspace name, leaving the workspace itself in place."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_forget)
    binding = forget_workspace(arguments.workspace)
    print(f"forgot {binding.name}")
    return 0


def handle_workspace_delete(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Destroy a registered workspace and deregister it.

    Destruction is irreversible, so it is refused without ``--force``.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_delete)
    if _by_path(arguments):
        # The protocol form one machine runs on another: destroy the workspace at
        # a literal path, with no registry involved.
        if not arguments.force:
            raise ValueError("workspace delete requires --force")
        remove_local_workspace(Path(arguments.workspace))
        print(arguments.workspace)
        return 0
    binding = resolve_workspace(arguments.workspace, project=context.cwd)
    if binding.remote != LOCAL_REMOTE and not arguments.force:
        raise ValueError("workspace delete requires --force")
    if binding.remote != LOCAL_REMOTE:
        target = resolve_remote(binding.remote, project=context.cwd)
        name = binding.name.split(":", 1)[1]
        result = run_adapter(
            target.bundle,
            "invoke",
            {"argv": [*REMOTE_WORKSPACE_DELETE_COMMAND, "--force", name]},
        )
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace delete failed: {result.get('stderr', '')}")
        print(f"deleted {name}")
        return 0
    delete_workspace(arguments.workspace, force=arguments.force)
    print(f"deleted {binding.name}")
    return 0


def handle_workspace_default(arguments: argparse.Namespace, context: CLIContext) -> int:
    project = require_project(context.cwd)
    section = read_project_section(project, "workspace")
    if arguments.unset:
        if arguments.workspace is not None:
            raise ValueError("workspace default --unset does not take a NAME")
        section.pop("default", None)
        write_project_section(project, "workspace", section)
        return 0
    if arguments.workspace is None:
        recorded = section.get("default")
        if isinstance(recorded, str):
            print(recorded)
        else:
            print("none recorded; the per-user default applies")
        return 0
    resolve_workspace(arguments.workspace, project=project)
    section["default"] = arguments.workspace
    write_project_section(project, "workspace", section)
    print(arguments.workspace)
    return 0


def handle_workspace_move(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move a local workspace within its filesystem and update its registry path.

    Refused unless every owner of the workspace is proven dead: a running
    manager or CLI owner keeps the workspace in use. A remote binding moves over
    its adapter.
    """
    binding = resolve_workspace(arguments.workspace, project=context.cwd)
    if binding.remote != LOCAL_REMOTE:
        target = resolve_remote(binding.remote, project=context.cwd)
        name = binding.name.split(":", 1)[1]
        result = run_adapter(
            target.bundle, "invoke", {"argv": [*REMOTE_WORKSPACE_MOVE_COMMAND, name, arguments.destination]}
        )
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace move failed: {result.get('stderr', '')}")
        sys.stdout.write(str(result.get("stdout", "")))
        return 0
    assert binding.path is not None
    destination = (Path(context.cwd) / arguments.destination).resolve()
    if os.path.lexists(destination):
        raise ValueError(f"workspace move destination already exists: {destination}")
    try:
        require_quiescent_workspace(Workspace(binding.path))
    except ValueError as exc:
        raise ValueError(f"cannot move the workspace while it is in use: {exc}") from exc
    try:
        # ponytail: check-then-rename; an empty directory created at the destination in between is replaced
        # (rename(2) replaces only an empty one), anything else that appeared refuses the move below.
        os.rename(binding.path, destination)
    except OSError as exc:
        if exc.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.ENOTDIR, errno.EISDIR):
            raise ValueError(f"workspace move destination appeared during the move: {destination}") from exc
        if exc.errno != errno.EXDEV:
            raise
        raise ValueError(
            "workspace move must stay within one filesystem; stop managers, copy the workspace manually, "
            "then forget the old name and run `workspace init --name NAME <newpath>`"
        ) from exc
    updated = _update_workspace_path(binding.name, destination, durable=_durable(arguments))
    move_project_member(Path(binding.path), destination)
    print(updated.path)
    return 0


def handle_workspace_settings_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show one workspace's application settings, or one named member."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_settings_show)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_SETTINGS_COMMAND, "show"),
            arguments,
            flags=("--json",),
            tail=() if arguments.key is None else ("--key", arguments.key),
        )
    workspace = Workspace(root)
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


def handle_workspace_configure(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove and set application settings, validating local changes before writing."""

    if not arguments.settings and not arguments.unset:
        raise ValueError("workspace configure requires --set KEY=VALUE or --unset KEY")
    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_configure)
    pairs = _pairs(arguments.settings or (), "--set")
    changes = {key: _json_value(value, f"setting {key}") for key, value in pairs}
    _validate_settings(changes)
    for key in changes:
        Workspace._check_setting_collision(key, changes)
    for key in arguments.unset or ():
        _validate_setting_key(key)
    _binding, root = _resolve_binding(arguments, context)
    if root is None:
        # Existing remote peers expose individual settings operations. Stop on
        # the first error; earlier remote changes have already been applied.
        for key in arguments.unset or ():
            item = copy(arguments)
            item.key = key
            item.json = False
            code = handle_workspace_settings_unset(item, context)
            if code:
                return code
        for key, value in pairs:
            item = copy(arguments)
            item.key, item.value = key, value
            item.json = False
            with redirect_stdout(StringIO()):
                code = handle_workspace_settings_set(item, context)
            if code:
                return code
    else:
        workspace = Workspace(root, durable=_durable(arguments))
        require_cli_modifiable(workspace)
        workspace.configure_settings(changes, unset=arguments.unset or ())
    item = copy(arguments)
    item.key = None
    return handle_workspace_settings_show(item, context)


def handle_workspace_settings_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one application setting on a workspace."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_settings_set)
    value = _json_value(arguments.value, f"setting {arguments.key}")
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_SETTINGS_COMMAND, "set"),
            arguments,
            flags=("--durable", "--no-durable"),
            tail=("--key", arguments.key, "--value", arguments.value),
        )
    workspace = Workspace(root, durable=_durable(arguments))
    require_cli_modifiable(workspace)
    settings = workspace.set_setting(arguments.key, value)
    print(json.dumps(settings[arguments.key], sort_keys=True))
    return 0


def handle_workspace_settings_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one application setting from a workspace."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_settings_unset)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_SETTINGS_COMMAND, "unset"),
            arguments,
            flags=("--durable", "--no-durable"),
            tail=("--key", arguments.key),
        )
    workspace = Workspace(root, durable=_durable(arguments))
    require_cli_modifiable(workspace)
    workspace.unset_setting(arguments.key)
    return 0


def _prelude_value(text: str) -> str:
    """Return a per-workflow prelude value, reading ``@file`` from disk.

    A leading ``@`` reads the shell text from that file, so an operator can keep
    a multi-line module-load script on disk instead of quoting it on the command
    line; any other text is stored literally. Unlike :func:`_json_value` this
    never parses the payload as JSON, which would corrupt a shell script.

    :param text: The ``VALUE`` argument, either literal text or ``@PATH``.
    :return: The shell text to store.
    """

    if text.startswith("@"):
        return Path(text[1:]).expanduser().read_text(encoding="utf-8")
    return text


def handle_workspace_workflow_prelude_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show one workspace's per-workflow preludes, or one workflow's prelude.

    :param arguments: The parsed ``workflow-prelude show`` arguments.
    :param context: The invocation context.
    :return: The process exit status.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_workflow_prelude_show)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND, "show"),
            arguments,
            flags=("--json",),
            tail=() if arguments.workflow is None else ("--workflow", arguments.workflow),
        )
    workspace = Workspace(root)
    preludes = workspace.read_workflow_preludes()
    if arguments.workflow is not None:
        if arguments.workflow not in preludes:
            raise ValueError(f"no per-workflow prelude is set for workflow: {arguments.workflow}")
        print(preludes[arguments.workflow])
        return 0
    if arguments.json:
        print(json.dumps(preludes, indent=2, sort_keys=True))
        return 0
    for workflow_id in sorted(preludes):
        print(f"{workflow_id}\t{preludes[workflow_id]}")
    return 0


def handle_workspace_workflow_prelude_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one per-workflow prelude on a workspace.

    :param arguments: The parsed ``workflow-prelude set`` arguments.
    :param context: The invocation context.
    :return: The process exit status.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_workflow_prelude_set)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND, "set"),
            arguments,
            flags=("--durable", "--no-durable"),
            tail=("--workflow", arguments.workflow, "--value", arguments.value),
        )
    # Resolve ``@file`` only for a local store: a remote store forwards the raw
    # argument so the file is read on the far side, as `settings set` forwards.
    value = _prelude_value(arguments.value)
    workspace = Workspace(root, durable=_durable(arguments))
    require_cli_modifiable(workspace)
    preludes = workspace.set_workflow_prelude(arguments.workflow, value)
    print(preludes[arguments.workflow])
    return 0


def handle_workspace_workflow_prelude_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one per-workflow prelude from a workspace.

    :param arguments: The parsed ``workflow-prelude unset`` arguments.
    :param context: The invocation context.
    :return: The process exit status.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_workflow_prelude_unset)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_WORKFLOW_PRELUDE_COMMAND, "unset"),
            arguments,
            flags=("--durable", "--no-durable"),
            tail=("--workflow", arguments.workflow),
        )
    workspace = Workspace(root, durable=_durable(arguments))
    require_cli_modifiable(workspace)
    workspace.unset_workflow_prelude(arguments.workflow)
    return 0


def handle_workspace_adopt(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Register copied workspaces on this machine under their recorded names."""

    paths = arguments.paths or [str(context.cwd)]
    if arguments.name is not None and len(paths) > 1:
        raise ValueError("workspace adopt --name accepts exactly one PATH")
    failed = False
    reports: list[dict[str, object]] = []
    for raw in paths:
        start = (Path(context.cwd) / Path(raw).expanduser()).resolve()
        root = Workspace.discover(start)
        if root is None:
            failed = True
            if arguments.json:
                reports.append({"path": str(start), "error": "not inside a workspace"})
            else:
                print(f"{start}\tERROR\tnot inside a workspace", file=sys.stderr)
            continue
        findings = adopt_workspace(root, name=arguments.name)
        reports.append({"root": str(root), "findings": list(findings)})
        if any(finding["status"] == "error" for finding in findings):
            failed = True
        if not arguments.json:
            for finding in findings:
                print(f"{finding['status']}\t{finding['check']}\t{finding['message']}")
    if arguments.json:
        print(json.dumps(reports if len(paths) > 1 else reports[0] if reports else {}, indent=2, sort_keys=True))
    return 1 if failed else 0


def build_workspace_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    *,
    program: str | None = None,
) -> None:
    """Declare the ``workspace`` group: the workspace itself, not its jobs."""

    _, group = _group(
        subparsers,
        "workspace",
        summary="create, inspect, tune, check, and collect one workspace",
        description="Manage one filesystem-native execution workspace",
        prog=program,
    )

    from .._daemon_cli import add_subcommands, launch

    _, daemon = _group(
        group,
        "daemon",
        summary="set up and run the confined workspace command daemon",
        description="Set up and run the confined workspace command daemon for an exchange enrollment",
    )
    add_subcommands(daemon, handler=lambda arguments, _context: launch(arguments))
    _, exchange_actions = _group(
        group,
        "exchange",
        summary="enable the client exchange directory",
        description="Manage the exchange extension: WORKSPACE/exchange, where clients send and fetch ejected jobs",
    )
    exchange_enable = _leaf(
        exchange_actions,
        "enable",
        summary="create WORKSPACE/exchange and enable the extension",
        description=(
            "Create WORKSPACE/exchange with its inbox and outbox and enable the exchange extension; every "
            "manager of the workspace must then confine its attempts. Enabling an enabled workspace changes nothing"
        ),
        handler=handle_workspace_exchange_enable,
    )
    _add_workspace_targets(exchange_enable, help_text="the workspace whose exchange to enable")
    _add_by_path_argument(exchange_enable)
    add_durability_arguments(exchange_enable)
    add_workspace_init_arguments(
        _leaf(
            group,
            "init",
            summary="initialize an execution workspace",
            description="Initialize one execution workspace",
            handler=handle_workspace_init,
        )
    )
    status = _leaf(
        group,
        "status",
        summary="summarize job counts, the seal, and the owners",
        description=(
            "Summarize one workspace: its job counts by state, whether it is sealed, and its owners with their "
            "liveness; an owner proven dead gets its tombstone, for the next manager tick or workspace gc to recover"
        ),
        handler=handle_workspace_status,
    )
    _add_workspace_targets(status, help_text="the workspace to summarize")
    status.add_argument("--json", action="store_true", help="print the machine-readable status document")
    _add_by_path_argument(status)
    add_durability_arguments(status)
    for name in ("owners", "managers"):
        # ``managers`` is the earlier name, kept as a hidden alias leaf of its own (so its prog names it).
        owners = _leaf(
            group,
            name,
            summary="list the owners (managers, CLI processes, daemons) of this workspace",
            description=(
                "List every owner registered in one workspace with its liveness from the read-only death proof "
                "(managers is an alias)"
            ),
            handler=handle_workspace_owners,
            hidden=name == "managers",
        )
        _add_workspace_targets(owners, help_text="the workspace whose owners to list")
        owners.add_argument("--kind", choices=("manager", "cli", "daemon"), help="list only owners of this kind")
        owners.add_argument("--json", action="store_true", help="print the owners as one JSON array")
    attest = _leaf(
        group,
        "attest-dead",
        summary="attest that an owner and its launches have ended",
        description=(
            "Write the dead.json tombstone of OWNER as the operator, so the next manager tick or workspace gc "
            "recovers its jobs. The death proof runs first: an owner proven alive is refused, and one whose "
            f"death cannot be decided needs --force. {_ATTEST_LIMITATION}"
        ),
        handler=handle_workspace_attest_dead,
    )
    attest.add_argument("owner", metavar="OWNER", help="the owner id, as workspace owners lists it")
    add_workspace_argument(attest, help_text="the workspace the owner belongs to")
    attest.add_argument("--reason", required=True, metavar="TEXT", help="why the owner is known dead, recorded")
    attest.add_argument(
        "--operator", metavar="NAME", help="who attests, recorded (default: the configured operator identity)"
    )
    attest.add_argument(
        "--force",
        action="store_true",
        help="attest an owner whose death the proof cannot decide (an owner proven alive is always refused)",
    )

    workflows = _leaf(
        group,
        "workflows",
        summary="list the workflows installed in this workspace",
        description="List the workflows installed in a workspace, with each one's id, short name and summary",
        handler=handle_workspace_workflows,
    )
    _add_workspace_targets(workflows, help_text="the workspace whose workflows to list")
    workflows.add_argument("--json", action="store_true", help="print the workflows as one JSON array")

    _, policy_actions = _group(
        group,
        "policy",
        summary="show or set the workspace policy",
        description="Show or set the tunables one workspace publishes to every attacher",
    )
    show = _leaf(
        policy_actions,
        "show",
        summary="print the current policy",
        description="Print the policy of one execution workspace",
        handler=handle_workspace_policy_show,
    )
    _add_workspace_targets(show, help_text="the workspace whose policy to print")
    show.add_argument("--json", action="store_true", help="print the policy as one JSON object")
    _add_by_path_argument(show)
    store = _leaf(
        policy_actions,
        "set",
        summary="store one policy member",
        description="Store one member of the policy of an execution workspace",
        handler=handle_workspace_policy_set,
    )
    _add_workspace_targets(store, help_text="the workspace whose policy to change")
    store.add_argument("--key", required=True, metavar="KEY", help="one of " + ", ".join(sorted(POLICY_KEYS)))
    store.add_argument("--value", required=True, metavar="VALUE", help="the JSON value to store")
    store.add_argument(
        "--json",
        action="store_true",
        help="print the resulting policy as one JSON object",
    )
    _add_by_path_argument(store)
    reset = _leaf(
        policy_actions,
        "unset",
        summary="restore one policy member's default",
        description="Restore a policy member or retention.KEY to its default, keeping the policy section",
        handler=handle_workspace_policy_unset,
    )
    _add_workspace_targets(reset, help_text="the workspace whose policy to change")
    reset.add_argument("--key", required=True, metavar="KEY", help="the policy member or retention.KEY to reset")
    reset.add_argument("--json", action="store_true", help="print the resulting policy as one JSON object")
    _add_by_path_argument(reset)

    listing = _leaf(
        group,
        "list",
        summary="list the registered workspaces",
        description="List the registered workspaces and where each name resolves to",
        handler=handle_workspace_list,
    )
    listing.add_argument("remote", metavar="REMOTE:", nargs="?", help="list a remote machine's workspaces")
    listing.add_argument("--json", action="store_true", help="print the registry as one JSON document")

    default = _leaf(
        group,
        "default",
        summary="read or set the project's default name",
        description="Read or record the workspace name this project uses by default",
        handler=handle_workspace_default,
    )
    default.add_argument("workspace", metavar="NAME", nargs="?", help="the workspace name to record")
    default.add_argument("--unset", action="store_true", help="clear the recorded project default")

    adopt = _leaf(
        group,
        "adopt",
        summary="register copied workspaces on this machine",
        description=(
            "Register one or more workspaces from a copied project tree under their recorded "
            "names, join them to the enclosing project, and record any missing name"
        ),
        handler=handle_workspace_adopt,
    )
    adopt.add_argument(
        "paths",
        nargs="*",
        metavar="PATH",
        help="a workspace or a directory inside one (default: the enclosing workspace)",
    )
    adopt.add_argument("--name", metavar="NAME", help="adopt the single PATH under this name")
    adopt.add_argument("--json", action="store_true", help="print the adoption findings as JSON")
    forget = _leaf(
        group,
        "forget",
        summary="deregister a workspace name",
        description="Deregister one workspace name, leaving the workspace itself untouched",
        handler=handle_workspace_forget,
    )
    forget.add_argument("workspace", metavar="NAME", nargs="+", help="the registered workspace name to forget")

    delete = _leaf(
        group,
        "delete",
        summary="destroy a workspace and deregister it",
        description="Destroy a registered workspace and deregister it; refused without --force",
        handler=handle_workspace_delete,
    )
    delete.add_argument("workspace", metavar="NAME", nargs="+", help="the registered workspace to destroy")
    delete.add_argument("--force", action="store_true", help="confirm the irreversible destruction")
    _add_by_path_argument(delete)

    move = _leaf(
        group,
        "move",
        summary="move a workspace and update its registry path",
        description="Move one local workspace to a new path",
        handler=handle_workspace_move,
    )
    move.add_argument("workspace", metavar="NAME", help="the local workspace name")
    move.add_argument("destination", metavar="DEST_DIR", help="the new workspace path")
    add_durability_arguments(move)

    configuration_show = _leaf(
        group,
        "show",
        summary="show a workspace's application configuration",
        description="Print application settings; use workspace policy show for execution policy",
        handler=handle_workspace_settings_show,
    )
    _add_workspace_targets(configuration_show, help_text="the workspace whose settings to read")
    configuration_show.add_argument("--key", metavar="KEY", help="print only this application setting")
    configuration_show.add_argument("--json", action="store_true", help="print the settings as JSON")
    _add_by_path_argument(configuration_show)
    configure = _leaf(
        group,
        "configure",
        summary="set or remove application configuration",
        description=(
            "Remove settings, then store values. Local changes are validated together; remote changes run "
            "as individual settings operations and stop at the first failure"
        ),
        handler=handle_workspace_configure,
    )
    _add_workspace_targets(configure, help_text="the workspace to configure")
    configure.add_argument(
        "--set", dest="settings", action="append", metavar="KEY=VALUE", help="store a setting (repeatable)"
    )
    configure.add_argument(
        "--unset", action="append", metavar="KEY", help="remove a setting before storing values (repeatable)"
    )
    configure.add_argument("--json", action="store_true", help="print the resulting settings as JSON")
    _add_by_path_argument(configure)
    add_durability_arguments(configure)

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
    _add_workspace_targets(settings_show, help_text="the workspace whose settings to read")
    settings_show.add_argument("--key", metavar="KEY", help="print only this setting (default: all of them)")
    settings_show.add_argument("--json", action="store_true", help="print the settings as one JSON object")
    _add_by_path_argument(settings_show)
    settings_set = _leaf(
        settings_actions,
        "set",
        summary="store one application setting",
        description="Store one application setting on a workspace, e.g. vasp.command",
        handler=handle_workspace_settings_set,
    )
    _add_workspace_targets(settings_set, help_text="the workspace to change")
    settings_set.add_argument("--key", required=True, metavar="KEY", help="the dotted setting name, e.g. vasp.command")
    settings_set.add_argument(
        "--value", required=True, metavar="VALUE", help="the JSON value, or a bare string, to store"
    )
    _add_by_path_argument(settings_set)
    add_durability_arguments(settings_set)
    settings_unset = _leaf(
        settings_actions,
        "unset",
        summary="remove one application setting",
        description="Remove one application setting from a workspace",
        handler=handle_workspace_settings_unset,
    )
    _add_workspace_targets(settings_unset, help_text="the workspace to change")
    settings_unset.add_argument("--key", required=True, metavar="KEY", help="the dotted setting name to remove")
    _add_by_path_argument(settings_unset)
    add_durability_arguments(settings_unset)

    _, prelude_actions = _group(
        group,
        "workflow-prelude",
        summary="show or set a workspace's per-workflow shell preludes",
        description="Show or set the per-workflow shell prelude a job's runner sources before it runs",
    )
    prelude_show = _leaf(
        prelude_actions,
        "show",
        summary="print the per-workflow preludes",
        description="Print the per-workflow preludes of one workspace, or one workflow's prelude",
        handler=handle_workspace_workflow_prelude_show,
    )
    _add_workspace_targets(prelude_show, help_text="the workspace whose preludes to read")
    prelude_show.add_argument(
        "--workflow", metavar="WORKFLOW", help="print only this workflow's prelude (default: all)"
    )
    prelude_show.add_argument("--json", action="store_true", help="print the preludes as one JSON object")
    _add_by_path_argument(prelude_show)
    prelude_set = _leaf(
        prelude_actions,
        "set",
        summary="store one per-workflow prelude",
        description="Store one per-workflow shell prelude on a workspace, keyed by workflow id",
        handler=handle_workspace_workflow_prelude_set,
    )
    _add_workspace_targets(prelude_set, help_text="the workspace to change")
    prelude_set.add_argument(
        "--workflow", required=True, metavar="WORKFLOW", help="the workflow id the prelude applies to"
    )
    prelude_set.add_argument(
        "--value",
        required=True,
        metavar="VALUE",
        help="the shell text to store, or @FILE to read it from a file",
    )
    _add_by_path_argument(prelude_set)
    add_durability_arguments(prelude_set)
    prelude_unset = _leaf(
        prelude_actions,
        "unset",
        summary="remove one per-workflow prelude",
        description="Remove one per-workflow prelude from a workspace",
        handler=handle_workspace_workflow_prelude_unset,
    )
    _add_workspace_targets(prelude_unset, help_text="the workspace to change")
    prelude_unset.add_argument(
        "--workflow", required=True, metavar="WORKFLOW", help="the workflow id whose prelude to remove"
    )
    _add_by_path_argument(prelude_unset)
    add_durability_arguments(prelude_unset)

    fsck = _leaf(
        group,
        "fsck",
        summary="check the job tree for what the kernel cannot read",
        description=(
            "Check a workspace's job tree: unparsable names, a job in two places, owned jobs without their owner, "
            "unreadable state.json, jobs another user owns, and stale or missing exchange index entries. Run it "
            "only while no other actor (manager, CLI operation, daemon or transfer) uses the workspace: findings "
            "from a busy workspace may be transient, and --repair is refused unless every owner is proven dead "
            "and recovered"
        ),
        handler=handle_workspace_fsck,
    )
    _add_workspace_targets(fsck, help_text="the workspace to check")
    fsck.add_argument(
        "--repair",
        action="store_true",
        help=(
            "quarantine unparsable entries, remove stale exchange index entries and recreate missing ones, after "
            "confirmation; every other finding is left to the operator"
        ),
    )
    fsck.add_argument("--yes", action="store_true", help="repair without asking for confirmation")
    fsck.add_argument("--json", action="store_true", help="print the findings as one JSON report")
    _add_by_path_argument(fsck)

    collect = _leaf(
        group,
        "gc",
        summary="collect the garbage the retention policy allows",
        description="Collect the garbage one execution workspace has accumulated",
        handler=handle_workspace_gc,
    )
    _add_workspace_targets(collect, help_text="the workspace to collect")
    collect.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be removed without touching it",
    )
    collect.add_argument(
        "--category",
        action="append",
        choices=GC_CATEGORIES,
        help="collect only this category (repeatable, default: every category)",
    )
    collect.add_argument("--json", action="store_true", help="print the collection as one JSON report")
    _add_by_path_argument(collect)

    seal = _leaf(
        group,
        "seal",
        summary="seal a snapshot of the job seals a workspace holds",
        description=(
            "Record the seal digest of every job in a workspace under one signed workspace seal; jobs without a "
            "seal are recorded and listed as unsealed"
        ),
        handler=handle_workspace_seal,
    )
    _add_workspace_targets(seal, help_text="the workspace to seal")
    seal.add_argument(
        "--keys",
        metavar="REFS",
        help="comma-separated seal-key refs to sign with (default: the workspace seal.keys setting)",
    )

    unseal = _leaf(
        group,
        "unseal",
        summary="remove a workspace's seal",
        description="Remove a workspace's seal; refused while the enclosing project is sealed",
        handler=handle_workspace_unseal,
    )
    _add_workspace_targets(unseal, help_text="the workspace to unseal")
    unseal.add_argument("--force", action="store_true", help="skip the confirmation prompt")

    verify = _leaf(
        group,
        "verify",
        summary="verify a workspace seal and report drift",
        description=(
            "Verify a workspace seal's signature and list every job that drifted since the snapshot: "
            "missing_job, unsealed, missing, mismatch; any drift exits one"
        ),
        handler=handle_workspace_verify,
    )
    _add_workspace_targets(verify, help_text="the workspace to verify")
    verify.add_argument("--json", action="store_true", help="print the verdict as one JSON document")
    verify.add_argument(
        "--trusted-key",
        action="append",
        default=[],
        metavar="KEY_OR_FINGERPRINT",
        help="trust this key as well (repeatable); the project's pinned keys and all local identities are trusted",
    )
