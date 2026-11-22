"""Workspace command group."""

import argparse
import errno
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from copy import copy
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any, cast

from httk.core.cli import CLIContext
from httk.core.identity import configured_operator_identity

from .. import _death, _kernel, _moving
from .._bundles import BundleError
from .._setting_catalog import annotate_setting, installed_workflow_settings, report_written_settings, use_color
from ..adapters import (
    REMOTE_STATUS_COMMAND,
    REMOTE_WORKSPACE_CHECK_COMMAND,
    REMOTE_WORKSPACE_DELETE_COMMAND,
    REMOTE_WORKSPACE_GC_COMMAND,
    REMOTE_WORKSPACE_INIT_COMMAND,
    REMOTE_WORKSPACE_LIST_COMMAND,
    REMOTE_WORKSPACE_MOVE_COMMAND,
    REMOTE_WORKSPACE_REPAIR_COMMAND,
    REMOTE_WORKSPACE_SET_COMMAND,
    REMOTE_WORKSPACE_SHOW_COMMAND,
    REMOTE_WORKSPACE_UNSET_COMMAND,
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
    verify_tree,
    verify_workspace_seal,
)
from ..workspace import (
    _RESERVED_SETTING_NAMESPACES,
    Workspace,
    _validate_setting_key,
    _validate_workflow_prelude_id,
)
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
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
    _setting_pair,
    add_durability_arguments,
    add_workspace_argument,
    confirm,
)
from ._seal import _default_trusted_keys, render_seal_entries

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
        help=help_text
        if required
        else f"{help_text} (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )


def add_workspace_init_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace init`."""

    parser.add_argument("workspace", metavar="PATH", help="the local path, or REMOTE:PATH, to initialize")
    parser.add_argument(
        "pairs",
        metavar="KEY=VALUE",
        nargs="*",
        type=_setting_pair,
        help="seed application settings at creation, e.g. vasp.command=...",
    )
    parser.add_argument("--name", metavar="NAME", help="the registry name (default: the path basename)")
    parser.set_defaults(pairs_parser=parser)
    _add_by_path_argument(parser)
    add_durability_arguments(parser)


def _init_settings(arguments: argparse.Namespace) -> dict[str, object]:
    """Return the application settings ``workspace init`` carried; policy and prelude keys are refused."""

    settings: dict[str, object] = {}
    for key, text in _pairs(arguments.pairs, "a setting"):
        if key.partition(".")[0] in _RESERVED_SETTING_NAMESPACES:
            raise ValueError(f"workspace init seeds application settings only; set {key} with `httk workspace set`")
        settings[_validate_setting_key(key)] = _json_value(text, f"setting {key}")
    return settings


def handle_workspace_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Initialize a local path or ask its owning machine to do so."""

    settings = _init_settings(arguments)
    if _by_path(arguments):
        workspace = Workspace.initialize(
            arguments.workspace,
            durable=_durable(arguments),
        )
        for key, value in settings.items():
            workspace.set_setting(key, value)
        report_written_settings(settings)
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
        argv += [remote_path, *(f"{key}={json.dumps(value)}" for key, value in merged.items())]
        result = run_adapter(target.bundle, "invoke", {"argv": argv})
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace init failed: {result.get('stderr', '')}")
        # Relay the far side's own warnings, such as an unrecognized setting.
        sys.stderr.write(str(result.get("stderr") or ""))
        print(name)
        return 0

    local_path = remote_path if separator and remote in machine_names() else arguments.workspace
    root = (Path(context.cwd) / local_path).resolve()
    name = arguments.name or root.name
    valid_workspace_name(name)
    format_path = root / WORKSPACE_DIRECTORY / "format.json"
    if format_path.exists():
        if settings:
            raise ValueError("existing workspace adopted unchanged; use `httk workspace set`")
        Workspace(root)
        created = register_workspace(name, root, durable=_durable(arguments))
    else:
        created = create_workspace(name, root, durable=_durable(arguments), settings=settings)
        report_written_settings(settings)
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


def _held_transfers(workspace: Workspace) -> tuple[list[_moving.Hold], list[str]]:
    """Return the readable held transfers of *workspace* and the transfer ids of those that cannot be read.

    A status summary, which a remote's compatibility probe reads too, must not fail on one damaged hold.
    """

    holds: list[_moving.Hold] = []
    unreadable: list[str] = []
    for name in _moving.held_names(workspace):
        try:
            hold = _moving.held_one(workspace, name)
        except (BundleError, OSError):
            unreadable.append(name)
            continue
        if hold is not None:
            holds.append(hold)
    return holds, unreadable


def handle_workspace_status(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Summarize one workspace: its job counts by state, its seal, its owners with their liveness, and its held
    transfers.

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
    now = time.time()
    holds, unreadable = _held_transfers(workspace)
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
                    "transfers": [hold.as_mapping(now) for hold in holds],
                    "transfers_unreadable": unreadable,
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
    damaged = f" ({len(unreadable)} unreadable: {', '.join(unreadable)})" if unreadable else ""
    print(f"held transfers: {len(holds)}{damaged}")
    for hold in holds:
        age = cast(int, hold.as_mapping(now)["age_seconds"]) // 3600
        destination = hold.destination_locator or "-"
        if hold.destination_workspace_id is not None:
            destination = f"{destination} ({hold.destination_workspace_id})"
        print(f"{hold.transfer_id}\t{hold.members[0]}\t{len(hold.members)} job(s)\tto {destination}\t{age}h old")
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
    """Verify a workspace seal and report every job that drifted since the snapshot; drift exits one.

    With ``--deep`` every job seal the workspace seal records is verified too, and the verdicts and exit status are
    those of ``job verify``; ``--content`` implies it and also checks each file against its sealed uncompressed
    digest.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_verify)
    workspace = Workspace(_local_root(arguments, context, action="verify its seal"))
    trusted = _default_trusted_keys(workspace.root, list(arguments.trusted_key))
    if arguments.deep or arguments.content:
        report = verify_tree(workspace.root, trusted_keys=trusted, content=arguments.content)
        return render_seal_entries(report.entries, as_json=arguments.json)
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
    if not confirm(f"Unseal the workspace {workspace.workspace_id}?", yes=arguments.yes):
        return 1
    unseal_workspace(workspace)
    print(f"{workspace.root}\tunsealed")
    return 0


def handle_workspace_check(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Check a workspace's job tree, which no other actor may use meanwhile; ``workspace repair`` asks first."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_check)
    if arguments.repair:
        prompt = "Make sure no other operations are ongoing in this workspace. Continue?"
        if not confirm(prompt, yes=arguments.yes, declined="not repaired"):
            return 1
        arguments.yes = True  # answered here: a remote repair runs without asking again
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            REMOTE_WORKSPACE_REPAIR_COMMAND if arguments.repair else REMOTE_WORKSPACE_CHECK_COMMAND,
            arguments,
            flags=("--yes", "--json"),
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

    Destruction is irreversible, so it is confirmed first: on the terminal, or by ``--yes``.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_delete)
    # The target is resolved before the prompt, so a mistyped name never asks.
    prompt = f"Destroy the workspace {arguments.workspace!r}? This is irreversible."
    if _by_path(arguments):
        # The protocol form one machine runs on another: destroy the workspace at
        # a literal path, with no registry involved.
        path = Path(arguments.workspace)
        if not (path.expanduser() / WORKSPACE_DIRECTORY / "format.json").is_file():
            raise ValueError(f"not an httk execution workspace: {path}")
        if not confirm(prompt, yes=arguments.yes, declined="not deleted"):
            return 1
        remove_local_workspace(path)
        print(arguments.workspace)
        return 0
    binding = resolve_workspace(arguments.workspace, project=context.cwd)
    target = None if binding.remote == LOCAL_REMOTE else resolve_remote(binding.remote, project=context.cwd)
    if not confirm(prompt, yes=arguments.yes, declined="not deleted"):
        return 1
    if target is not None:
        name = binding.name.split(":", 1)[1]
        result = run_adapter(
            target.bundle,
            "invoke",
            {"argv": [*REMOTE_WORKSPACE_DELETE_COMMAND, "--yes", name]},
        )
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace delete failed: {result.get('stderr', '')}")
        print(f"deleted {name}")
        return 0
    delete_workspace(arguments.workspace, force=True)
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


def _policy_value(key: str, text: str) -> object:
    """Parse one command-line policy value as the JSON it denotes."""

    if key.startswith("retention.") and text == "keep":
        return "keep"
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"policy {key} must be given as JSON: {text!r} ({exc})") from exc


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


def _workspace_key(key: str) -> tuple[str, str]:
    """Route one workspace key to its store: ``("setting" | "policy" | "prelude", name within that store)``.

    ``policy.*`` names a policy member (``retention.X`` a retention member) and
    ``prelude.<workflow-id>`` a per-workflow prelude; every other key is an
    application setting.

    :param key: The workspace key as written on the command line.
    :return: The store and the name within it.
    :raises ValueError: If the key is not a valid name in its namespace.
    """

    space, _, name = key.partition(".")
    if space == "policy":
        member = name.removeprefix("retention.")
        if name in POLICY_KEYS or (name != member and member in RETENTION_KEYS):
            return "policy", name
        raise ValueError(
            f"unknown workspace key: {key}; the policy keys are policy.visibility_deadline_seconds, "
            f"policy.retention and policy.retention.{{{','.join(sorted(RETENTION_KEYS))}}}"
        )
    if space == "prelude":
        return "prelude", _validate_workflow_prelude_id(name)
    return "setting", _validate_setting_key(key)


def _workspace_values(workspace: Workspace, keys: Sequence[str] = ()) -> dict[str, object]:
    """Return the one flat key view over the settings, policy and preludes, or only *keys* of it.

    :param workspace: The workspace to read.
    :param keys: The keys to select, which may include the whole ``policy.retention`` object.
    :return: Settings, then policy members, then preludes, each in key order; *keys* in their given order.
    :raises ValueError: If a selected key is not set.
    """

    policy = workspace.policy
    values: dict[str, object] = dict(sorted(workspace.settings.items()))
    values["policy.visibility_deadline_seconds"] = policy.visibility_deadline_seconds
    values.update({f"policy.retention.{key}": getattr(policy.retention, key) for key in sorted(RETENTION_KEYS)})
    values.update({f"prelude.{key}": text for key, text in sorted(workspace.read_workflow_preludes().items())})
    if not keys:
        return values
    values["policy.retention"] = policy.retention.as_mapping()
    missing = [key for key in keys if key not in values]
    if missing:
        raise ValueError(f"workspace key is not set: {missing[0]}")
    return {key: values[key] for key in keys}


def _forward(
    arguments: argparse.Namespace, context: CLIContext, command: Sequence[str], tail: Sequence[str]
) -> tuple[Path | None, int]:
    """Run a settings verb on a remote workspace in one far-side call, or return the local root.

    :param arguments: Parsed arguments of ``workspace show``, ``set`` or ``unset``.
    :param context: Current CLI invocation context.
    :param command: The pinned far-side vector.
    :param tail: The keys or pairs, forwarded verbatim; the far side validates and applies them.
    :return: The local workspace root and ``0``, or ``None`` and the far side's exit status.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is not None:
        return root, 0
    assert binding is not None
    return None, _remote_workspace_read(
        binding,
        context,
        command,
        arguments,
        flags=("--json", "--no-durable"),
        tail=tail,
        unwrap_json_array=False,
        workspace_option=True,
    )


def handle_workspace_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print the workspace's settings, policy and preludes as one key view, or the selected keys.

    One KEY prints its bare value: JSON for a setting or policy member, the
    shell text for a prelude. Text rows are ``key<TAB>value`` with the value as
    JSON (a prelude's newlines escaped), plus the annotation for a setting.
    """

    keys: list[str] = arguments.key
    for key in keys:
        _workspace_key(key)
    root, code = _forward(arguments, context, REMOTE_WORKSPACE_SHOW_COMMAND, keys)
    if root is None:
        return code
    workspace = Workspace(root)
    values = _workspace_values(workspace, keys)
    if arguments.json:
        print(json.dumps(values, indent=2, sort_keys=True))
        return 0
    if len(keys) == 1:
        value = values[keys[0]]
        print(value if keys[0].startswith("prelude.") else json.dumps(value, sort_keys=True))
        return 0
    color = use_color()
    workflows = installed_workflow_settings(workspace)
    for key, value in values.items():
        if key.startswith(("policy.", "prelude.")):
            print(f"{key}\t{json.dumps(value, sort_keys=True)}")
            continue
        shown, annotation = annotate_setting(key, value, workflows=workflows, color=color)
        print(f"{shown}\t{json.dumps(value, sort_keys=True)}\t{annotation}")
    return 0


def handle_workspace_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store settings, policy members and preludes in one validated write and print the changed keys as JSON.

    A remote workspace receives the pairs verbatim in one forwarded command,
    so an ``@FILE`` value is read on the far side.
    """

    pairs = [(key, _workspace_key(key), text) for key, text in _pairs(arguments.pairs, "a workspace setting")]
    root, code = _forward(arguments, context, REMOTE_WORKSPACE_SET_COMMAND, arguments.pairs)
    if root is None:
        return code
    workspace = Workspace(root, durable=_durable(arguments))
    require_cli_modifiable(workspace)
    settings: dict[str, object] = {}
    policy: dict[str, object] = {}
    preludes: dict[str, str] = {}
    for key, (space, name), text in pairs:
        if space == "setting":
            settings[name] = _json_value(text, f"setting {name}")
        elif space == "prelude":
            preludes[name] = _prelude_value(text)
        elif name.startswith("retention."):
            # Pairs apply in order: a member merges into a pending whole-object
            # retention pair, and a later whole-object pair replaces earlier merges.
            pending = policy.get("retention", workspace.policy.retention.as_mapping())
            if not isinstance(pending, Mapping):
                raise ValueError(f"policy.retention must be a JSON object to merge {key}")
            policy["retention"] = {**pending, name.split(".", 1)[1]: _policy_value(name, text)}
        else:
            policy[name] = _policy_value(name, text)
    workspace.update_configuration(settings, policy, preludes)
    print(json.dumps(_workspace_values(workspace, list(dict.fromkeys(key for key, _, _ in pairs))), sort_keys=True))
    report_written_settings(settings, workflows=installed_workflow_settings(workspace))
    return 0


def handle_workspace_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove settings and preludes and restore policy defaults in one write, then print the keys as a JSON list."""

    keys: list[str] = arguments.key
    routed = [_workspace_key(key) for key in keys]
    root, code = _forward(arguments, context, REMOTE_WORKSPACE_UNSET_COMMAND, keys)
    if root is None:
        return code
    workspace = Workspace(root, durable=_durable(arguments))
    require_cli_modifiable(workspace)
    defaults = WorkspacePolicy()
    policy: dict[str, object] = {}
    for space, name in routed:
        if space == "policy" and name in POLICY_KEYS:
            policy[name] = defaults.as_mapping()[name]
        elif space == "policy":
            member = name.split(".", 1)[1]
            pending = policy.get("retention", workspace.policy.retention.as_mapping())
            assert isinstance(pending, Mapping)
            policy["retention"] = {**pending, member: getattr(defaults.retention, member)}
    workspace.update_configuration(
        {},
        policy,
        {},
        unset_settings=[name for space, name in routed if space == "setting"],
        unset_preludes=[name for space, name in routed if space == "prelude"],
    )
    print(json.dumps(keys))
    return 0


def handle_workspace_register(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Register copied workspaces on this machine under their recorded names."""

    paths = arguments.paths or [str(context.cwd)]
    if arguments.name is not None and len(paths) > 1:
        raise ValueError("workspace register --name accepts exactly one PATH")
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
        summary="summarize job counts, the seal, the owners and held transfers",
        description=(
            "Summarize one workspace: its job counts by state, whether it is sealed, its owners with their "
            "liveness, and its held transfers (transfer id, root job, members, destination and age); an owner "
            "proven dead gets its tombstone, for the next manager tick or workspace gc to recover"
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

    register = _leaf(
        group,
        "register",
        summary="register copied workspaces on this machine",
        description=(
            "Register one or more workspaces from a copied project tree under their recorded "
            "names, join them to the enclosing project, and record any missing name"
        ),
        handler=handle_workspace_register,
    )
    register.add_argument(
        "paths",
        nargs="*",
        metavar="PATH",
        help="a workspace or a directory inside one (default: the enclosing workspace)",
    )
    register.add_argument("--name", metavar="NAME", help="register the single PATH under this name")
    register.add_argument("--json", action="store_true", help="print the registration findings as JSON")
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
        description="Destroy a registered workspace and deregister it, after confirmation",
        handler=handle_workspace_delete,
    )
    delete.add_argument("workspace", metavar="NAME", nargs="+", help="the registered workspace to destroy")
    delete.add_argument("--yes", action="store_true", help="skip the confirmation of the irreversible destruction")
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

    workspace_help = (
        "the workspace (default: the enclosing workspace, this project's workspace, or the per-user default)"
    )
    show = _leaf(
        group,
        "show",
        summary="show settings, policy and preludes as one key view",
        description=(
            "Print every key of a workspace: application settings (e.g. vasp.command), policy.* members and "
            "prelude.<workflow-id> shell preludes; one KEY prints its bare value"
        ),
        handler=handle_workspace_show,
    )
    show.add_argument("key", metavar="KEY", nargs="*", help="print only these keys (default: every key)")
    show.add_argument("--json", action="store_true", help="print the keys as one flat JSON object")
    store = _leaf(
        group,
        "set",
        summary="store settings, policy members and preludes",
        description=(
            "Validate every pair, then store application settings, policy.visibility_deadline_seconds, "
            "policy.retention[.MEMBER] and prelude.<workflow-id> values; a remote workspace receives the whole "
            "change in one forwarded command"
        ),
        handler=handle_workspace_set,
    )
    store.add_argument(
        "pairs",
        metavar="KEY=VALUE",
        nargs="+",
        type=_setting_pair,
        help=(
            "a setting's JSON value or bare string, a policy member's JSON value (retention members also keep), "
            "or a prelude's shell text or @FILE"
        ),
    )
    store.set_defaults(pairs_parser=store)
    add_durability_arguments(store)
    remove = _leaf(
        group,
        "unset",
        summary="remove settings and preludes, restore policy defaults",
        description="Remove application settings and preludes and restore policy members to their defaults",
        handler=handle_workspace_unset,
    )
    remove.add_argument("key", metavar="KEY", nargs="+", help="the keys to remove or reset")
    add_durability_arguments(remove)
    for leaf in (show, store, remove):
        leaf.add_argument("-w", "--workspace", metavar="WORKSPACE", help=workspace_help)
        _add_adapter_timeout(leaf)

    check = _leaf(
        group,
        "check",
        summary="check the job tree for what the kernel cannot read",
        description=(
            "Check a workspace's job tree, read-only: unparsable names, a job in two places, owned jobs without "
            "their owner, unreadable state.json, jobs another user owns, and stale or missing exchange index "
            "entries. Run it only while no other actor (manager, CLI operation, daemon or transfer) uses the "
            "workspace: findings from a busy workspace may be transient"
        ),
        handler=handle_workspace_check,
    )
    _add_workspace_targets(check, help_text="the workspace to check")
    check.set_defaults(repair=False)
    repair = _leaf(
        group,
        "repair",
        summary="check the job tree and repair what can be repaired",
        description=(
            "Check a workspace's job tree like workspace check, then quarantine unparsable entries, remove stale "
            "exchange index entries and recreate missing ones, after confirmation; every other finding is left to "
            "the operator. Refused unless every owner of the workspace is proven dead and recovered"
        ),
        handler=handle_workspace_check,
    )
    _add_workspace_targets(repair, help_text="the workspace to repair", required=True)
    repair.set_defaults(repair=True)
    repair.add_argument("--yes", action="store_true", help="repair without asking for confirmation")
    for leaf in (check, repair):
        leaf.add_argument("--json", action="store_true", help="print the findings as one JSON report")
        _add_by_path_argument(leaf)

    collect = _leaf(
        group,
        "gc",
        summary="collect the garbage the retention policy allows",
        description="Collect the garbage one execution workspace has accumulated",
        handler=handle_workspace_gc,
    )
    _add_workspace_targets(collect, help_text="the workspace to collect")
    collect.add_argument(
        "-n",
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
    unseal.add_argument("--yes", action="store_true", help="skip the confirmation prompt")

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
        "--deep",
        action="store_true",
        help="also verify every recorded job seal (the report and exit status of job verify)",
    )
    verify.add_argument(
        "--content",
        action="store_true",
        help=(
            "with --deep (which it implies), also decompress job files and check them against their sealed "
            "uncompressed.sha256 manifests"
        ),
    )
    verify.add_argument(
        "--trusted-key",
        action="append",
        default=[],
        metavar="KEY_OR_FINGERPRINT",
        help="trust this key as well (repeatable); the project's pinned keys and all local identities are trusted",
    )
