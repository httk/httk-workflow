"""Workspace command group."""

import errno
import os
from contextlib import redirect_stdout
from copy import copy
from io import StringIO

from ..adapters import REMOTE_WORKSPACE_DELETE_COMMAND, REMOTE_WORKSPACE_INIT_COMMAND, seed_application_settings
from ..configuration import machine_names
from ..introspection import read_managers
from ..manifests import read_maintenance_lock, workspace_maintenance_guard
from ..models import DEFAULT_LEASE_SECONDS, WORKSPACE_DIRECTORY
from ..packages import load_workflow_package
from ..projects import read_project_section, require_project, write_project_section
from ..registry import _update_workspace_path, adopt_workspace, move_project_member, valid_workspace_name
from ..seals import (
    default_workspace_keys,
    is_workspace_sealed,
    read_seal,
    seal_job,
    seal_workspace,
    unseal_workspace,
    unsealed_jobs,
    workspace_seal_path,
)
from ._common import *
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
    _published_runner_entries,
    _remote_workspace_read,
    _resolve_binding,
    _run_adapter,
)

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
        result = _run_adapter(target.bundle, "invoke", {"argv": argv})
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
    return 0


def add_workspace_status_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace status`."""

    add_workspace_argument(parser, help_text="the workspace to summarize")
    parser.add_argument("--json", action="store_true", help="print the machine-readable status document")
    _add_by_path_argument(parser)
    add_durability_arguments(parser)


def handle_workspace_status(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Summarize the authoritative markers of one workspace.

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
                    "format_version": 2,
                    "workspace_id": workspace.workspace_id,
                    "root": str(workspace.root),
                    "workspace_format_version": workspace.format["format_version"],
                    "core_profile": workspace.format["core_profile"],
                    "extensions": sorted(workspace.extensions),
                    "sealed": is_workspace_sealed(workspace),
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
    print(f"sealed: {'yes' if is_workspace_sealed(workspace) else 'no'}")
    return 0


def handle_workspace_managers(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the managers registered to serve one workspace.

    Answers "what serves this workspace?" directly, rather than by running
    ``job why`` on an arbitrary job: one line per registered manager, its
    live-or-stale liveness against the default lease, and what it serves.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_managers)
    workspace = Workspace(_local_root(arguments, context, action="list its managers"), mutable=False)
    managers = read_managers(workspace)
    if arguments.json:
        print(json.dumps([manager.as_mapping() for manager in managers], indent=2, sort_keys=True))
        return 0
    if not managers:
        print("no manager has ever registered in this workspace")
        return 0
    for manager in managers:
        live = "live" if manager.alive(lease_seconds=DEFAULT_LEASE_SECONDS) else "stale"
        age = "no heartbeat" if manager.heartbeat_age_seconds is None else f"{manager.heartbeat_age_seconds:.0f}s ago"
        pools = "any" if manager.accept_any_pool else ",".join(sorted(manager.pools)) or "-"
        print(
            f"{manager.manager_id}\t{live}\t{age}\t{manager.hostname or '-'}\t"
            f"pools={pools}\tcapabilities={','.join(sorted(manager.capabilities)) or '-'}\t"
            f"executors={','.join(sorted(manager.executors)) or '-'}\t"
            f"runner-modules={','.join(manager.runner_modules) or '-'}"
        )
    return 0


def handle_workspace_workflows(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the runners a workspace publishes, with each tree's workflow identity.

    A directory package's workflow id, alias, and summary come from loading its
    manifest with ``register=False`` — the runner is never executed. A file
    runner can only be described by running it, which this read never does, so it
    reports no identity. A manifest that fails to load leaves the listing intact:
    the id column is ``-`` and the error text takes the summary column.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_workflows)
    workspace = Workspace(_local_root(arguments, context, action="list its runners"), mutable=False)
    store = workspace.runners
    entries = list(_published_runner_entries(store)) if store.is_dir() else []
    rows: list[dict[str, object]] = []
    for path in entries:
        is_tree = path.is_dir()
        row: dict[str, object] = {
            "path": path.relative_to(store).as_posix(),
            "kind": "tree" if is_tree else "file",
            "sha256": tree_digest(path) if is_tree else sha256_file(path),
            "workflow": None,
            "alias": None,
            "summary": None,
            "error": None,
        }
        if is_tree:
            try:
                provider = load_workflow_package(path, register=False)
            except _ERRORS as exc:
                row["error"] = str(exc)
            else:
                row["workflow"] = provider.workflow_id
                row["alias"] = provider.alias
                row["summary"] = provider.summary or None
        rows.append(row)
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if not rows:
        print("no runners published in this workspace")
        return 0
    for row in rows:
        summary = row["error"] or row["summary"] or "-"
        print(f"{row['path']}\t{row['kind']}\t{row['workflow'] or '-'}\t{summary}")
    return 0


def handle_workspace_seal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal a workspace by recording the seal digest of every job it holds.

    Every job must already be sealed. ``--force`` seals the still-unsealed jobs
    first — any quiescent kind, not just succeeded ones — and then the workspace;
    without it, the unsealed jobs are listed and the command refuses.
    """

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_seal)
    workspace = Workspace(_local_root(arguments, context, action="seal it"))
    refs = [ref.strip() for ref in arguments.keys.split(",") if ref.strip()] if arguments.keys else None
    resolved = None
    with workspace_maintenance_guard(workspace):
        unsealed = unsealed_jobs(workspace)
        if unsealed and not arguments.force:
            for marker in unsealed:
                print(f"{marker.job_id}\t{marker.kind}", file=sys.stderr)
            return 1
        resolved = default_workspace_keys(workspace, refs)
        for marker in unsealed:
            seal_job(workspace, marker, keys=resolved)
        seal_workspace(workspace, keys=resolved)
    seal = read_seal(workspace_seal_path(workspace))
    roles = ",".join(str(signature.get("role")) for signature in seal.signatures)
    print(f"{workspace.root}\tsealed\t{roles}")
    if resolved.missing_roles:
        print(f"warning: no key resolved for seal role(s): {', '.join(resolved.missing_roles)}", file=sys.stderr)
    return 0


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
    return _print_policy(Workspace(root, mutable=False).policy, as_json=arguments.json)


def handle_workspace_policy_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one policy member of a workspace and print the result."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_policy_set)
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

    if isinstance(arguments.workspace, list):
        if (arguments.repair or arguments.quarantine_unrepairable) and not arguments.workspace:
            raise ValueError("workspace fsck repair requires at least one WORKSPACE")
        return _workspace_batch(arguments, context, handle_workspace_fsck)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            REMOTE_WORKSPACE_FSCK_COMMAND,
            arguments,
            flags=("--repair", "--quarantine-unrepairable", "--json"),
        )
    workspace = Workspace(root, mutable=arguments.repair)
    report = workspace.check(
        repair=arguments.repair,
        quarantine_unrepairable=arguments.quarantine_unrepairable,
    )
    if arguments.json:
        print(json.dumps(report.as_mapping(), indent=2, sort_keys=True))
    else:
        for finding in report.findings:
            print(f"{finding.action}\t{finding.problem}\t{finding.job_key or '-'}\t{finding.entry}\t{finding.detail}")
        print(f"checked {report.markers_checked} markers, {len(report.findings)} findings")
        if report.always_safe_candidates:
            details = ", ".join(
                f"{category}={count}" for category, count in sorted(report.always_safe_counts.items()) if count
            )
            print(
                f"{report.always_safe_candidates} always-safe leftovers ({details}); "
                "any manager run or workspace gc collects them"
            )
    # A clean workspace and a fully repaired one both exit zero; anything an
    # operator still has to deal with exits one, as a check command should.
    return 1 if report.unresolved else 0


def handle_workspace_gc(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Free the disk the workspace retention policy says may be freed."""

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
        )
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
    if report.skipped_foreign:
        details = ", ".join(f"{category}={count}" for category, count in sorted(report.skipped_foreign.items()))
        print(f"skipped foreign: {details}")
    if arguments.dry_run:
        print("dry run: nothing was removed")
    return 0


def handle_workspace_unlock(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Release a workspace maintenance lock."""

    if isinstance(arguments.workspace, list):
        return _workspace_batch(arguments, context, handle_workspace_unlock)
    workspace = Workspace(_local_root(arguments, context, action="release its lock"))
    print(release_maintenance_lock(workspace, force=arguments.force))
    return 0


def handle_workspace_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the registered workspaces and where each resolves to."""

    if arguments.remote is not None:
        remote_name, separator, empty = arguments.remote.partition(":")
        if not separator or empty:
            raise ValueError("workspace list expects REMOTE:")
        target = resolve_remote(remote_name, project=context.cwd)
        result = _run_adapter(target.bundle, "invoke", {"argv": [*REMOTE_WORKSPACE_LIST_COMMAND, "--json"]})
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
    binding = forget_workspace(arguments.workspace, force=arguments.force)
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
        result = _run_adapter(
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
    binding = resolve_workspace(arguments.workspace, project=context.cwd)
    if binding.remote != LOCAL_REMOTE:
        target = resolve_remote(binding.remote, project=context.cwd)
        name = binding.name.split(":", 1)[1]
        result = _run_adapter(
            target.bundle, "invoke", {"argv": [*REMOTE_WORKSPACE_MOVE_COMMAND, name, arguments.destination]}
        )
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace move failed: {result.get('stderr', '')}")
        sys.stdout.write(str(result.get("stdout", "")))
        return 0
    assert binding.path is not None
    workspace = Workspace(binding.path, mutable=False)
    for manager in read_managers(workspace):
        if manager.alive(lease_seconds=DEFAULT_LEASE_SECONDS):
            raise ValueError(f"cannot move workspace while manager {manager.manager_id!r} has a fresh heartbeat")
    lock = read_maintenance_lock(workspace)
    if lock is not None and not lock.is_stale():
        raise ValueError(f"cannot move workspace while the maintenance lock is held by {lock.describe()}")
    destination = (Path(context.cwd) / arguments.destination).resolve()
    if destination.exists():
        raise ValueError(f"workspace move destination already exists: {destination}")
    renamed = False
    try:
        with workspace_maintenance_guard(workspace):
            for manager in read_managers(workspace):
                if manager.alive(lease_seconds=DEFAULT_LEASE_SECONDS):
                    raise ValueError(
                        f"cannot move workspace while manager {manager.manager_id!r} has a fresh heartbeat"
                    )
            try:
                os.rename(binding.path, destination)
            except OSError as exc:
                if exc.errno != errno.EXDEV:
                    raise
                raise ValueError(
                    "workspace move must stay within one filesystem; stop managers, copy the workspace manually, "
                    "then forget the old name and run `workspace init --name NAME <newpath>`"
                ) from exc
            renamed = True
            updated = _update_workspace_path(binding.name, destination, durable=_durable(arguments))
            move_project_member(Path(binding.path), destination)
    finally:
        if renamed:
            release_maintenance_lock(Workspace(destination), force=True)
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
    workspace = Workspace(root, mutable=False)
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
    workspace = Workspace(root, mutable=False)
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
        summary="summarize the authoritative markers",
        description="Summarize the authoritative markers of one workspace",
        handler=handle_workspace_status,
    )
    _add_workspace_targets(status, help_text="the workspace to summarize")
    status.add_argument("--json", action="store_true", help="print the machine-readable status document")
    _add_by_path_argument(status)
    add_durability_arguments(status)
    managers = _leaf(
        group,
        "managers",
        summary="list the managers serving this workspace",
        description="List every manager registered to serve one workspace, live or stale",
        handler=handle_workspace_managers,
    )
    _add_workspace_targets(managers, help_text="the workspace whose managers to list")
    managers.add_argument("--json", action="store_true", help="print the managers as one JSON array")

    workflows = _leaf(
        group,
        "workflows",
        summary="list the runners this workspace publishes",
        description="List the runners in a workspace's runner store, with each directory package's workflow identity",
        handler=handle_workspace_workflows,
    )
    _add_workspace_targets(workflows, help_text="the workspace whose runners to list")
    workflows.add_argument("--json", action="store_true", help="print the runners as one JSON array")

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
    forget.add_argument(
        "--force", action="store_true", help="deregister the name even when unretired outbound transfers remain"
    )

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
        summary="check that every marker resolves to its journal frame",
        description="Check, and optionally repair, the marker-to-journal integrity of a workspace",
        handler=handle_workspace_fsck,
    )
    _add_workspace_targets(fsck, help_text="the workspace to check")
    fsck.add_argument(
        "--repair",
        action="store_true",
        help="re-point damaged markers at the last readable frame",
    )
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
        description="Collect the garbage one execution workspace has accumulated",
        handler=handle_workspace_gc,
    )
    _add_workspace_targets(collect, help_text="the workspace to collect")
    collect.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be removed without touching it",
    )
    collect.add_argument("--json", action="store_true", help="print the collection as one JSON report")
    _add_by_path_argument(collect)

    unlock = _leaf(
        group,
        "unlock",
        summary="release a maintenance lock",
        description="Release a stale, or with --force a live, workspace maintenance lock",
        handler=handle_workspace_unlock,
    )
    _add_workspace_targets(unlock, help_text="the workspace whose lock to release")
    unlock.add_argument(
        "--force",
        action="store_true",
        help="also remove a lock whose holder is still alive",
    )

    seal = _leaf(
        group,
        "seal",
        summary="seal a workspace and the jobs it holds",
        description="Record the seal digest of every job in a workspace under one signed workspace seal",
        handler=handle_workspace_seal,
    )
    _add_workspace_targets(seal, help_text="the workspace to seal")
    seal.add_argument(
        "--force",
        action="store_true",
        help="seal every still-unsealed job first, rather than refusing",
    )
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
