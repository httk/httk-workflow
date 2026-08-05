"""Workspace command group."""

import os

from ..adapters import REMOTE_WORKSPACE_DELETE_COMMAND, REMOTE_WORKSPACE_INIT_COMMAND, seed_application_settings
from ..configuration import machine_names
from ..introspection import read_managers
from ..manifests import read_maintenance_lock, workspace_maintenance_guard
from ..models import DEFAULT_LEASE_SECONDS
from ..projects import read_project_section, require_project, write_project_section
from ..registry import _update_workspace_path, valid_workspace_name
from ._common import *
from ._common import (
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
    _run_adapter,
)

# ---------------------------------------------------------------------------
# workspace
# ---------------------------------------------------------------------------


def add_workspace_init_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`workspace init`, shared with ``httk-taskmanager init``."""

    parser.add_argument("workspace", metavar="PATH", help="the local path, or REMOTE:PATH, to initialize")
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
        argv = [*REMOTE_WORKSPACE_INIT_COMMAND, remote_path]
        if arguments.name:
            argv += ["--name", name]
        merged = {**seed_application_settings(target.bundle), **settings}
        for key, value in merged.items():
            argv += ["--setting", f"{key}={json.dumps(value)}"]
        result = _run_adapter(target.bundle, "invoke", {"argv": argv})
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace init failed: {result.get('stderr', '')}")
        print(name)
        return 0

    local_path = remote_path if separator and remote in machine_names() else arguments.workspace
    root = (Path(context.cwd) / local_path).resolve()
    name = arguments.name or root.name
    valid_workspace_name(name)
    format_path = root / ".httk-workflow" / "format.json"
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
    """Declare :command:`workspace status`, shared with ``httk-taskmanager status``."""

    add_workspace_argument(parser, help_text="the workspace to summarize")
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
                    "format_version": 1,
                    "workspace_id": workspace.workspace_id,
                    "root": str(workspace.root),
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
    # A clean workspace and a fully repaired one both exit zero; anything an
    # operator still has to deal with exits one, as a check command should.
    return 1 if report.unresolved else 0


def handle_workspace_gc(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Free the disk the workspace retention policy says may be freed."""

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
        reachable = (Path(binding.path) / ".httk-workflow" / "format.json").is_file()
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
        print("no workspaces are registered; create one with `httk workflow workspace init`")
        return 0
    for row in rows:
        mark = "?" if row["reachable"] is None else ("ok" if row["reachable"] else "missing")
        print(f"{row['name']}\t{row['path']}\t{mark}")
    return 0


def handle_workspace_forget(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Deregister one workspace name, leaving the workspace itself in place."""

    binding = forget_workspace(arguments.workspace)
    print(f"forgot {binding.name}")
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
    binding = resolve_workspace(arguments.workspace, project=context.cwd)
    if binding.remote != LOCAL_REMOTE and not arguments.force:
        raise ValueError("workspace delete requires --force")
    if binding.remote != LOCAL_REMOTE:
        target = resolve_remote(binding.remote, project=context.cwd)
        name = binding.name.split(":", 1)[1]
        result = _run_adapter(
            target.bundle,
            "invoke",
            {"argv": [*REMOTE_WORKSPACE_DELETE_COMMAND, name, "--force"]},
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
                if exc.errno != getattr(os, "EXDEV", 18):
                    raise
                raise ValueError(
                    "workspace move must stay within one filesystem; stop managers, copy the workspace manually, "
                    "then forget the old name and run `workspace init <newpath> --name NAME`"
                ) from exc
            renamed = True
            updated = _update_workspace_path(binding.name, destination, durable=_durable(arguments))
    finally:
        if renamed:
            release_maintenance_lock(Workspace(destination), force=True)
    print(updated.path)
    return 0


def handle_workspace_settings_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show one workspace's application settings, or one named member."""

    ambiguous_key = False
    if arguments.key is None and arguments.workspace is not None:
        candidate = arguments.workspace
        try:
            resolve_workspace(candidate, project=context.cwd)
        except ValueError as exc:
            if not str(exc).startswith("unknown workspace:"):
                raise
            arguments.workspace = None
            arguments.key = candidate
            ambiguous_key = True
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_SETTINGS_COMMAND, "show"),
            arguments,
            flags=("--json",),
            tail=() if arguments.key is None else (arguments.key,),
        )
    workspace = Workspace(root, mutable=False)
    settings = workspace.settings
    if arguments.key is not None:
        if arguments.key not in settings:
            if ambiguous_key:
                raise ValueError(
                    f"{arguments.key!r} is neither a registered workspace nor a setting key of the default workspace"
                )
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
            tail=(arguments.key, arguments.value),
        )
    workspace = Workspace(root, durable=_durable(arguments))
    settings = workspace.set_setting(arguments.key, value)
    print(json.dumps(settings[arguments.key], sort_keys=True))
    return 0


def handle_workspace_settings_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one application setting from a workspace."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_workspace_read(
            binding,
            context,
            (*REMOTE_WORKSPACE_SETTINGS_COMMAND, "unset"),
            arguments,
            flags=("--durable", "--no-durable"),
            tail=(arguments.key,),
        )
    workspace = Workspace(root, durable=_durable(arguments))
    workspace.unset_setting(arguments.key)
    return 0


def build_workspace_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
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
        summary="show or set the workspace policy",
        description="Show or set the tunables one workspace publishes to every attacher",
    )
    show = _leaf(
        policy_actions,
        "show",
        summary="print the current policy",
        description="Print the policy of one workflow workspace",
        handler=handle_workspace_policy_show,
    )
    add_workspace_argument(show, help_text="the workspace whose policy to print")
    show.add_argument("--json", action="store_true", help="print the policy as one JSON object")
    store = _leaf(
        policy_actions,
        "set",
        summary="store one policy member",
        description="Store one member of the policy of a workflow workspace",
        handler=handle_workspace_policy_set,
    )
    add_workspace_argument(store, help_text="the workspace whose policy to change")
    store.add_argument("key", metavar="KEY", help="one of " + ", ".join(sorted(POLICY_KEYS)))
    store.add_argument("value", metavar="VALUE", help="the JSON value to store")
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
    add_workspace_argument(settings_show, help_text="the workspace whose settings to read")
    settings_show.add_argument(
        "key",
        metavar="KEY",
        nargs="?",
        help="print only this setting (default: all of them)",
    )
    settings_show.add_argument("--json", action="store_true", help="print the settings as one JSON object")
    _add_by_path_argument(settings_show)
    settings_set = _leaf(
        settings_actions,
        "set",
        summary="store one application setting",
        description="Store one application setting on a workspace, e.g. vasp.command",
        handler=handle_workspace_settings_set,
    )
    add_workspace_argument(settings_set, help_text="the workspace to change")
    settings_set.add_argument("key", metavar="KEY", help="the dotted setting name, e.g. vasp.command")
    settings_set.add_argument("value", metavar="VALUE", help="the JSON value, or a bare string, to store")
    _add_by_path_argument(settings_set)
    add_durability_arguments(settings_set)
    settings_unset = _leaf(
        settings_actions,
        "unset",
        summary="remove one application setting",
        description="Remove one application setting from a workspace",
        handler=handle_workspace_settings_unset,
    )
    add_workspace_argument(settings_unset, help_text="the workspace to change")
    settings_unset.add_argument("key", metavar="KEY", help="the dotted setting name to remove")
    _add_by_path_argument(settings_unset)
    add_durability_arguments(settings_unset)

    fsck = _leaf(
        group,
        "fsck",
        summary="check that every marker resolves to its journal frame",
        description="Check, and optionally repair, the marker-to-journal integrity of a workspace",
        handler=handle_workspace_fsck,
    )
    add_workspace_argument(fsck, help_text="the workspace to check")
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
        description="Collect the garbage one workflow workspace has accumulated",
        handler=handle_workspace_gc,
    )
    add_workspace_argument(collect, help_text="the workspace to collect")
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
    add_workspace_argument(unlock, help_text="the workspace whose lock to release")
    unlock.add_argument(
        "--force",
        action="store_true",
        help="also remove a lock whose holder is still alive",
    )
