"""The per-user registry of local workspaces.

Remote workspace names are references, not registrations: the owning machine
keeps the path and resolves the name when a command reaches it.
"""

import json
import os
import shutil
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from httk.core.project.members import ProjectMember

from ._util import read_json, write_json_atomic
from .adapters import resolve_remote, valid_remote_name
from .configuration import config_home, data_home, machine_names
from .errors import ResolutionMiss
from .models import WORKSPACE_DIRECTORY
from .projects import discover_project, read_project_section
from .workspace import Workspace

__all__ = [
    "DEFAULT_WORKSPACE_NAME",
    "LOCAL_REMOTE",
    "WORKSPACES_FILE",
    "WorkspaceBinding",
    "adopt_workspace",
    "create_workspace",
    "default_workspace",
    "delete_workspace",
    "forget_workspace",
    "is_local",
    "list_workspaces",
    "register_workspace",
    "remove_local_workspace",
    "resolve_workspace",
    "split_workspace_binding",
    "valid_workspace_binding_name",
    "valid_workspace_name",
    "workspaces_path",
]

LOCAL_REMOTE = "local"
DEFAULT_WORKSPACE_NAME = "default"
WORKSPACES_FILE = "workspaces.json"
_REGISTRY_FORMAT = "httk-workspaces"
_REGISTRY_FORMAT_VERSION = 2


@dataclass(frozen=True)
class WorkspaceBinding:
    """Represent one workspace reference, with a path only when it is local.

    :param name: Preserve the plain or remote-qualified workspace name.
    :param remote: Identify the owning machine, or the local sentinel.
    :param path: Locate the workspace locally, or leave it unset for remote
        bindings.
    """

    name: str
    remote: str
    path: str | None


def valid_workspace_name(name: str) -> str:
    """Validate one plain local workspace name.

    :param name: Supply the name to validate.
    :return: The unchanged valid name.
    :raises httk.workflow.errors.ResolutionMiss: If the name contains reserved separators or is
        empty.
    """
    if not name or "/" in name or ":" in name or name in {".", ".."}:
        raise ResolutionMiss(f"invalid workspace name: {name!r}; ':' is reserved for remote-qualified names")
    return name


def valid_workspace_binding_name(name: str) -> str:
    """Validate one remote-qualified workspace name.

    :param name: Supply the ``REMOTE:NAME`` binding to validate.
    :return: The unchanged valid binding.
    :raises httk.workflow.errors.ResolutionMiss: If the remote or workspace component is invalid.
    """
    if name.count(":") != 1:
        raise ResolutionMiss(f"invalid workspace binding name: {name!r}; use REMOTE:NAME")
    remote, workspace = name.split(":", 1)
    valid_remote_name(remote)
    valid_workspace_name(workspace)
    if remote == LOCAL_REMOTE:
        raise ResolutionMiss("local:NAME is invalid; plain NAME addresses a local workspace")
    return name


def split_workspace_binding(name: str) -> tuple[str, str] | None:
    """Split a workspace name into remote and local parts when qualified.

    :param name: Supply a plain or remote-qualified workspace name.
    :return: The remote and workspace names, or ``None`` for a local name.
    :raises httk.workflow.errors.ResolutionMiss: If the supplied name is invalid.
    """
    if ":" not in name:
        valid_workspace_name(name)
        return None
    valid_workspace_binding_name(name)
    return tuple(name.split(":", 1))  # type: ignore[return-value]


def workspaces_path() -> Path:
    """Return the per-user workspace registry path.

    :return: The path of the registry file.
    """
    return config_home() / WORKSPACES_FILE


def _read_global() -> dict[str, dict[str, str]]:
    path = workspaces_path()
    if not path.exists():
        return {}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read workspace registry {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"workspace registry is not a JSON object: {path}")
    if document.get("format") != _REGISTRY_FORMAT or document.get("format_version") != _REGISTRY_FORMAT_VERSION:
        raise ValueError(
            f"workspace registry is not {_REGISTRY_FORMAT} format version {_REGISTRY_FORMAT_VERSION}: {path}"
        )
    workspaces = document.get("workspaces", {})
    if not isinstance(workspaces, dict):
        raise ValueError(f"workspace registry workspaces member is not an object: {path}")
    result: dict[str, dict[str, str]] = {}
    for name, record in workspaces.items():
        if not isinstance(name, str) or not isinstance(record, Mapping):
            raise ValueError(f"workspace registry has an invalid entry: {path}")
        location = record.get("path")
        if not isinstance(location, str) or not location or not Path(location).is_absolute():
            raise ValueError(f"workspace registry entry {name!r} has no absolute path: {path}")
        result[name] = {"path": location}
    return result


def _write_global(workspaces: Mapping[str, Mapping[str, str]], *, durable: bool = True) -> Path:
    path = workspaces_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(
        path,
        {
            "format": _REGISTRY_FORMAT,
            "format_version": _REGISTRY_FORMAT_VERSION,
            "workspaces": {name: {"path": str(record["path"])} for name, record in workspaces.items()},
        },
        durable=durable,
    )
    return path


def _unknown(name: str, *, project: str | os.PathLike[str] | None = None) -> ValueError:
    if discover_project(project) is not None:
        return ResolutionMiss(
            f"unknown workspace: {name}; if this project was copied here, adopt it with "
            "`httk workspace adopt`, or register a new one with `httk workspace init`"
        )
    return ResolutionMiss(
        f"unknown workspace: {name}; register it with `httk workspace init` "
        "or list the registered ones with `httk workspace list`"
    )


def _member_binding(name: str, project: str | os.PathLike[str] | None) -> "WorkspaceBinding | None":
    """Resolve a workspace name from the enclosing project's members.json, if present."""

    root = discover_project(project)
    if root is None:
        return None
    from httk.core.project.members import project_members

    for member in project_members(root):
        if member.kind == "workspace" and member.name == name:
            return WorkspaceBinding(name, LOCAL_REMOTE, str((root / member.path).resolve()))
    return None


def _local_binding(name: str) -> WorkspaceBinding:
    valid_workspace_name(name)
    record = _read_global().get(name)
    if record is None:
        raise _unknown(name)
    return WorkspaceBinding(name, LOCAL_REMOTE, record["path"])


def _refuse_registered(workspaces: Mapping[str, Mapping[str, str]], name: str, location: str) -> None:
    """Refuse a name or canonical path that is already registered."""

    valid_workspace_name(name)
    if name in workspaces:
        raise ValueError(f"workspace {name!r} is already registered; forget it first")
    for existing_name, record in workspaces.items():
        if Path(record["path"]).resolve() == Path(location):
            raise ValueError(f"workspace path is already registered as {existing_name!r}")


def register_workspace(
    name: str,
    path: str | os.PathLike[str],
    *,
    durable: bool = True,
) -> WorkspaceBinding:
    """Register one absolute local workspace path under a plain name.

    :param name: Assign this plain name to the workspace.
    :param path: Locate the local workspace to register.
    :param durable: Flush the registry update durably when true.
    :return: The registered local binding.
    :raises ValueError: If the name or path is already registered.
    """

    workspaces = _read_global()
    location = str(Path(path).expanduser().resolve())
    _refuse_registered(workspaces, name, location)
    workspaces[name] = {"path": location}
    _write_global(workspaces, durable=durable)
    _record_member_name(Path(location), name)
    return WorkspaceBinding(name, LOCAL_REMOTE, location)


def _record_member_name(path: Path, name: str) -> None:
    """Best-effort record a workspace's registered *name* in its project's members.json.

    A workspace outside a project, or one whose project is sealed, records
    nothing; the name a copied tree carries is what makes it adoptable elsewhere.
    """

    project = discover_project(path.expanduser().resolve())
    if project is None:
        return
    from httk.core.project.members import register_project_member
    from httk.core.project.sealing import SealedError

    try:
        register_project_member(project, path, "workspace", name=name)
    except (ValueError, SealedError):
        pass


def _update_workspace_path(name: str, path: Path, *, durable: bool = True) -> WorkspaceBinding:
    valid_workspace_name(name)
    workspaces = _read_global()
    if name not in workspaces:
        raise _unknown(name)
    location = str(path.expanduser().resolve())
    workspaces[name] = {"path": location}
    _write_global(workspaces, durable=durable)
    return WorkspaceBinding(name, LOCAL_REMOTE, location)


def forget_workspace(name: str, *, durable: bool = True, force: bool = False) -> WorkspaceBinding:
    """Forget one registered workspace without removing its files.

    :param name: Identify the registered workspace.
    :param durable: Flush the registry update durably when true.
    :param force: Permit forgetting a workspace with outbound transfers pending.
    :return: The forgotten binding.
    :raises ValueError: If pending transfers block the operation.
    """
    binding = _local_binding(name)
    assert binding.path is not None
    _unregister_project_member(Path(binding.path), allow_sealed=True)
    if not force:
        transfers = Path(binding.path) / WORKSPACE_DIRECTORY / "transfers"
        pending = []
        for ledger_path in sorted(transfers.glob("*.json")) if transfers.is_dir() else ():
            ledger = read_json(ledger_path)
            if ledger.get("status") != "retired":
                pending.append(ledger_path.name)
        if pending:
            raise ValueError(
                f"workspace {name!r} has unretired outbound transfers; fetch or retire them first, "
                "or use `workspace forget --force` to deregister the name anyway"
            )
    workspaces = _read_global()
    del workspaces[name]
    _write_global(workspaces, durable=durable)
    return binding


def resolve_workspace(name: str, *, project: str | os.PathLike[str] | None = None) -> WorkspaceBinding:
    """Resolve a local name globally or a remote name at use time.

    :param name: Supply a plain or remote-qualified workspace name.
    :param project: Locate project configuration for remote resolution.
    :return: The resolved workspace binding.
    :raises httk.workflow.errors.ResolutionMiss: If the workspace or remote is unknown.
    """

    binding = split_workspace_binding(name)
    if binding is None:
        try:
            return _local_binding(name)
        except ResolutionMiss:
            # A copied project carries its workspace names in members.json; resolve
            # from there for this invocation without silently writing the registry
            # (adoption is explicit — `httk workspace adopt`).
            fallback = _member_binding(name, project)
            if fallback is not None:
                return fallback
            raise _unknown(name, project=project) from None
    remote, plain_name = binding
    if remote in machine_names():
        return _local_binding(plain_name)
    resolve_remote(remote, project=project)
    return WorkspaceBinding(name, remote, None)


def default_workspace(*, project: str | os.PathLike[str] | None = None, durable: bool = True) -> WorkspaceBinding:
    """Resolve or initialize the project's default local workspace.

    :param project: Locate project configuration for its default binding.
    :param durable: Flush any initialized registry state durably when true.
    :return: The default workspace binding.
    """
    project_root = discover_project(project)
    if project_root is not None:
        section = read_project_section(project_root, "workspace")
        recorded = section.get("default")
        if recorded is not None:
            if not isinstance(recorded, str):
                raise ValueError("project workspace default must be a workspace name")
            return resolve_workspace(recorded, project=project_root)
    try:
        return _local_binding(DEFAULT_WORKSPACE_NAME)
    except ResolutionMiss:
        pass

    root = data_home() / "workspace"
    format_path = root / WORKSPACE_DIRECTORY / "format.json"
    if not format_path.exists():
        try:
            Workspace.initialize(root, durable=durable)
        except FileExistsError as race:
            deadline = time.monotonic() + 2.0
            while not format_path.is_file() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not format_path.is_file():
                control = root / WORKSPACE_DIRECTORY
                raise ValueError(
                    f"default workspace initialization left a partial directory at {control}; "
                    f"delete {control} and retry"
                ) from race
    try:
        return register_workspace(DEFAULT_WORKSPACE_NAME, root, durable=durable)
    except ValueError:
        existing = _local_binding(DEFAULT_WORKSPACE_NAME)
        assert existing.path is not None
        if Path(existing.path).resolve() != root.resolve():
            raise
        return existing


def _choose_adoption_name(
    name: str | None,
    member: ProjectMember | None,
    project: Path | None,
    root: Path,
    members: list[ProjectMember],
) -> str:
    """Pick the name to adopt a workspace under, by the documented precedence."""

    if name:
        return name
    if member is not None and member.name:
        return member.name
    if project is not None:
        section = read_project_section(project, "workspace")
        default = section.get("default")
        # ponytail: only when this is the sole workspace member can an unnamed
        # member be assumed to be the recorded default; richer disambiguation
        # would need a name already, which is the case this fallback exists for.
        workspaces = [candidate for candidate in members if candidate.kind == "workspace"]
        if isinstance(default, str) and default and len(workspaces) == 1:
            return default
    return root.name


def adopt_workspace(root: str | os.PathLike[str], *, name: str | None = None) -> tuple[dict[str, object], ...]:
    """Re-establish a workspace's local links on this machine, idempotently.

    Adoption registers the workspace centrally under its name (never overwriting
    a different path already holding that name), ensures it is a member of its
    enclosing project, and records the chosen name in members.json when absent.
    It never mutates sealed state and reports every step as a doctor-shaped
    finding rather than raising.

    :param root: The workspace root to adopt.
    :param name: Override the adopted name; otherwise the recorded/derived name.
    :return: The adoption findings.
    """

    from httk.core.project.members import (
        project_members,
        register_project_member,
        set_project_member_name,
    )
    from httk.core.project.sealing import SealedError

    from .hygiene import Finding

    root = Path(root).expanduser().resolve()
    findings: list[dict[str, object]] = []
    project = discover_project(root)
    members = list(project_members(project)) if project is not None else []
    member = None
    if project is not None:
        for candidate in members:
            if (project / candidate.path).resolve() == root:
                member = candidate
                break
    chosen = _choose_adoption_name(name, member, project, root, members)

    # (b)+(c) project membership and the recorded name.
    if project is not None:
        try:
            if member is None:
                register_project_member(project, root, "workspace", name=chosen)
                findings.append(
                    Finding(
                        "member", "ok", f"registered {root} as project member {chosen!r}", repaired=True
                    ).as_mapping()
                )
            elif member.name is None or (name is not None and member.name != chosen):
                set_project_member_name(project, root, chosen)
                findings.append(Finding("member", "ok", f"recorded member name {chosen!r}", repaired=True).as_mapping())
        except SealedError as exc:
            findings.append(Finding("member", "error", str(exc)).as_mapping())
        except ValueError as exc:
            findings.append(Finding("member", "error", str(exc)).as_mapping())

    # (a) central registration under the chosen name.
    workspaces = _read_global()
    registered_here = next((n for n, r in workspaces.items() if Path(r["path"]).resolve() == root), None)
    if registered_here is not None:
        findings.append(Finding("registry", "ok", f"already registered here as {registered_here!r}").as_mapping())
    elif chosen in workspaces:
        findings.append(
            Finding(
                "registry",
                "error",
                f"name {chosen!r} is already registered to a different path {workspaces[chosen]['path']}",
            ).as_mapping()
        )
    else:
        register_workspace(chosen, root)
        findings.append(Finding("registry", "ok", f"registered workspace {chosen!r}", repaired=True).as_mapping())
    return tuple(findings)


def list_workspaces(*, project: str | os.PathLike[str] | None = None) -> list[WorkspaceBinding]:
    """List registered local workspaces in stable name order.

    :param project: Retain the project-aware API shape for callers.
    :return: The registered local bindings.
    """
    return [WorkspaceBinding(name, LOCAL_REMOTE, record["path"]) for name, record in sorted(_read_global().items())]


def is_local(binding: WorkspaceBinding) -> bool:
    """Report whether a workspace binding points to the local machine.

    :param binding: Supply the workspace binding to inspect.
    :return: Whether the binding is local.
    """
    return binding.remote == LOCAL_REMOTE


def _unregister_project_member(path: Path, *, allow_sealed: bool = False) -> None:
    """Remove the project-member record of the workspace at *path*.

    A workspace outside any project, or one the project never recorded, is a
    no-op. A sealed project refuses so its members cannot silently change; when
    *allow_sealed* is set — forgetting a name while the files stay in place — the
    refusal is swallowed and the still-present workspace keeps its membership.
    """

    project = discover_project(path.expanduser().resolve())
    if project is None:
        return
    from httk.core.project.members import unregister_project_member
    from httk.core.project.sealing import SealedError

    try:
        unregister_project_member(project, path)
    except ValueError:
        pass
    except SealedError:
        if not allow_sealed:
            raise


def move_project_member(old_path: Path, new_path: Path) -> None:
    """Follow a moved workspace in its project's member registry.

    A move that stays inside the same project updates the recorded relpath; one
    that leaves the project unregisters it; a workspace outside any project is a
    no-op. A sealed project refuses either change.
    """

    old = old_path.expanduser().resolve()
    new = new_path.expanduser().resolve()
    project = discover_project(old)
    if project is None:
        return
    from httk.core.project.members import unregister_project_member, update_project_member_path

    if new.is_relative_to(project):
        update_project_member_path(project, old, new)
    else:
        try:
            unregister_project_member(project, old)
        except ValueError:
            pass


def create_workspace(
    name: str,
    path: str | os.PathLike[str] | None = None,
    *,
    durable: bool = True,
    policy: Mapping[str, object] | None = None,
    settings: Mapping[str, object] | None = None,
) -> WorkspaceBinding:
    """Initialize a local workspace, apply settings, and register its name.

    :param name: Assign this plain name to the initialized workspace.
    :param path: Locate the workspace directory to initialize.
    :param durable: Flush initialization and registry updates durably when true.
    :param policy: Apply workspace policy values during initialization.
    :param settings: Apply workspace settings after initialization.
    :return: The registered local binding.
    :raises TypeError: If the path is omitted.
    :raises ValueError: If the workspace cannot be registered.
    """

    if path is None:
        raise TypeError("create_workspace requires a path")
    location = str(Path(path).expanduser().resolve())
    _refuse_registered(_read_global(), name, location)
    workspace = Workspace.initialize(path, durable=durable, policy=policy)
    for key, value in (settings or {}).items():
        workspace.set_setting(key, value)
    return register_workspace(name, path, durable=durable)


def delete_workspace(name: str, *, force: bool = False) -> WorkspaceBinding:
    """Delete a local workspace and forget its registry entry.

    :param name: Identify the registered workspace to delete.
    :param force: Confirm the destructive deletion.
    :return: The deleted binding.
    :raises ValueError: If deletion is not explicitly forced.
    """
    if not force:
        raise ValueError(f"destroying the workspace {name!r} requires force")
    binding = _local_binding(name)
    assert binding.path is not None
    remove_local_workspace(Path(binding.path))
    return forget_workspace(name, force=True)


def remove_local_workspace(path: Path) -> None:
    """Remove one local execution workspace directory.

    :param path: Locate the workspace directory to remove.
    :raises ValueError: If the path is not an execution workspace.
    """
    resolved = path.expanduser().resolve()
    if not (resolved / WORKSPACE_DIRECTORY / "format.json").is_file():
        raise ValueError(f"not an httk execution workspace: {resolved}")
    _unregister_project_member(resolved)
    shutil.rmtree(resolved)
