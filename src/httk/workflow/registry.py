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


def _unknown(name: str) -> ValueError:
    return ResolutionMiss(
        f"unknown workspace: {name}; register it with `httk workflow workspace init` "
        "or list the registered ones with `httk workflow workspace list`"
    )


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
    return WorkspaceBinding(name, LOCAL_REMOTE, location)


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
        return _local_binding(name)
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
    """Remove one local workflow workspace directory.

    :param path: Locate the workspace directory to remove.
    :raises ValueError: If the path is not a workflow workspace.
    """
    resolved = path.expanduser().resolve()
    if not (resolved / WORKSPACE_DIRECTORY / "format.json").is_file():
        raise ValueError(f"not an httk workflow workspace: {resolved}")
    shutil.rmtree(resolved)
