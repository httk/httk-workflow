"""The workspace registry: the named, init:ed workspaces the CLI addresses.

Every :command:`httk workflow` command that operates on a workspace takes a
*registered name*, never a bare path. A name is bound once — "init:ed" — to a
place: either the built-in ``local`` remote, or exactly one defined remote, plus
the path the workspace lives at on that machine. The binding is always explicit;
being local is never implied, because a name that silently meant "here" would be
a different workspace on a different machine.

A binding lives in one of two scopes:

* **global** — ``$XDG_CONFIG_HOME/httk/workspaces.json``, shared by every
  project this user works in.
* **project** — the ``workspaces`` member of ``httk_project/project.json``,
  travelling with the project.

A project binding shadows a global one of the same name, exactly as a
project-local remote shadows a global remote: the more specific definition wins.

This registry is the *CLI and UX contract*. The Python API deliberately keeps
:class:`~httk.workflow.workspace.Workspace`, which a library caller constructs
straight from a path; the registry names those paths for the command line and
records which machine each lives on. The wire protocol a remote adapter speaks
stays path-based — the client resolves a name to its remote path and sends the
path — so registration is a purely client-side concept a remote never sees.
"""

import json
import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ._util import write_json_atomic
from .adapters import resolve_remote, run_adapter, seed_application_settings
from .configuration import config_home
from .projects import (
    discover_project,
    read_project_section,
    require_project,
    write_project_section,
)
from .workspace import Workspace

__all__ = [
    "LOCAL_REMOTE",
    "REMOTE_WORKSPACE_DELETE_COMMAND",
    "REMOTE_WORKSPACE_INIT_COMMAND",
    "WORKSPACES_FILE",
    "WORKSPACE_SECTION",
    "WorkspaceBinding",
    "create_workspace",
    "delete_workspace",
    "forget_workspace",
    "list_workspaces",
    "register_workspace",
    "remove_local_workspace",
    "resolve_workspace",
    "valid_workspace_name",
    "workspaces_path",
]

#: The reserved name of the built-in remote that means "this machine". It is a
#: real, always-resolvable remote name a binding may carry, and defining a remote
#: called this is refused elsewhere so the two can never disagree.
LOCAL_REMOTE = "local"

#: Where the global registry is written.
WORKSPACES_FILE = "workspaces.json"

#: The ``project.json`` member the project-local registry lives in.
WORKSPACE_SECTION = "workspaces"

_REGISTRY_FORMAT = "httk-workspaces"
_REGISTRY_FORMAT_VERSION = 1

#: The frozen command a client runs on a remote to create or destroy a workspace
#: there. Like the transfer choreography's vectors, these are protocol: they
#: carry a path and the ``--by-path`` switch, because the far side has no
#: registry and addresses its own workspaces by path.
REMOTE_WORKSPACE_INIT_COMMAND = ("httk", "workflow", "workspace", "init")
REMOTE_WORKSPACE_DELETE_COMMAND = ("httk", "workflow", "workspace", "delete")


@dataclass(frozen=True)
class WorkspaceBinding:
    """Where one registered workspace name resolves to."""

    name: str
    remote: str
    path: str
    scope: str


def valid_workspace_name(name: str) -> str:
    """Return *name* if it is a legal workspace name, else raise.

    A workspace name is validated like every other httk name: it is nonempty,
    holds no path separator, and is neither ``.`` nor ``..`` — so it can never be
    read as a path, which is exactly the ambiguity the registry removes.
    """

    if not name or "/" in name or name in {".", ".."}:
        raise ValueError(f"invalid workspace name: {name!r}")
    return name


def workspaces_path() -> Path:
    """Return where this user's global workspace registry is written."""

    return config_home() / WORKSPACES_FILE


def _read_global() -> dict[str, dict[str, str]]:
    """Return the global registry's bindings, empty when the file is absent."""

    path = workspaces_path()
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as stream:
        document = json.load(stream)
    if not isinstance(document, dict):
        raise ValueError(f"workspace registry is not a JSON object: {path}")
    recorded = document.get("format")
    if recorded is not None and recorded != _REGISTRY_FORMAT:
        raise ValueError(f"workspace registry is not a {_REGISTRY_FORMAT} document but {recorded!r}: {path}")
    workspaces = document.get("workspaces", {})
    if not isinstance(workspaces, dict):
        raise ValueError(f"workspace registry workspaces member is not an object: {path}")
    return {name: dict(binding) for name, binding in workspaces.items() if isinstance(binding, Mapping)}


def _write_global(workspaces: Mapping[str, Mapping[str, str]]) -> Path:
    """Write the global registry."""

    path = workspaces_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(
        path,
        {
            "format": _REGISTRY_FORMAT,
            "format_version": _REGISTRY_FORMAT_VERSION,
            "workspaces": {name: dict(binding) for name, binding in workspaces.items()},
        },
    )
    return path


def _read_project(project_root: Path) -> dict[str, dict[str, str]]:
    """Return the project-local registry's bindings."""

    section = read_project_section(project_root, WORKSPACE_SECTION)
    return {name: dict(binding) for name, binding in section.items() if isinstance(binding, Mapping)}


def _write_project(project_root: Path, workspaces: Mapping[str, Mapping[str, str]]) -> None:
    """Write the project-local registry."""

    write_project_section(
        project_root, WORKSPACE_SECTION, {name: dict(binding) for name, binding in workspaces.items()}
    )


def _binding(name: str, raw: Mapping[str, object], scope: str) -> WorkspaceBinding:
    """Return the validated binding one stored record denotes."""

    remote = raw.get("remote")
    path = raw.get("path")
    if not isinstance(remote, str) or not remote:
        raise ValueError(f"workspace {name!r} is missing its remote")
    if not isinstance(path, str) or not path:
        raise ValueError(f"workspace {name!r} is missing its path")
    return WorkspaceBinding(name=name, remote=remote, path=path, scope=scope)


def _validate_remote(remote: str, *, project: str | os.PathLike[str] | None) -> None:
    """Refuse a remote that is neither the built-in ``local`` nor defined.

    A binding names exactly one place. Binding a name to a remote that does not
    exist would defer the failure to first use, so it is refused at registration
    while the operator is still looking at the command they typed.
    """

    if remote == LOCAL_REMOTE:
        return
    resolve_remote(remote, project=project)


def register_workspace(
    name: str,
    remote: str,
    path: str | os.PathLike[str],
    *,
    scope: str = "global",
    project: str | os.PathLike[str] | None = None,
) -> WorkspaceBinding:
    """Record one name -> (remote, path) binding in the chosen scope.

    Registration does not touch the workspace itself; it only records where a
    name resolves. :func:`create_workspace` is what also brings the workspace
    into being. A name already registered in the same scope is refused, so a
    binding is never silently replaced.
    """

    valid_workspace_name(name)
    if scope not in {"global", "project"}:
        raise ValueError(f"unknown registry scope: {scope!r}")
    _validate_remote(remote, project=project)
    record = {"remote": remote, "path": str(Path(path).expanduser())}
    if scope == "global":
        workspaces = _read_global()
        if name in workspaces:
            raise ValueError(f"workspace {name!r} is already registered globally; forget it first")
        workspaces[name] = record
        _write_global(workspaces)
    else:
        project_root = require_project(project)
        workspaces = _read_project(project_root)
        if name in workspaces:
            raise ValueError(f"workspace {name!r} is already registered in this project; forget it first")
        workspaces[name] = record
        _write_project(project_root, workspaces)
    return WorkspaceBinding(name=name, remote=remote, path=record["path"], scope=scope)


def forget_workspace(
    name: str,
    *,
    scope: str | None = None,
    project: str | os.PathLike[str] | None = None,
) -> WorkspaceBinding:
    """Remove one binding and return it, deregistering only.

    Without an explicit *scope* the project binding is removed before the global
    one, matching the precedence resolution uses, so ``forget`` undoes what the
    name currently resolves to. The workspace on disk is left untouched;
    :func:`delete_workspace` is what also destroys it.
    """

    project_root = discover_project(project)
    if scope in (None, "project") and project_root is not None:
        workspaces = _read_project(project_root)
        if name in workspaces:
            removed = _binding(name, workspaces.pop(name), "project")
            _write_project(project_root, workspaces)
            return removed
        if scope == "project":
            raise ValueError(f"workspace {name!r} is not registered in this project")
    if scope in (None, "global"):
        workspaces = _read_global()
        if name in workspaces:
            removed = _binding(name, workspaces.pop(name), "global")
            _write_global(workspaces)
            return removed
    raise ValueError(f"workspace {name!r} is not registered")


def resolve_workspace(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
) -> WorkspaceBinding:
    """Resolve one registered name, project bindings shadowing global ones."""

    project_root = discover_project(project)
    if project_root is not None:
        workspaces = _read_project(project_root)
        if name in workspaces:
            return _binding(name, workspaces[name], "project")
    workspaces = _read_global()
    if name in workspaces:
        return _binding(name, workspaces[name], "global")
    raise ValueError(
        f"unknown workspace: {name}; register it with `httk workflow workspace init` "
        "or list the registered ones with `httk workflow workspace list`"
    )


def list_workspaces(*, project: str | os.PathLike[str] | None = None) -> list[WorkspaceBinding]:
    """Return every registered binding, project entries shadowing global ones."""

    rows: dict[str, WorkspaceBinding] = {}
    for name, raw in _read_global().items():
        rows[name] = _binding(name, raw, "global")
    project_root = discover_project(project)
    if project_root is not None:
        for name, raw in _read_project(project_root).items():
            rows[name] = _binding(name, raw, "project")
    return [rows[name] for name in sorted(rows)]


def is_local(binding: WorkspaceBinding) -> bool:
    """Report whether one binding lives on this machine."""

    return binding.remote == LOCAL_REMOTE


def create_workspace(
    name: str,
    *,
    remote: str,
    path: str | os.PathLike[str],
    scope: str = "global",
    project: str | os.PathLike[str] | None = None,
    durable: bool = True,
    policy: Mapping[str, object] | None = None,
    settings: Mapping[str, object] | None = None,
    adapter_timeout: float | None = None,
) -> WorkspaceBinding:
    """Create a workspace on its remote and register it under *name*.

    A local binding initializes the workspace here through
    :meth:`~httk.workflow.workspace.Workspace.initialize` and applies any application *settings*. A remote
    binding creates the workspace on the far side over the adapter — the frozen
    ``workspace init ... --by-path`` spelling — first seeding it with the remote
    definition's whitelisted queue settings and then any explicit *settings*, and
    records the binding locally. The name is refused if it is already registered
    in the target scope, before anything is created.
    """

    valid_workspace_name(name)
    _guard_unregistered(name, scope=scope, project=project)
    explicit = dict(settings or {})
    if remote == LOCAL_REMOTE:
        workspace = Workspace.initialize(path, durable=durable, policy=policy)
        for key, value in explicit.items():
            workspace.set_setting(key, value)
    else:
        target = resolve_remote(remote, project=project)
        seeds = seed_application_settings(target.bundle, target.queue)
        merged = {**seeds, **explicit}
        argv = [*REMOTE_WORKSPACE_INIT_COMMAND, str(path), "--by-path"]
        for key, value in merged.items():
            argv += ["--setting", f"{key}={json.dumps(value)}"]
        result = run_adapter(
            target.bundle,
            "invoke",
            {"queue": target.queue, "argv": argv},
            timeout=adapter_timeout,
        )
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace init failed: {result.get('stderr', '')}")
    return register_workspace(name, remote, path, scope=scope, project=project)


def delete_workspace(
    name: str,
    *,
    project: str | os.PathLike[str] | None = None,
    force: bool = False,
    adapter_timeout: float | None = None,
) -> WorkspaceBinding:
    """Destroy a registered workspace and deregister it.

    Destruction is irreversible, so it is refused without *force*. A local
    binding removes the workspace directory here; a remote binding runs the
    frozen ``workspace delete ... --by-path --force`` spelling over the adapter.
    The binding is forgotten either way, so a name never outlives its workspace.
    """

    if not force:
        raise ValueError(f"destroying the workspace {name!r} requires force")
    binding = resolve_workspace(name, project=project)
    if binding.remote == LOCAL_REMOTE:
        remove_local_workspace(Path(binding.path))
    else:
        target = resolve_remote(binding.remote, project=project)
        argv = [*REMOTE_WORKSPACE_DELETE_COMMAND, binding.path, "--by-path", "--force"]
        result = run_adapter(
            target.bundle,
            "invoke",
            {"queue": target.queue, "argv": argv},
            timeout=adapter_timeout,
        )
        if result.get("returncode") != 0:
            raise RuntimeError(f"remote workspace delete failed: {result.get('stderr', '')}")
    return forget_workspace(name, scope=binding.scope, project=project)


def remove_local_workspace(path: Path) -> None:
    """Remove one local workspace directory, refusing a foreign one."""

    resolved = path.expanduser().resolve()
    if not (resolved / ".httk-workflow" / "format.json").is_file():
        raise ValueError(f"not an httk workflow workspace: {resolved}")
    shutil.rmtree(resolved)


def _guard_unregistered(
    name: str,
    *,
    scope: str,
    project: str | os.PathLike[str] | None,
) -> None:
    """Refuse a name already registered in the scope create() would write to."""

    if scope == "global":
        if name in _read_global():
            raise ValueError(f"workspace {name!r} is already registered globally; forget it first")
    else:
        project_root = require_project(project)
        if name in _read_project(project_root):
            raise ValueError(f"workspace {name!r} is already registered in this project; forget it first")
