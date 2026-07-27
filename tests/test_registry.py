"""The workspace registry: the named, init:ed workspaces the CLI addresses.

Every command now takes a *registered name*, never a bare path, so the registry
is the contract those names resolve through. These tests pin that contract: a
name binds to exactly one place in one scope, a project binding shadows a global
one, the built-in ``local`` remote is always resolvable and can never be
redefined, and creating or destroying a workspace acts on the filesystem while
registration only records where a name points.
"""

from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]

from httk.workflow.adapters import add_remote
from httk.workflow.projects import initialize_project
from httk.workflow.registry import (
    LOCAL_REMOTE,
    WorkspaceBinding,
    create_workspace,
    delete_workspace,
    forget_workspace,
    list_workspaces,
    register_workspace,
    remove_local_workspace,
    resolve_workspace,
    valid_workspace_name,
)


def test_a_global_binding_registers_resolves_and_is_forgotten(tmp_path: Path) -> None:
    """A name bound globally resolves to its place, and forgetting it removes the
    binding without touching anything on disk."""

    binding = register_workspace("home", LOCAL_REMOTE, tmp_path / "runs")
    assert binding == WorkspaceBinding(name="home", remote="local", path=str(tmp_path / "runs"), scope="global")
    assert resolve_workspace("home") == binding

    forgotten = forget_workspace("home")
    assert forgotten.name == "home"
    with pytest.raises(ValueError, match="unknown workspace: home"):
        resolve_workspace("home")


def test_a_project_binding_registers_resolves_and_is_forgotten(tmp_path: Path) -> None:
    """A project-scoped binding lives in the project and resolves against it."""

    project = tmp_path / "project"
    initialize_project(project, name="scoped")
    # A project binding may name any defined remote; define one so a remote-bound
    # binding validates, alongside the local binding this round-trips.
    add_remote("cluster-like", template="local", project=project)
    register_workspace("station", "cluster-like", tmp_path / "runs", scope="project", project=project)

    register_workspace("local-ws", LOCAL_REMOTE, tmp_path / "here", scope="project", project=project)
    resolved = resolve_workspace("local-ws", project=project)
    assert resolved.scope == "project" and resolved.remote == LOCAL_REMOTE

    forget_workspace("local-ws", scope="project", project=project)
    with pytest.raises(ValueError):
        resolve_workspace("local-ws", project=project)


def test_a_project_binding_shadows_a_global_one_of_the_same_name(tmp_path: Path) -> None:
    """The more specific definition wins: a project binding hides a global one, in
    both point resolution and the merged listing."""

    project = tmp_path / "project"
    initialize_project(project, name="shadow")
    register_workspace("shared", LOCAL_REMOTE, tmp_path / "global-place")
    register_workspace("shared", LOCAL_REMOTE, tmp_path / "project-place", scope="project", project=project)

    resolved = resolve_workspace("shared", project=project)
    assert resolved.scope == "project"
    assert resolved.path == str(tmp_path / "project-place")

    listed = {binding.name: binding for binding in list_workspaces(project=project)}
    assert listed["shared"].scope == "project"
    assert listed["shared"].path == str(tmp_path / "project-place")

    # Away from the project, the global binding is what "shared" means.
    assert resolve_workspace("shared").path == str(tmp_path / "global-place")


def test_the_built_in_local_remote_resolves_without_being_defined(tmp_path: Path) -> None:
    """`local` is a real, always-resolvable remote a binding may carry, even though
    no remote by that name is ever defined."""

    binding = register_workspace("here", LOCAL_REMOTE, tmp_path / "runs")
    assert binding.remote == LOCAL_REMOTE
    assert resolve_workspace("here").remote == LOCAL_REMOTE


def test_defining_a_remote_named_local_is_refused(tmp_path: Path) -> None:
    """Defining a remote called `local` would make a binding to it ambiguous, so
    the name is reserved for the built-in local remote."""

    project = tmp_path / "project"
    initialize_project(project, name="reserved")
    with pytest.raises(ValueError, match="reserved"):
        add_remote("local", template="local", project=project)


def test_binding_to_an_undefined_remote_is_refused_at_registration(tmp_path: Path) -> None:
    """A name binds to exactly one place; a remote that does not exist is refused
    while the operator is still looking at the command, not deferred to first use."""

    with pytest.raises(ValueError):
        register_workspace("ghost", "no-such-remote", tmp_path / "runs")


def test_a_name_already_registered_in_the_same_scope_is_refused(tmp_path: Path) -> None:
    """A binding is never silently replaced: re-registering a name is refused."""

    register_workspace("home", LOCAL_REMOTE, tmp_path / "a")
    with pytest.raises(ValueError, match="already registered"):
        register_workspace("home", LOCAL_REMOTE, tmp_path / "b")


def test_forgetting_an_unknown_name_is_refused(tmp_path: Path) -> None:
    """Forgetting a name that was never registered is an error, not a no-op."""

    with pytest.raises(ValueError, match="not registered"):
        forget_workspace("nobody")


@pytest.mark.parametrize("name", ["", "a/b", ".", "..", "runs/ws"])
def test_an_illegal_workspace_name_is_refused(name: str) -> None:
    """A workspace name can never be read as a path, which is the ambiguity the
    registry exists to remove."""

    with pytest.raises(ValueError, match="invalid workspace name"):
        valid_workspace_name(name)


def test_create_local_workspace_initializes_the_directory_and_applies_settings(tmp_path: Path) -> None:
    """Creating a local workspace both brings it into being and registers it, and
    the application settings passed at creation land in the workspace."""

    binding = create_workspace(
        "home",
        remote=LOCAL_REMOTE,
        path=tmp_path / "runs",
        settings={"vasp.command": "srun vasp_std"},
    )
    assert binding.remote == LOCAL_REMOTE
    assert (Path(binding.path) / ".httk-workflow" / "format.json").is_file()
    assert resolve_workspace("home").path == str(tmp_path / "runs")

    from httk.workflow import Workspace

    assert Workspace(binding.path).settings["vasp.command"] == "srun vasp_std"


def test_deleting_a_local_workspace_requires_force_then_destroys_and_deregisters(tmp_path: Path) -> None:
    """Destruction is irreversible, so it is refused without force; with force the
    directory is removed and the name never outlives its workspace."""

    create_workspace("home", remote=LOCAL_REMOTE, path=tmp_path / "runs")
    with pytest.raises(ValueError, match="requires force"):
        delete_workspace("home")
    assert (tmp_path / "runs").is_dir()

    delete_workspace("home", force=True)
    assert not (tmp_path / "runs").exists()
    with pytest.raises(ValueError, match="unknown workspace"):
        resolve_workspace("home")


def test_removing_a_directory_that_is_not_a_workspace_is_refused(tmp_path: Path) -> None:
    """The low-level removal refuses a foreign directory, so a mis-registered path
    can never delete something that is not an httk workspace."""

    foreign = tmp_path / "not-a-workspace"
    foreign.mkdir()
    (foreign / "keepme").write_text("important", encoding="utf-8")
    with pytest.raises(ValueError, match="not an httk workflow workspace"):
        remove_local_workspace(foreign)
    assert (foreign / "keepme").is_file()
