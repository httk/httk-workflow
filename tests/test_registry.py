"""The workspace registry: the named, init:ed workspaces the CLI addresses.

Every command now takes a *registered name*, never a bare path, so the registry
is the contract those names resolve through. These tests pin that contract: a
name binds to exactly one place in one scope, a project binding shadows a global
one, the built-in ``local`` remote is always resolvable and can never be
redefined, and creating or destroying a workspace acts on the filesystem while
registration only records where a name points.
"""

import json
from pathlib import Path

import pytest
from httk.core.project import initialize_project as initialize_anchor

from httk.workflow import Workspace
from httk.workflow.adapters import add_remote
from httk.workflow.configuration import data_home
from httk.workflow.projects import initialize_project
from httk.workflow.registry import (
    DEFAULT_WORKSPACE_NAME,
    LOCAL_REMOTE,
    WorkspaceBinding,
    create_workspace,
    default_workspace,
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
    default = resolve_workspace(DEFAULT_WORKSPACE_NAME, project=project)
    assert default.scope == "project" and default.path == str(project)
    # A project binding may name any defined remote; define one so a remote-bound
    # binding validates, alongside the local binding this round-trips.
    add_remote("cluster-like", template="local", project=project)
    register_workspace("cluster-like:station", "cluster-like", tmp_path / "runs", scope="project", project=project)

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


def test_remote_names_cannot_contain_colons(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="colon-remote")
    with pytest.raises(ValueError, match="workspace bindings"):
        add_remote("ka:ppa", template="local", project=project)


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


def test_a_colon_is_reserved_in_workspace_names() -> None:
    with pytest.raises(ValueError, match="reserved"):
        valid_workspace_name("a:b")


def test_default_workspace_creates_and_registers_at_a_project_root(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_anchor(project, name="default-project")

    binding = default_workspace(project=project)

    assert binding == WorkspaceBinding(DEFAULT_WORKSPACE_NAME, LOCAL_REMOTE, str(project), "project")
    document = json.loads((project / "httk_project" / "project.json").read_text(encoding="utf-8"))
    assert document["workspaces"][DEFAULT_WORKSPACE_NAME] == {"remote": LOCAL_REMOTE, "path": str(project)}


def test_default_workspace_without_a_project_uses_the_global_data_home(tmp_path: Path) -> None:
    binding = default_workspace(project=tmp_path)

    assert binding.scope == "global"
    assert Path(binding.path) == data_home() / "workspace"
    assert (Path(binding.path) / ".httk-workflow" / "format.json").is_file()
    assert resolve_workspace(DEFAULT_WORKSPACE_NAME) == binding


def test_default_workspace_is_idempotent(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_anchor(project, name="idempotent")

    first = default_workspace(project=project)
    workspace_id = Workspace(first.path).workspace_id
    second = default_workspace(project=project)

    assert second == first
    assert Workspace(second.path).workspace_id == workspace_id


def test_default_workspace_adopts_an_existing_project_workspace(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_anchor(project, name="adopt")
    existing = Workspace.initialize(project)

    binding = default_workspace(project=project)

    assert binding.scope == "project" and Path(binding.path) == project
    assert Workspace(binding.path).workspace_id == existing.workspace_id


def test_default_workspace_reports_a_crash_left_partial_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import registry

    monkeypatch.setattr(registry, "data_home", lambda: tmp_path)
    control = tmp_path / "workspace" / ".httk-workflow"
    control.mkdir(parents=True)
    with pytest.raises(ValueError, match=r"partial directory.*\.httk-workflow.*delete"):
        registry.default_workspace()


def test_default_workspace_adopts_a_workspace_that_won_the_initialization_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import registry

    monkeypatch.setattr(registry, "data_home", lambda: tmp_path)
    real_initialize = Workspace.initialize

    def initialize_then_report_race(cls, root, **kwargs):
        real_initialize(root, **kwargs)
        raise FileExistsError(root)

    monkeypatch.setattr(Workspace, "initialize", classmethod(initialize_then_report_race))
    binding = registry.default_workspace()
    assert Path(binding.path).joinpath(".httk-workflow", "format.json").is_file()


def test_default_workspace_preserves_an_explicit_project_binding(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_anchor(project, name="explicit-default")
    elsewhere = tmp_path / "elsewhere"
    explicit = register_workspace(
        DEFAULT_WORKSPACE_NAME,
        LOCAL_REMOTE,
        elsewhere,
        scope="project",
        project=project,
    )

    assert default_workspace(project=project) == explicit


def test_project_default_ignores_an_existing_global_default(tmp_path: Path) -> None:
    global_binding = default_workspace(project=tmp_path / "outside")
    project = tmp_path / "project"
    initialize_anchor(project, name="project-default")

    project_binding = default_workspace(project=project)

    assert global_binding.scope == "global"
    assert project_binding.scope == "project"
    assert Path(project_binding.path) == project
    assert project_binding != global_binding
    assert json.loads((project / "httk_project" / "project.json").read_text(encoding="utf-8"))["workspaces"][
        DEFAULT_WORKSPACE_NAME
    ] == {"remote": LOCAL_REMOTE, "path": str(project)}


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


@pytest.mark.parametrize("key", ["bad=name", "bad/name", "bad name", "bad\x00name", "1bad"])
def test_setting_names_are_dotted_identifiers(tmp_path: Path, key: str) -> None:
    workspace = Workspace.initialize(tmp_path / "settings")
    with pytest.raises(ValueError, match="dotted identifier"):
        workspace.set_setting(key, "x")


def test_setting_values_reject_nul_and_derived_name_collisions(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "settings")
    with pytest.raises(ValueError, match="NUL"):
        workspace.set_setting("answer", "bad\x00value")
    workspace.set_setting("a.b", "first")
    with pytest.raises(ValueError, match=r"a\.b.*a_b|a_b.*a\.b"):
        workspace.set_setting("a_b", "second")
    workspace.set_setting("a.b", "updated")


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
