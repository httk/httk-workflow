"""Declared sub-workflow calls: ``[workflow.calls]``, installed with the workflow, ready before a job starts.

A workflow names the workflows it calls in its manifest. Installing it installs
(or finds) each of them, recorded alias to installed id; a job may call only
those, and is not claimed by a manager until each is installed and, when
compiled, built on the manager's machine.
"""

import json
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from attempt_fixtures import every_job, find, run_manager
from conftest import register_ws
from httk.workflow import Workspace, _kernel, _store, scaffold
from httk.workflow._job import JobDefinition
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import new_job
from httk.workflow.workflow_cli import command

_SRC = str(Path(__file__).parents[1] / "src")

_PARENT = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.calls.parent")


@run.step
def start(a):
    a.call(a.parameter("target", "child"), label="child")
    a.gather("done", when="all_succeeded")


@run.step
def done(a):
    a.succeed()


raise SystemExit(run.main())
'''

_CHILD = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.calls.child")


@run.step
def work(a):
    a.succeed()


raise SystemExit(run.main())
'''


def _write_package(root: Path, manifest: str, runner: str) -> None:
    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (root / "run.py").write_text(runner, encoding="utf-8")
    (root / "run.py").chmod(0o755)


def _packages(root: Path, *, calls: str = 'child = "tests.calls.child"') -> Path:
    """Write a compiled child package and a parent package that declares calling it; return their directory."""

    _write_package(
        root / "child",
        '[workflow]\nname = "tests.calls.child"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["work"]\n\n'
        '[workflow.build]\ncommand = "./build.sh"\nartifacts = ["stamp"]\n',
        _CHILD,
    )
    (root / "child" / "build.sh").write_text("#!/bin/sh\nset -eu\necho built > stamp\n", encoding="utf-8")
    (root / "child" / "build.sh").chmod(0o755)
    _write_package(
        root / "parent",
        '[workflow]\nname = "tests.calls.parent"\n\n[workflow.runner]\nentry = "run.py"\n'
        f'steps = ["start", "done"]\n\n[workflow.calls]\n{calls}\n',
        _PARENT,
    )
    return root


def _install(workspace: Workspace, source: Path, **options: object) -> _store.Installed:
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        return _store.install(workspace, owner, source, **options)  # type: ignore[arg-type]
    finally:
        owner.close()


@pytest.fixture(autouse=True)
def _fresh() -> Iterator[None]:
    yield
    for name in ("tests.calls.parent", "tests.calls.child", "tests.calls.middle"):
        scaffold._WORKFLOW_PROVIDERS.pop(name, None)


@pytest.fixture()
def workspace(tmp_path: Path) -> Workspace:
    return Workspace.initialize(tmp_path / "workspace", durable=False)


def test_a_call_must_name_a_workflow_not_a_path(tmp_path: Path) -> None:
    root = _packages(tmp_path / "packages", calls='child = "../child"')
    with pytest.raises(ValueError, match="must name a workflow or a git URI, not a path"):
        parse_workflow_manifest(root / "parent")


def test_a_workflow_whose_declared_call_is_unknown_cannot_be_installed(tmp_path: Path, workspace: Workspace) -> None:
    root = _packages(tmp_path / "packages", calls='child = "tests.calls.nowhere"')
    with pytest.raises(ValueError, match="no workflow package, git URI, runner file or known workflow name"):
        new_job(workspace, root / "parent", install=True)
    assert _store.list_installed(workspace) == [] and every_job(workspace) == []


def test_a_job_waits_unclaimed_until_its_called_workflow_is_built(tmp_path: Path, workspace: Workspace) -> None:
    root = _packages(tmp_path / "packages")
    child = _install(workspace, root / "child", build=False)
    parent = _install(workspace, root / "parent")
    assert parent.record["calls"] == {"child": child.id}
    job = new_job(workspace, "tests.calls.parent")

    # The child is compiled and not built here: the parent is never claimed.
    run_manager(workspace)
    assert find(workspace, job.job_id).state == "ready"
    assert _store.closure(workspace, parent.id, check_builds=True).unbuilt == (child.id,)

    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        _store.build(workspace, owner, child.id)
    finally:
        owner.close()
    run_manager(workspace)
    assert {ref.state for ref in every_job(workspace)} == {"succeeded"}
    children = [ref for ref in every_job(workspace) if ref.job_id != job.job_id]
    assert [JobDefinition.from_path(ref.path / "job.json").workflow_id for ref in children] == [child.id]


def test_a_job_may_call_only_what_its_workflow_declares(tmp_path: Path, workspace: Workspace) -> None:
    root = _packages(tmp_path / "packages")
    _install(workspace, root / "child")
    _install(workspace, root / "parent")
    job = new_job(workspace, "tests.calls.parent", parameters={"target": "tests.calls.elsewhere"})
    run_manager(workspace)
    failed = find(workspace, job.job_id)
    assert failed.state == "failed"
    errors = list(failed.path.glob("attempts/*/error.json"))
    assert any("is not declared in [workflow.calls]" in path.read_text(encoding="utf-8") for path in errors)


@pytest.mark.parametrize(
    ("calls", "message"),
    [
        ({"child": "git+https://example.test/repo@main#x"}, "must pin its git URI to a full commit hash"),
        ({"child": "tests.calls\tchild"}, "without whitespace"),
        ({"child": "../child"}, "not a path"),
        ({"a": "b", "b": "c"}, "alias 'b' is also another call's reference"),
    ],
)
def test_declared_calls_are_validated(calls: dict[str, str], message: str) -> None:
    from httk.workflow.errors import FormatError
    from httk.workflow.models import validate_calls

    with pytest.raises(FormatError, match=message):
        validate_calls(calls, "[workflow.calls]")


def test_a_call_name_never_resolves_as_a_directory_where_the_workflow_is_installed(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _packages(tmp_path / "packages", calls='child = "childpkg"')
    # A package directory of that name in the working directory is not a workflow name.
    _write_package(
        tmp_path / "cwd" / "childpkg",
        '[workflow]\nname = "childpkg"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["work"]\n',
        _CHILD,
    )
    monkeypatch.chdir(tmp_path / "cwd")
    with pytest.raises(
        ValueError, match="no workflow package, git URI, runner file or known workflow name: 'childpkg'"
    ):
        _install(workspace, root / "parent")
    assert _store.list_installed(workspace) == []


def test_calls_are_installed_and_checked_transitively(tmp_path: Path, workspace: Workspace) -> None:
    root = _packages(tmp_path / "packages", calls='middle = "tests.calls.middle"')
    _write_package(
        root / "middle",
        '[workflow]\nname = "tests.calls.middle"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["work"]\n\n'
        '[workflow.calls]\nchild = "tests.calls.child"\n',
        _CHILD.replace("tests.calls.child", "tests.calls.middle"),
    )
    child = _install(workspace, root / "child", build=False)
    middle = _install(workspace, root / "middle")
    parent = _install(workspace, root / "parent")
    # The parent calls only the middle workflow, whose own call is a compiled, unbuilt child.
    report = _store.closure(workspace, parent.id, check_builds=True)
    assert report.missing == () and report.unbuilt == (child.id,)
    assert middle.record["calls"] == {"child": child.id}


def test_declaring_calls_leaves_the_workflow_alias_alone(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_packages(tmp_path / "packages") / "parent")
    assert provider.alias is None and provider.calls == {"child": "tests.calls.child"}


_INTERFACE = {
    "inputs": [{"name": "structure", "entry_type": "structures", "ref": "https://example.test/structure"}],
    "outputs": [
        {"name": "forces", "entry_type": "records", "ref": "https://example.test/forces", "product_of": "structure"}
    ],
}
_BUILD = '\n[workflow.build]\ncommand = "./build.sh"\nartifacts = ["stamp"]\n'


def _slotted(root: Path, *, default: bool = True, extra: str = "") -> Path:
    """Write the packages with a parent whose call slot defaults to the child, which is known by name."""

    slot = '[workflow.calls.forces]\ndeclaration_file = "forces.json"\n'
    _packages(root, calls=slot + ('default = "tests.calls.child"\n' if default else "") + extra)
    (root / "parent" / "forces.json").write_text(json.dumps(_INTERFACE), encoding="utf-8")
    shutil.copy2(root / "child" / "build.sh", root / "parent" / "build.sh")
    scaffold.register_workflow(parse_workflow_manifest(root / "child"))
    return root


def test_a_slot_default_is_never_installed_and_an_absent_one_gets_a_notice(
    tmp_path: Path, workspace: Workspace, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _slotted(tmp_path / "packages")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "w")
    assert command(["install", "--workspace", name, "--no-build", str(root / "parent")], context) == 0
    notice = (
        "notice: the default workflow tests.calls.child of call slot 'forces' (tests.calls.parent) is not "
        "installed; install it (httk workflow install tests.calls.child) or bind another with --call forces=WORKFLOW"
    )
    assert notice in capsys.readouterr().err
    parent = _store.lookup(workspace, "tests.calls.parent")
    assert (
        parent is not None
        and parent.record["calls"] == {}
        and parent.record["slots"] == {"forces": "tests.calls.child"}
    )
    assert [installed.id for installed in _store.list_installed(workspace)] == [parent.id]

    # Installed first, the default is recorded by its id, with no notice.
    child = _install(workspace, root / "child", build=False)
    assert command(["install", "--workspace", name, "--no-build", str(root / "parent")], context) == 0
    assert "notice:" not in capsys.readouterr().err
    parent = _store.lookup(workspace, "tests.calls.parent")
    assert parent is not None and parent.record["slots"] == {"forces": child.id}
    # A job may rebind the slot, so the default is no dependency: the unbuilt child does not block the parent.
    assert set(_store.resolve_calls(workspace, [parent.id])) == {parent.id}
    assert set(_store.resolve_calls(workspace, [parent.id], include_slot_defaults=True)) == {parent.id, child.id}
    assert _store.closure(workspace, parent.id, check_builds=True).ok


def test_a_slot_without_a_default_is_recorded_as_null(tmp_path: Path, workspace: Workspace) -> None:
    root = _slotted(tmp_path / "packages", default=False)
    parent = _install(workspace, root / "parent")
    assert parent.record["slots"] == {"forces": None}
    assert set(_store.resolve_calls(workspace, [parent.id], include_slot_defaults=True)) == {parent.id}
    assert _store.closure(workspace, parent.id, check_builds=True).ok
    assert [installed.id for installed in _store.list_installed(workspace)] == [parent.id]


def test_workflow_build_also_builds_installed_slot_defaults(tmp_path: Path, workspace: Workspace) -> None:
    root = _slotted(tmp_path / "packages", extra=_BUILD)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "w")
    # A default that is not installed is skipped.
    parent = _install(workspace, root / "parent", build=False)
    assert command(["build", "--workspace", name, parent.id], context) == 0
    assert _store.built(parent) is True and _store.list_installed(workspace) == [parent]
    child = _install(workspace, root / "child", build=False)
    parent = _install(workspace, root / "parent", build=False)
    assert _store.built(child) is False
    assert command(["build", "--workspace", name, parent.id], context) == 0
    assert _store.built(child) is True and _store.built(parent) is True


def test_a_fixed_call_target_must_give_every_slot_a_default(tmp_path: Path, workspace: Workspace) -> None:
    root = _packages(tmp_path / "packages", calls='middle = "tests.calls.middle"')
    _write_package(
        root / "middle",
        '[workflow]\nname = "tests.calls.middle"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["work"]\n\n'
        '[workflow.calls.forces]\ndeclaration_file = "forces.json"\n',
        _CHILD.replace("tests.calls.child", "tests.calls.middle"),
    )
    (root / "middle" / "forces.json").write_text(json.dumps(_INTERFACE), encoding="utf-8")
    scaffold.register_workflow(parse_workflow_manifest(root / "middle"))
    message = (
        r"\[workflow\.calls\]\.middle of local:tests\.calls\.parent names local:tests\.calls\.middle, "
        r"whose call slot 'forces' has no default"
    )
    with pytest.raises(ValueError, match=message):
        _install(workspace, root / "parent")
    assert _store.list_installed(workspace) == []
    # Installed on its own first (a root workflow may leave a slot without a default), it is refused all the same.
    _install(workspace, root / "middle")
    with pytest.raises(ValueError, match=message):
        _install(workspace, root / "parent")


def test_a_copied_slot_workflow_installs_and_an_old_record_without_slots_still_reads(
    tmp_path: Path, workspace: Workspace
) -> None:
    root = _slotted(tmp_path / "packages")
    child = _install(workspace, root / "child", build=False)
    parent = _install(workspace, root / "parent", build=False)
    landing = tmp_path / "landing"
    for installed in (parent, child):
        shutil.copytree(installed.directory, landing / installed.directory.name)
    # The child was installed before call slots existed: its record has no slots member.
    record_path = landing / child.directory.name / "install.json"
    old = json.loads(record_path.read_text(encoding="utf-8"))
    del old["slots"]
    record_path.write_text(json.dumps(old), encoding="utf-8")

    target = Workspace.initialize(tmp_path / "target", durable=False)
    owner = _kernel.register_owner(target, kind="cli", label="test", allocation=None, advertised={})
    try:
        status, copy = _store.install_copy(target, owner, landing / parent.directory.name, source_label="t")
        assert status == "installed" and copy.record["slots"] == {"forces": child.id}
        status, copy = _store.install_copy(target, owner, landing / child.directory.name, source_label="t")
        assert status == "installed" and copy.record["slots"] == {}

        mismatched = json.loads((landing / parent.directory.name / "install.json").read_text(encoding="utf-8"))
        mismatched["slots"] = {}
        (landing / parent.directory.name / "install.json").write_text(json.dumps(mismatched), encoding="utf-8")
        other = Workspace.initialize(tmp_path / "third", durable=False)
        other_owner = _kernel.register_owner(other, kind="cli", label="test", allocation=None, advertised={})
        try:
            with pytest.raises(ValueError, match=r"the recorded call slots \[\] are not the manifest's"):
                _store.install_copy(other, other_owner, landing / parent.directory.name, source_label="t")
        finally:
            other_owner.close()
    finally:
        owner.close()
    # A store record without the member reads as one without slots.
    stripped = _store.lookup(workspace, child.id)
    assert stripped is not None
    (stripped.directory / "install.json").write_text(json.dumps(old), encoding="utf-8")
    assert set(_store.resolve_calls(workspace, [parent.id], include_slot_defaults=True)) == {parent.id, child.id}
