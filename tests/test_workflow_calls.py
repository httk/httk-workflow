"""Declared sub-workflow calls: ``[workflow.calls]``, pinned at creation, ready before a job starts.

A workflow names the workflows it calls in its manifest. A job records them when
it is created (refused if one is unknown), may call only those, and is not
claimed by a manager until each is known and, when compiled, built on the
manager's machine.
"""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.plugins.install import install_plugin

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, scaffold
from httk.workflow._calls import reset_call_readiness
from httk.workflow.packages import _reset_plugin_workflow_cache, parse_workflow_manifest
from httk.workflow.precheck import precheck_jobs
from httk.workflow.scaffold import new_job, workflow_provider
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


def _plugin(root: Path, *, calls: str = 'child = "tests.calls.child"') -> Path:
    """Write a plugin with a compiled child and a parent that declares calling it."""

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
    (root / "httk_plugin.toml").write_text(
        '[plugin]\nname = "tests-calls"\nworkflows = ["child", "parent"]\n', encoding="utf-8"
    )
    return root


@pytest.fixture(autouse=True)
def _fresh() -> Iterator[None]:
    _reset_plugin_workflow_cache()
    reset_call_readiness()
    yield
    for name in ("tests.calls.parent", "tests.calls.child"):
        scaffold._WORKFLOW_PROVIDERS.pop(name, None)
    _reset_plugin_workflow_cache()
    reset_call_readiness()


def _run(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)


def test_a_call_must_name_a_workflow_not_a_path(tmp_path: Path) -> None:
    root = _plugin(tmp_path / "plugin", calls='child = "../child"')
    with pytest.raises(ValueError, match="must name a workflow or a git URI, not a path"):
        parse_workflow_manifest(root / "parent")


def test_a_job_whose_declared_call_is_unknown_is_refused_at_creation(tmp_path: Path) -> None:
    install_plugin(_plugin(tmp_path / "plugin", calls='child = "tests.calls.nowhere"'))
    _reset_plugin_workflow_cache()
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match="declared call child = 'tests.calls.nowhere' does not resolve"):
        new_job(workspace, "tests.calls.parent")


def test_a_job_waits_unclaimed_until_its_called_workflow_is_built(tmp_path: Path) -> None:
    install_plugin(_plugin(tmp_path / "plugin"))
    _reset_plugin_workflow_cache()
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, "tests.calls.parent")
    recorded = json.loads((job.payload / "job.json").read_text(encoding="utf-8"))
    assert recorded["calls"] == {"child": "tests.calls.child"}

    # The child is compiled and not built here: the parent is never claimed.
    _run(workspace)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind in {"submitted", "ready"}
    (finding,) = [item for item in precheck_jobs(workspace) if item["job_id"] == job.job_id]
    assert "not built" in str(finding["calls"]) and "tests.calls.child" in str(finding["calls"])

    # Building the parent by name builds what it declares it calls.
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "calls")
    assert command(["build", "--workspace", name, "tests.calls.parent"], context) == 0
    reset_call_readiness()

    _run(workspace)
    assert {marker.kind for marker in workspace.scan_markers()} == {"succeeded"}
    children = [marker for marker in workspace.scan_markers() if marker.job_id != job.job_id]
    assert [workspace.load_job(marker).workflow for marker in children] == ["tests.calls.child"]


def test_a_job_may_call_only_what_its_workflow_declares(tmp_path: Path) -> None:
    install_plugin(_plugin(tmp_path / "plugin"))
    _reset_plugin_workflow_cache()
    workspace = Workspace.initialize(tmp_path / "workspace")
    child = workflow_provider("tests.calls.child")
    assert child is not None and child.directory is not None
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "calls-undeclared")
    assert command(["build", "--workspace", name, str(child.directory)], context) == 0

    job = new_job(workspace, "tests.calls.parent", parameters={"target": "tests.calls.elsewhere"})
    _run(workspace)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "failed"
    errors = list(workspace.payload_path(marker.placement, marker.job_key).glob("attempts/*/error.json"))
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


def test_a_call_name_never_resolves_as_a_directory_where_the_job_is_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_plugin(_plugin(tmp_path / "plugin", calls='child = "childpkg"'))
    _reset_plugin_workflow_cache()
    workspace = Workspace.initialize(tmp_path / "workspace")
    # A package directory of that name in the working directory is not a workflow name.
    _write_package(
        tmp_path / "cwd" / "childpkg",
        '[workflow]\nname = "childpkg"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["work"]\n',
        _CHILD,
    )
    monkeypatch.chdir(tmp_path / "cwd")
    with pytest.raises(ValueError, match="no registered, plugin, or installed workflow has that name"):
        new_job(workspace, "tests.calls.parent")


def test_calls_are_resolved_and_checked_transitively(tmp_path: Path) -> None:
    root = _plugin(tmp_path / "plugin", calls='middle = "tests.calls.middle"')
    _write_package(
        root / "middle",
        '[workflow]\nname = "tests.calls.middle"\n\n[workflow.runner]\nentry = "run.py"\nsteps = ["work"]\n\n'
        '[workflow.calls]\nchild = "tests.calls.child"\n',
        _CHILD.replace("tests.calls.child", "tests.calls.middle"),
    )
    manifest = root / "httk_plugin.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('"parent"]', '"parent", "middle"]'), encoding="utf-8"
    )
    install_plugin(root)
    _reset_plugin_workflow_cache()
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, "tests.calls.parent")
    # The parent calls only the middle workflow, whose own call is a compiled, unbuilt child.
    (finding,) = [item for item in precheck_jobs(workspace) if item["job_id"] == job.job_id]
    assert "its call child (tests.calls.child): not built" in str(finding["calls"])


def test_declaring_calls_leaves_the_workflow_alias_alone(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_plugin(tmp_path / "plugin") / "parent")
    assert provider.alias is None and provider.calls == {"child": "tests.calls.child"}
