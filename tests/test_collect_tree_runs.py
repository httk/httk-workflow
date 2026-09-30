"""Collecting a job tree stores one run per job, linked parent to children.

A parent that calls another workflow leaves a run for itself and one for every
child, each naming its own workflow declaration, even though neither workflow
has anything else to collect; the parent's run names each child's run as an
artifact. ``--no-bare-runs`` opts out of the runs of workflows with nothing to
collect.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from httk.core import Run
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.packages import load_workflow_package
from httk.workflow.scaffold import new_job
from httk.workflow.workflow_cli import command

_SRC = str(Path(__file__).parents[1] / "src")
_DECLARATIONS = "https://example.org/httk/tests/declarations"

_PARENT = f'''#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.tree_runs.parent")


@run.step
def start(a):
    for index in range(2):
        a.call(a.parameter("child"), label="child-%d" % index)
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

run = Runner("tests.tree_runs.child")


@run.step
def work(a):
    a.succeed()


raise SystemExit(run.main())
'''


def _package(root: Path, name: str, steps: list[str], runner: str) -> Path:
    """Write one workflow package with a declaration and no collector."""

    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "{name}"\ndeclaration_uri = "{_DECLARATIONS}/{name}"\n\n'
        f'[workflow.runner]\nentry = "run.py"\nsteps = {json.dumps(steps)}\n',
        encoding="utf-8",
    )
    (root / "run.py").write_text(runner, encoding="utf-8")
    (root / "run.py").chmod(0o755)
    load_workflow_package(root)
    return root


def _collect(tmp_path: Path, workspace: Workspace, store: Path, *extra: str) -> None:
    context = CLIContext("httk", tmp_path)
    name = f"tree-runs-{workspace.workspace_id[:8]}"
    try:
        register_ws(context, workspace.root, name)
    except ValueError:
        pass  # registered by an earlier collect of this test
    arguments = ["collect", "--workspace", name, "--into", str(store), "--id-base", "httk.probe", "--no-id-ledger"]
    assert command([*arguments, *extra], context) == 0


def _runs(path: Path) -> list[Run]:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(path) as database:
        searcher = SqlStore(database).searcher()
        variable = searcher.variable(Run)
        return [row.run for row in searcher.results(run=variable)]


@pytest.fixture
def tree(tmp_path: Path) -> tuple[Workspace, Any]:
    pytest.importorskip("httk.store")
    child = _package(tmp_path / "child", "tests.tree_runs.child", ["work"], _CHILD)
    _package(tmp_path / "parent", "tests.tree_runs.parent", ["start", "done"], _PARENT)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, "tests.tree_runs.parent", parameters={"child": str(child)})
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)
    assert {marker.kind for marker in workspace.scan_markers()} == {"succeeded"}
    return workspace, job


def test_a_called_tree_collects_one_linked_run_per_job(tmp_path: Path, tree: tuple[Workspace, Any]) -> None:
    workspace, job = tree
    store = tmp_path / "runs.sqlite"
    _collect(tmp_path, workspace, store)

    runs = _runs(store)
    by_declaration: dict[str | None, list[Run]] = {}
    for run in runs:
        by_declaration.setdefault(run.workflow_declaration_uri, []).append(run)
    assert sorted(by_declaration, key=str) == [
        f"{_DECLARATIONS}/tests.tree_runs.child",
        f"{_DECLARATIONS}/tests.tree_runs.parent",
    ]
    (parent,) = by_declaration[f"{_DECLARATIONS}/tests.tree_runs.parent"]
    children = by_declaration[f"{_DECLARATIONS}/tests.tree_runs.child"]
    assert len(children) == 2
    assert parent.source_id == f"{workspace.workspace_id}:{job.job_id}"
    # The parent's run names each child's run as an artifact, by spawn label.
    assert sorted((edge.label, edge.entry_type) for edge in parent.artifacts) == [
        ("child-0", "runs"),
        ("child-1", "runs"),
    ]
    assert {edge.entry_id for edge in parent.artifacts} == {child.id for child in children}

    # Collecting again converges on the same runs rather than duplicating them.
    _collect(tmp_path, workspace, store)
    assert len(_runs(store)) == 3


def test_bare_runs_can_be_opted_out_of(tmp_path: Path, tree: tuple[Workspace, Any]) -> None:
    workspace, _job = tree
    store = tmp_path / "none.sqlite"
    _collect(tmp_path, workspace, store, "--no-bare-runs")
    assert _runs(store) == []


def _latest_runs(path: Path) -> list[Run]:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(path) as database:
        searcher = SqlStore(database).searcher(only_latest=True)
        variable = searcher.variable(Run)
        return [row.run for row in searcher.results(run=variable)]


@pytest.mark.parametrize("ledger", [False, True], ids=["no-ledger", "ledger"])
def test_a_parent_collected_before_its_children_is_revised_not_duplicated(
    tmp_path: Path, tree: tuple[Workspace, Any], ledger: bool
) -> None:
    from httk.core.crypto import ed25519_generate_seed

    from httk.workflow import collect, store_collected

    workspace, job = tree
    items = list(collect(workspace))
    parent = next(item for item in items if item.child_runs)
    store = tmp_path / "revised.sqlite"
    options: dict[str, Any] = {"id_base": "httk.probe", "id_series": "1"}
    if ledger:
        options |= {"ledger_path": str(tmp_path / "ids.sqlite"), "ledger_keys": [("test", ed25519_generate_seed())]}

    first = store_collected([parent], str(store), **options)
    assert not any("storage_error" in report for report in first)
    (alone,) = _runs(store)
    assert alone.artifacts == ()

    second = store_collected(items, str(store), **options)
    assert not any("storage_error" in report for report in second)
    source = f"{workspace.workspace_id}:{job.job_id}"
    revisions = [run for run in _runs(store) if run.source_id == source]
    # Two revisions of one lineage: same entry id, the latest names both children.
    assert len(revisions) == 2 and {run.id for run in revisions} == {alone.id}
    (latest,) = [run for run in _latest_runs(store) if run.source_id == source]
    assert sorted(edge.label for edge in latest.artifacts) == ["child-0", "child-1"]
    assert len(_latest_runs(store)) == 3
