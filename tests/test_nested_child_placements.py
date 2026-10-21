"""Spawned children default to ``<placement>/<job key>.children`` unless the workspace setting says flat."""

import dataclasses
import json
from pathlib import Path

import pytest

from attempt_fixtures import FabricatedAttempt, every_job, fabricate, find, sub_package, submit_runner
from httk.workflow import Attempt, ChildSpec, Runner, TaskManager, Workspace
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload
from test_sdk import _attempt

_SRC = str(Path(__file__).resolve().parents[1] / "src")


def _parent(tmp_path: Path, **kwargs: object):
    run = Runner("tests.sdk")
    run.step(name="start")(lambda a: a.succeed())
    return _attempt(tmp_path, step="start", runner=run, **kwargs)  # type: ignore[arg-type]


def _hint(attempt, **kwargs: object) -> str:
    return str(attempt.spawn(ChildSpec(step="start"), label="c", **kwargs).placement_hint)


def test_default_is_nested_under_the_parent_placement(tmp_path: Path) -> None:
    attempt = _parent(tmp_path)
    assert _hint(attempt) == f"project/fabricated/{attempt.context.job_key}.children"


def test_prepared_payload_is_nested_too(tmp_path: Path) -> None:
    attempt = _parent(tmp_path)
    child = attempt.workdir / "child"
    (child / "files").mkdir(parents=True)
    prepare_job_payload(
        child,
        JobSpec(name="Child", workflow_id="local:tests.sdk", workflow_name="tests.sdk", tag="c", initial_step="start"),
    )
    reference = attempt.spawn(child, label="prepared")
    assert reference.placement_hint == f"project/fabricated/{attempt.context.job_key}.children"


def test_call_validates_the_setting_before_staging(tmp_path: Path) -> None:
    attempt = _parent(tmp_path, settings={"children.placement": "bogus"})
    with pytest.raises(ValueError, match="children.placement"):
        attempt.call("anything", label="c")
    assert not list(attempt.control.glob("call.*"))


def test_flat_explicit_and_invalid(tmp_path: Path) -> None:
    flat = _parent(tmp_path / "flat", settings={"children.placement": "flat"})
    assert _hint(flat) == "project/fabricated"
    nested = _parent(tmp_path / "nested", settings={"children.placement": "nested"})
    assert _hint(nested, placement="elsewhere") == "elsewhere"

    bad = _parent(tmp_path / "bad", settings={"children.placement": "sideways"})
    with pytest.raises(ValueError, match="'nested' or 'flat', not 'sideways'"):
        _hint(bad)
    assert not list(bad.control.glob("outcome.tmp.*/children/jobs/*"))


def _placement_for(job_key: str, nested_bytes: int) -> str:
    """Return a parent placement whose nested placement is *nested_bytes* bytes long."""

    length = nested_bytes - len(f"/{job_key}.children")
    parts = ["d" * 100] * (length // 101)
    parts.append("d" * (length - sum(map(len, parts)) - len(parts)))
    placement = "/".join(parts)
    assert len(placement) == length
    return placement


@pytest.mark.parametrize(("nested_bytes", "nests"), [(1024, True), (1025, False)])
def test_budget_boundary(tmp_path: Path, nested_bytes: int, nests: bool) -> None:
    attempt = _parent(tmp_path)
    placement = _placement_for(attempt.context.job_key, nested_bytes)
    attempt.context = dataclasses.replace(attempt.context, placement=placement)
    expected = f"{placement}/{attempt.context.job_key}.children" if nests else placement
    assert len(expected.encode()) == (1024 if nests else len(placement))
    assert _hint(attempt) == expected


def _caller(tmp_path: Path, **settings: object) -> tuple[FabricatedAttempt, Attempt]:
    fabricated = fabricate(tmp_path, step="start", calls={"sub": sub_package(tmp_path / "sub")}, settings=settings)
    return fabricated, Attempt.initialize(fabricated.environment)


def test_call_placements(tmp_path: Path) -> None:
    _, attempt = _caller(tmp_path / "nested")
    assert attempt.call("sub", label="a").placement_hint == f"project/fabricated/{attempt.context.job_key}.children"
    assert attempt.call("sub", label="b", placement="elsewhere").placement_hint == "elsewhere"
    _, flat = _caller(tmp_path / "flat", **{"children.placement": "flat"})
    assert flat.call("sub", label="a").placement_hint == "project/fabricated"


def test_parameter_and_environment_cannot_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTK_CHILDREN_PLACEMENT", "flat")
    attempt = _parent(tmp_path, parameters={"children.placement": "flat"})
    assert _hint(attempt).endswith(".children")


_TREE = f'''#!/usr/bin/env python3
import json, sys
sys.path.insert(0, {_SRC!r})
from httk.workflow import ChildSpec, Runner

run = Runner("tests.tree")


@run.step
def root(a):
    a.spawn(ChildSpec(step="mid"), label="child")
    a.gather("done")


@run.step
def mid(a):
    a.spawn(ChildSpec(step="leaf"), label="grandchild")
    a.gather("done")


@run.step
def leaf(a):
    (a.workdir / "parent.json").write_text(json.dumps(a.parent.job_key), encoding="utf-8")
    a.succeed()


@run.step
def done(a):
    a.succeed()


raise SystemExit(run.main())
'''


def _tree(tmp_path: Path, **settings: object) -> tuple[Workspace, str]:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for key, value in settings.items():
        workspace.set_setting(key.replace("__", "."), value)
    source = tmp_path / "tree.py"
    source.write_text(_TREE, encoding="utf-8")
    root = submit_runner(workspace, source, placement="project/campaign", install_step="root", name="Tree")
    with TaskManager(workspace, placement_prefixes=["project/campaign"]) as manager:
        manager.run_until_idle(timeout=120)
    return workspace, root.job_id


def test_three_level_tree_nests(tmp_path: Path) -> None:
    workspace, root_id = _tree(tmp_path)
    jobs = every_job(workspace)
    assert len(jobs) == 3 and all(job.state == "succeeded" for job in jobs)
    root = find(workspace, root_id)
    base = workspace.jobs / "succeeded" / "project" / "campaign"
    assert root.path.parent == base
    (child,) = [job for job in jobs if job.path.parent == base / f"{root.job_key}.children"]
    (grandchild,) = [
        job for job in jobs if job.path.parent == base / f"{root.job_key}.children" / f"{child.job_key}.children"
    ]
    assert json.loads((grandchild.path / "run" / "parent.json").read_text(encoding="utf-8")) == child.job_key


def test_three_level_tree_flat(tmp_path: Path) -> None:
    workspace, _ = _tree(tmp_path, children__placement="flat")
    jobs = every_job(workspace)
    assert len(jobs) == 3 and all(job.state == "succeeded" for job in jobs)
    assert {job.path.parent for job in jobs} == {workspace.jobs / "succeeded" / "project" / "campaign"}
