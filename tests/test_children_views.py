"""``a.children`` derived from the job's own ``state.json`` and ``a.spawned`` over every child it published."""

import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from attempt_fixtures import BASH_API, child_observation, fabricate, find, state, submit_runner
from httk.workflow import Attempt, FormatError, TaskManager, Workspace, _kernel
from httk.workflow._state import Release

_SRC = str(Path(__file__).resolve().parents[1] / "src")

# ``first`` spawns a, b (fails) and x and gathers them; ``second`` spawns x again and y and gathers them;
# ``third`` advances to ``fourth``. Every step records what it reads into ``<step>.json`` of its workdir.
_RUNNER = f'''#!/usr/bin/env python3
import json
import os
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.views")


def view(children):
    return [
        {{
            "label": child.label,
            "kind": child.kind,
            "job_id": child.job_id,
            "failure": None if child.failure is None else child.failure.code,
            "payload": None if child.payload is None else str(child.payload),
            "workdir": None if child.workdir is None else str(child.workdir),
            "raw": dict(child.raw),
        }}
        for child in children
    ]


def record(a):
    spawned = a.spawned
    seen = {{
        "context": json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"]),
        "children": view(a.children),
        "spawned": view(spawned),
        "spawned_x": spawned["x"].job_id if "x" in spawned else None,
    }}
    (a.workdir / (a.step + ".json")).write_text(json.dumps(seen), encoding="utf-8")


def spawn(a, *labels):
    for label in labels:
        mode = "fail" if label == "b" else "succeed"
        a.spawn(ChildSpec(step="leaf", parameters={{"mode": mode}}, maximum_attempts_per_activation=1), label=label)


@run.step
def first(a):
    record(a)
    spawn(a, "a", "b", "x")
    a.gather("second", when="all_terminal")


@run.step
def second(a):
    record(a)
    spawn(a, "x", "y")
    a.gather("third", when="all_terminal")


@run.step
def third(a):
    record(a)
    a.advance("fourth")


@run.step
def fourth(a):
    record(a)
    a.succeed()


@run.step
def leaf(a):
    if a.parameter("mode") == "fail":
        a.fail("leaf.broken", "the leaf failed")
    a.succeed()


raise SystemExit(run.main())
'''


# A parent spawns ``children`` leaves nested under it and gathers them; a leaf publishes its outcome without the
# SDK (no import of httk), so hundreds of them stay cheap. ``collect`` records the labels it reads and its context.
_WIDE_RUNNER = f'''#!/usr/bin/env python3
import json
import os
import sys

if os.environ.get("HTTK_WORKFLOW_STEP") == "leaf":
    context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
    control = os.environ["HTTK_WORKFLOW_CONTROL_DIR"]
    outcome = {{key: context[key] for key in ("job_id", "activation_id", "attempt_id")}}
    outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
    os.mkdir(os.path.join(control, "outcome.tmp.leaf"))
    with open(os.path.join(control, "outcome.tmp.leaf", "outcome.json"), "w") as stream:
        json.dump(outcome, stream)
    os.rename(os.path.join(control, "outcome.tmp.leaf"), os.path.join(control, "outcome.ready"))
    raise SystemExit(0)

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.wide")


@run.step
def fan(a):
    for index in range(a.parameter("children")):
        a.spawn(ChildSpec(step="leaf", maximum_attempts_per_activation=1), label="c-%03d" % index)
    a.gather("collect")


@run.step
def leaf(a):
    a.succeed()


@run.step
def collect(a):
    seen = {{"labels": a.children.labels, "context": json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])}}
    (a.workdir / "seen.json").write_text(json.dumps(seen), encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
'''


def _run(root: Path) -> tuple[Workspace, _kernel.JobRef]:
    """Run one parent through ``first`` → ``fourth`` with a manager; return its workspace and directory."""

    workspace = Workspace.initialize(root / "workspace", durable=False)
    source = root / "source" / "views.py"
    source.parent.mkdir()
    source.write_text(_RUNNER, encoding="utf-8")
    job_id = submit_runner(
        workspace,
        source,
        placement="project/views",
        install_step="first",
        initial_step="first",
        maximum_attempts_per_activation=1,
    ).job_id
    with TaskManager(workspace, maximum_workers=4, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120)
    parent = find(workspace, job_id)
    assert parent.state == "succeeded", state(parent).failure
    return workspace, parent


@pytest.fixture(scope="module")
def finished(tmp_path_factory: pytest.TempPathFactory) -> tuple[Workspace, _kernel.JobRef]:
    """One finished parent the read-only tests share; its children stay in place."""

    return _run(tmp_path_factory.mktemp("views"))


def _seen(parent: _kernel.JobRef, step: str) -> dict[str, Any]:
    return dict(json.loads((parent.path / "run" / f"{step}.json").read_text(encoding="utf-8")))


def test_the_join_step_derives_its_children_from_state(finished: tuple[Workspace, _kernel.JobRef]) -> None:
    workspace, parent = finished
    first, second = _seen(parent, "first"), _seen(parent, "second")
    # The initial activation follows no join; no activation's context carries the children.
    assert first["children"] == [] and first["spawned"] == []
    assert all("children" not in _seen(parent, step)["context"] for step in ("first", "second", "third", "fourth"))

    children = second["children"]
    assert [(child["label"], child["kind"], child["failure"]) for child in children] == [
        ("a", "succeeded", None),
        ("b", "failed", "leaf.broken"),
        ("x", "succeeded", None),
    ]
    for child in children:
        located = find(workspace, child["job_id"])
        assert child["payload"] == str(located.path) and child["workdir"] == str(located.path / "run")
        # raw is the join observation itself.
        assert child["raw"]["state"] == child["kind"] and "payload_path" not in child["raw"]


def test_an_advance_activation_has_no_children(finished: tuple[Workspace, _kernel.JobRef]) -> None:
    _workspace, parent = finished
    fourth = _seen(parent, "fourth")
    assert fourth["context"]["activation_reason"] == "advance"
    assert fourth["children"] == [] and len(fourth["spawned"]) == 5


def test_spawned_spans_activations_in_publication_order(finished: tuple[Workspace, _kernel.JobRef]) -> None:
    _workspace, parent = finished
    first, second, third = (_seen(parent, step) for step in ("first", "second", "third"))
    spawned = third["spawned"]
    assert [child["label"] for child in spawned] == ["a", "b", "x", "x", "y"]
    assert [child["kind"] for child in spawned] == ["succeeded", "failed", "succeeded", "succeeded", "succeeded"]
    assert spawned[1]["failure"] == "leaf.broken"
    # Each entry records the activation and attempt that published it.
    publishers = [(child["raw"]["activation_id"], child["raw"]["attempt_id"]) for child in spawned]
    by_first = (first["context"]["activation_id"], first["context"]["attempt_id"])
    by_second = (second["context"]["activation_id"], second["context"]["attempt_id"])
    assert publishers == [by_first] * 3 + [by_second] * 2
    # The second join observed only the children of the second activation; a reused label names the newest child.
    assert [child["label"] for child in third["children"]] == ["x", "y"]
    assert third["spawned_x"] == spawned[3]["job_id"] != spawned[2]["job_id"]
    assert second["spawned_x"] == spawned[2]["job_id"]


def _bound(fabricated_root: Path, workspace: Workspace, parent: _kernel.JobRef) -> dict[str, str]:
    """The environment of an attempt bound to *parent*'s current activation, its control directory elsewhere."""

    doc = state(parent)
    assert doc.activation is not None
    control = fabricated_root / "control"
    control.mkdir(parents=True)
    context = {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        "workspace_id": workspace.workspace_id,
        "job_id": doc.job_id,
        "job_key": parent.job_key,
        "placement": "project/views",
        "payload": str(parent.path),
        "step": str(doc.activation["step"]),
        "activation_id": str(doc.activation["id"]),
        "attempt_id": str(uuid.uuid4()),
        "settings": {},
        "durable": False,
        "deadline": None,
    }
    environment = {
        **os.environ,
        "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(parent.path),
        "HTTK_WORKFLOW_WORKDIR": str(parent.path / "run"),
        "HTTK_WORKFLOW_DATA_DIR": str(parent.path / "data"),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(workspace.root),
        "HTTK_WORKFLOW_STEP": str(doc.activation["step"]),
        "HTTK_WORKFLOW_PYTHON": sys.executable,
        "HTTK_WORKFLOW_BASH_API": str(BASH_API),
    }
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, (_SRC, environment.get("PYTHONPATH"))))
    return environment


def test_spawned_reports_owned_and_missing_children_and_the_bridge_reads_it(tmp_path: Path) -> None:
    # Its own run: the test claims one child and removes another.
    workspace, parent = _run(tmp_path / "run")
    environment = _bound(tmp_path, workspace, parent)
    labels = [child["label"] for child in _seen(parent, "third")["spawned"]]
    ids = [child["job_id"] for child in _seen(parent, "third")["spawned"]]
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    claimed = _kernel.claim(workspace, owner, find(workspace, ids[0]))
    assert claimed is not None
    removed = find(workspace, ids[4])
    shutil.rmtree(removed.path)

    spawned = Attempt.initialize(environment).spawned
    assert [child.kind for child in spawned] == ["owned", "failed", "succeeded", "succeeded", "missing"]
    assert spawned.all[0].payload == claimed.path and spawned.all[0].raw["from_state"] == "succeeded"
    assert spawned.all[1].failure is not None and spawned.all[1].failure.code == "leaf.broken"
    assert spawned.all[4].payload is None and spawned.all[4].workdir is None and spawned.all[4].data is None
    assert spawned["x"].job_id == ids[3]

    script = tmp_path / "read.sh"
    script.write_text(
        'set -euo pipefail\nsource "$HTTK_WORKFLOW_BASH_API"\n'
        "printf 'joined\\n'\nhttk_workflow_children\n"
        "printf 'spawned\\n'\nhttk_workflow_children --spawned\n"
        "printf 'failed\\n'\nhttk_workflow_children --spawned --failed\n"
        "printf 'fields\\n'\nhttk_workflow_child --spawned x job_id\nhttk_workflow_child --spawned y state\n"
        "httk_workflow_child --spawned y workdir || printf 'absent=%s\\n' \"$?\"\n",
        encoding="utf-8",
    )
    completed = subprocess.run(
        ["bash", str(script)], env=environment, text=True, capture_output=True, check=False, cwd=tmp_path
    )
    assert completed.returncode == 0, completed.stderr
    lines = completed.stdout.splitlines()
    # The fourth activation followed an advance, so the join view is empty.
    assert lines[:2] == ["joined", "spawned"]
    rows = [line.split("\t") for line in lines[2:7]]
    keys = [child.job_key for child in spawned]
    assert [row[:3] for row in rows] == [
        [label, kind, key] for label, kind, key in zip(labels, [child.kind for child in spawned], keys, strict=True)
    ]
    assert rows[0][3] == str(claimed.path / "run") and rows[4][3:] == ["", ""]
    assert lines[7:9] == ["failed", "\t".join(rows[1])]
    assert lines[9:] == ["fields", ids[3], "missing", "absent=1"]
    doc = claimed.read_state()
    assert doc is not None
    claimed.release(doc, Release("succeeded", 500))
    owner.close()


def _fabricated(tmp_path: Path) -> tuple[dict[str, str], Path]:
    attempt = fabricate(tmp_path / "attempt", step="aggregate", children=[child_observation("site-0", "succeeded")])
    return attempt.environment, attempt.payload


def test_the_join_view_requires_this_activation_and_this_job(tmp_path: Path) -> None:
    environment, payload = _fabricated(tmp_path)
    assert Attempt.initialize(environment).children.labels == ("site-0",)

    context = json.loads(environment["HTTK_WORKFLOW_CONTEXT"])
    other_activation = {**context, "activation_id": str(uuid.uuid4())}
    assert len(Attempt.initialize({**environment, "HTTK_WORKFLOW_CONTEXT": json.dumps(other_activation)}).children) == 0
    # A join activation whose state.json records another activation is a broken system, not zero children.
    other_join = {**other_activation, "activation_reason": "join"}
    with pytest.raises(FormatError, match="does not record the join"):
        _ = Attempt.initialize({**environment, "HTTK_WORKFLOW_CONTEXT": json.dumps(other_join)}).children

    other_job = {**context, "job_id": str(uuid.uuid4())}
    with pytest.raises(FormatError, match="names job"):
        _ = Attempt.initialize({**environment, "HTTK_WORKFLOW_CONTEXT": json.dumps(other_job)}).children

    (payload / "state.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(FormatError, match="damaged"):
        _ = Attempt.initialize(environment).children
    with pytest.raises(FormatError, match="damaged"):
        _ = Attempt.initialize(environment).spawned

    (payload / "state.json").unlink()
    with pytest.raises(FormatError, match="taken away from this attempt"):
        _ = Attempt.initialize(environment).children
    with pytest.raises(FormatError, match="taken away from this attempt"):
        _ = Attempt.initialize(environment).spawned


def test_an_observed_child_that_vanished_keeps_its_kind_without_paths(tmp_path: Path) -> None:
    observation = child_observation("site-0", "succeeded")
    attempt = fabricate(tmp_path / "attempt", step="aggregate", children=[observation])
    shutil.rmtree(attempt.workspace.root / str(observation["payload_path"]))
    (child,) = Attempt.initialize(attempt.environment).children
    assert child.label == "site-0" and child.kind == "succeeded" and child.job_id == observation["job_id"]
    assert child.payload is None and child.workdir is None and child.data is None


def test_a_gather_beyond_the_former_context_ceiling_sees_every_child(tmp_path: Path) -> None:
    # 400 nested children: about three times what the attempt context could carry before.
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    source = tmp_path / "source" / "wide.py"
    source.parent.mkdir()
    source.write_text(_WIDE_RUNNER, encoding="utf-8")
    job_id = submit_runner(
        workspace,
        source,
        placement="project/campaign",
        install_step="fan",
        initial_step="fan",
        maximum_attempts_per_activation=1,
        parameters={"children": 400},
    ).job_id
    with TaskManager(workspace, maximum_workers=8, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=600)

    parent = find(workspace, job_id)
    assert parent.state == "succeeded", state(parent).failure
    seen = json.loads((parent.path / "run" / "seen.json").read_text(encoding="utf-8"))
    assert seen["labels"] == [f"c-{index:03d}" for index in range(400)]
    assert "children" not in seen["context"]
    nested = f"project/campaign/{parent.job_key}.children"
    assert {str(entry["placement"]) for entry in state(parent).observations} == {nested}
