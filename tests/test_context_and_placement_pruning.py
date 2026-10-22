"""Nested child placements at scale: the attempt context, claim-time pruning, over-long placements, the monitor."""

import json
import os
import uuid
from pathlib import Path

import pytest

from attempt_fixtures import every_job, find, run_manager, state, submit_runner
from httk.workflow import TaskManager, Workspace, _fs
from httk.workflow._children import _PATH_HEADROOM
from httk.workflow._kernel import claim, format_job_name
from httk.workflow.monitor.ui import render_job_pane
from kernel.test_kernel import FakeWorkspace, make_workspace, new_job, new_owner

_SRC = str(Path(__file__).parents[1] / "src")

# A parent spawns ``children`` leaves, each at ``placement`` or nested under the parent, then gathers them.
_FAN_RUNNER = f'''#!/usr/bin/env python3
import json
import os
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import ChildSpec, Runner

run = Runner("tests.fan")


@run.step
def fan(a):
    nested = "%s/%s.children" % (a.context.placement, a.context.job_key)
    for index in range(a.parameter("children")):
        a.spawn(
            ChildSpec(step="leaf", maximum_attempts_per_activation=1),
            label="c-%03d" % index,
            placement=a.parameter("placement", nested),
        )
    a.gather("collect")


@run.step
def leaf(a):
    a.succeed()


@run.step
def collect(a):
    context = os.environ["HTTK_WORKFLOW_CONTEXT"]
    (a.workdir / "seen.json").write_text(
        json.dumps(
            {{
                "labels": [child.label for child in a.children.all],
                "placements": sorted({{str(child.placement) for child in a.children.all}}),
                "context": json.loads(context),
            }}
        ),
        encoding="utf-8",
    )
    a.succeed()


raise SystemExit(run.main())
'''


def _fan(workspace: Workspace, root: Path, parameters: dict[str, object]) -> str:
    root.mkdir(parents=True, exist_ok=True)
    source = root / "fan.py"
    source.write_text(_FAN_RUNNER, encoding="utf-8")
    return submit_runner(
        workspace,
        source,
        placement="project/campaign",
        install_step="fan",
        initial_step="fan",
        maximum_attempts_per_activation=1,
        parameters=parameters,
    ).job_id


@pytest.mark.slow
def test_a_wide_nested_gather_fits_the_attempt_context(tmp_path: Path) -> None:
    # 120 nested observations exceeded the 100000-byte context while each was carried twice; the join's children
    # are now read from the job's own state.json, so the context carries neither.
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _fan(workspace, tmp_path / "source", {"children": 120})
    with TaskManager(workspace, maximum_workers=8, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300)

    parent = find(workspace, job_id)
    assert parent.state == "succeeded", state(parent).failure
    seen = json.loads((parent.path / "run" / "seen.json").read_text(encoding="utf-8"))
    assert seen["labels"] == [f"c-{index:03d}" for index in range(120)]
    context = seen["context"]
    assert "join" not in context and "children" not in context
    assert seen["placements"] == [f"project/campaign/{parent.job_key}.children"]


def test_a_child_placement_too_long_for_the_filesystem_fails_the_outcome(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    # Every component is valid (at most 255 bytes); the whole path exceeds PATH_MAX.
    placement = "/".join(["x" * 250] * 20)
    job_id = _fan(workspace, tmp_path / "source", {"children": 1, "placement": placement})
    run_manager(workspace)

    parent = find(workspace, job_id)
    failure = state(parent).failure
    assert parent.state == "failed" and failure is not None
    assert failure["code"] == "protocol_error" and "too long for the filesystem" in str(failure["message"])


def _exact_placement(length: int) -> str:
    """Return a placement of exactly *length* bytes in valid (at most 255-byte) components."""

    parts = []
    while length > 200:
        parts.append("x" * 199)
        length -= 200
    return "/".join([*parts, "y" * length])


@pytest.mark.parametrize(("short", "accepted"), [(0, False), (1, True)])
def test_a_child_job_directory_must_leave_path_headroom(tmp_path: Path, short: int, accepted: bool) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    try:
        path_max = os.pathconf(workspace.jobs, "PC_PATH_MAX")
    except (OSError, ValueError):
        path_max = 4096
    # The fan runner's child key is "<label>--<uuid>" with label c-000; succeeded is the longest unowned state.
    name = format_job_name(f"c-000--{uuid.uuid4()}", 999, _fs.fresh_token())
    base = len(os.fsencode(workspace.jobs / "succeeded")) + 1 + 1 + len(name)
    placement = _exact_placement(path_max - _PATH_HEADROOM - short - base)
    assert len(os.fsencode(workspace.jobs / "succeeded" / placement / name)) == path_max - _PATH_HEADROOM - short
    job_id = _fan(workspace, tmp_path / "source", {"children": 1, "placement": placement})
    run_manager(workspace, timeout=60)  # NotIdleError if a commit is retried or a parent waits forever

    parent = find(workspace, job_id)
    failure = state(parent).failure
    if accepted:
        assert parent.state == "succeeded", failure
        assert [ref.state for ref in every_job(workspace) if ref.job_id != job_id] == ["succeeded"]
    else:
        assert parent.state == "failed" and failure is not None
        assert failure["code"] == "protocol_error" and "too long for the filesystem" in str(failure["message"])
        assert not (workspace.jobs / "ready" / ("x" * 199)).exists()


def _ready(ws: FakeWorkspace) -> Path:
    return ws.jobs / "ready"


def test_a_claim_prunes_the_ready_placement_directories_it_emptied(tmp_path: Path) -> None:
    ws = make_workspace(tmp_path / "ws")
    owner, rival = new_owner(ws), new_owner(ws)
    first, second = (new_job(ws, owner, placement="a/b/c") for _ in range(2))
    assert claim(ws, owner, first) is not None
    assert (_ready(ws) / "a" / "b" / "c").is_dir()
    assert claim(ws, owner, second) is not None
    assert not (_ready(ws) / "a").exists() and _ready(ws).is_dir()

    # A lost claim prunes nothing, even a directory it would otherwise empty.
    (_ready(ws) / "a" / "b" / "c").mkdir(parents=True)
    assert claim(ws, rival, second) is None
    assert (_ready(ws) / "a" / "b" / "c").is_dir()

    # Only a placement's own empty chain goes: a sibling keeps the shared ancestor.
    lone = new_job(ws, owner, placement="a/b/d")
    assert claim(ws, owner, lone) is not None
    assert not (_ready(ws) / "a" / "b" / "d").exists() and (_ready(ws) / "a" / "b" / "c").is_dir()


def test_a_claim_never_prunes_the_state_directory_or_other_states(tmp_path: Path) -> None:
    ws = make_workspace(tmp_path / "ws")
    owner = new_owner(ws)
    assert claim(ws, owner, new_job(ws, owner, placement="")) is not None
    assert _ready(ws).is_dir()
    assert claim(ws, owner, new_job(ws, owner, state="failed", placement="a/b")) is not None
    assert (ws.jobs / "failed" / "a" / "b").is_dir()


def test_the_monitor_shows_the_trailing_end_of_a_long_placement() -> None:
    row = {"job_key": "k", "state": "ready", "step": "s", "placement": "project/campaign/leaf.children", "priority": 1}
    (line,) = render_job_pane([row], width=200)[2:]
    assert "…/leaf.children " in line and "project" not in line
