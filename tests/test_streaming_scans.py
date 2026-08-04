"""The streaming, bounded, fair scheduler discovery of Phase 13.

The manager no longer materializes or globally sorts a state tree to schedule.
It walks the active kinds with :class:`~httk.workflow.workspace.MarkerStream`,
an ``os.scandir`` walk that resumes where it stopped, rotates its placement
roots so nothing starves, and stops at a discovery budget while heartbeating
from inside the walk. These tests exercise the walker directly and through a
tick: the budgets and rotation, the in-walk heartbeat, that a tick never opens a
terminal tree, that priority is best-within-window, that a placement-prefixed
manager scans only its subtrees, and that the marker index tracks active work.
"""

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.models import ACTIVE_STATE_KINDS, TERMINAL_KINDS
from httk.workflow.workspace import MarkerStream

_RUNNER = "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n"


def _touch_marker(workspace: Workspace, kind: str, placement: str, *, priority: int = 500) -> str:
    """Create one synthetic marker of *kind* at *placement*, returning its job key.

    A touched marker is enough for the discovery tests: the walker parses its
    name and placement without ever loading a payload, so no ``job.json`` is
    needed to count what a scan visits.
    """

    job_id = str(uuid.uuid4())
    directory = workspace.control.joinpath("state", kind, *placement.split("/"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"job--{job_id}.p{priority:03d}.g0.init").touch()
    return f"job--{job_id}"


def _payload(root: Path, *, tag: str, priority: int) -> tuple[Path, str]:
    """Write one minimal, loadable payload of a given priority."""

    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": job_id,
        "tag": tag,
        "name": f"Streaming job {tag}",
        "workflow": "tests.streaming",
        "runner": {"path": "files/runner", "arguments": [], "executor": "path"},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "only",
        "priority": priority,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _progress(**extra: object) -> dict[str, object]:
    """Return the progress members a scheduling frame carries."""

    return {
        "step": "only",
        "activation_id": str(uuid.uuid4()),
        "activation_ordinal": 1,
        "attempt_ordinal": 1,
        "total_attempts": 1,
        "data_generation": None,
        **extra,
    }


def _submit_ready(workspace: Workspace, source: Path, placement: str, *, tag: str, priority: int) -> str:
    """Submit one payload and move it to ``ready``, returning its job id."""

    payload, job_id = _payload(source, tag=tag, priority=priority)
    marker = workspace.submit(payload, placement)
    with workspace.open_journal_writer() as writer:
        workspace.transition(writer, marker, "ready", _progress(reason="submitted"))
    return job_id


class _ScandirSpy:
    """Record every directory ``os.scandir`` opens while it is installed."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.paths: list[str] = []
        real = os.scandir

        def spy(*args: Any, **kwargs: Any) -> Any:
            if args:
                self.paths.append(str(args[0]))
            return real(*args, **kwargs)

        monkeypatch.setattr(os, "scandir", spy)


# ---------------------------------------------------------------------------
# The streaming walker: budgets, resume, rotation, heartbeat
# ---------------------------------------------------------------------------


def test_the_walker_advances_by_budget_resumes_and_rotates_roots(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    keys: dict[str, list[str]] = {}
    for root in ("a", "b", "c"):
        for index in range(3):
            keys.setdefault(root, []).append(_touch_marker(workspace, "ready", f"{root}/{index:02d}"))
    everything = {key for group in keys.values() for key in group}

    stream = MarkerStream(workspace, "ready")
    seen: set[str] = set()
    first_root: list[str] = []
    for _ in range(6):
        batch = stream.advance(processing_budget=2, discovery_budget=10_000)
        # No advance ever collects more than its processing budget.
        assert len(batch) <= 2
        if batch:
            first_root.append(batch[0].placement.parts[0])
        seen.update(marker.job_key for marker in batch)

    # The bound defers work, it never drops it: every marker is yielded across
    # the resumed advances.
    assert seen == everything
    # Fairness: the first three advances each open a different root, so a subtree
    # holding many markers cannot starve its siblings.
    assert set(first_root[:3]) == {"a", "b", "c"}


def test_the_discovery_budget_bounds_one_advance_and_the_next_resumes(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for _ in range(30):
        _touch_marker(workspace, "ready", "flat")

    stream = MarkerStream(workspace, "ready")
    first = stream.advance(processing_budget=10_000, discovery_budget=8)
    # Discovery itself is bounded: the walk stops at the directory-entry budget
    # even though the processing budget is far from full.
    assert 0 < len(first) <= 8

    collected = {marker.job_key for marker in first}
    for _ in range(10):
        collected.update(marker.job_key for marker in stream.advance(processing_budget=10_000, discovery_budget=8))
    # Resuming across advances eventually reaches every marker in the directory.
    assert len(collected) == 30


def test_the_heartbeat_fires_during_discovery_of_a_large_flat_directory(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    for _ in range(50):
        _touch_marker(workspace, "ready", "flat")

    beats: list[int] = []
    stream = MarkerStream(workspace, "ready")
    stream.advance(
        processing_budget=10_000,
        discovery_budget=10_000,
        heartbeat=lambda: beats.append(1),
        heartbeat_every=10,
    )
    # The lease is refreshed from inside the walk, not only between passes: one
    # advance over fifty entries beats once every ten of them.
    assert len(beats) == 5


# ---------------------------------------------------------------------------
# Terminal kinds are out of scheduling
# ---------------------------------------------------------------------------


def test_a_tick_never_opens_the_terminal_kind_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    _touch_marker(workspace, "submitted", "project/active")
    for kind in TERMINAL_KINDS:
        for index in range(5):
            _touch_marker(workspace, kind, f"project/done/{index:02d}")

    spy = _ScandirSpy(monkeypatch)
    with TaskManager(workspace, pools=("nothing-runs-here",), heartbeat_interval=600.0) as manager:
        spy.paths.clear()
        manager.tick()

    separator = os.sep
    for kind in TERMINAL_KINDS:
        needle = f"{separator}state{separator}{kind}{separator}"
        assert not any(needle in path for path in spy.paths), f"a tick opened the {kind} tree"
    # The spy is wired: the tick did open the one active tree it owns work in.
    assert any(f"{separator}state{separator}submitted{separator}" in path for path in spy.paths)


# ---------------------------------------------------------------------------
# Windowed priority and placement-prefix assignment
# ---------------------------------------------------------------------------


def test_windowed_priority_is_best_in_window_and_rotation_reaches_a_starved_root(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = tmp_path / "source"
    for index in range(3):
        _submit_ready(workspace, source, f"a/{index:02d}", tag=f"a{index}", priority=500)
    # The globally most urgent job — the lowest priority number — sits in a
    # separate subtree the first window will not reach.
    _submit_ready(workspace, source, "b/00", tag="b0", priority=100)
    for index in range(1, 3):
        _submit_ready(workspace, source, f"b/{index:02d}", tag=f"b{index}", priority=500)

    with TaskManager(workspace, pools=("default",), maximum_pass_markers=3, maximum_workers=1) as manager:
        first = manager._eligible_ready()
        # The window covered one root; the priority-100 job in root ``b`` is not
        # in it, so the best candidate here is best-within-window, not global.
        assert first
        assert {marker.placement.parts[0] for marker in first} == {"a"}
        assert first[0].priority == 500

        # Cursor rotation reaches the starved root on the next window, and there
        # the best candidate is the global best.
        second = manager._eligible_ready()
        assert {marker.placement.parts[0] for marker in second} == {"b"}
        assert second[0].priority == 100


def test_a_placement_prefixed_manager_schedules_only_within_its_prefix(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source = tmp_path / "source"
    a_ids = {_submit_ready(workspace, source, f"a/{index:02d}", tag=f"a{index}", priority=500) for index in range(2)}
    b_ids = {_submit_ready(workspace, source, f"b/{index:02d}", tag=f"b{index}", priority=500) for index in range(2)}

    with (
        TaskManager(workspace, pools=("default",), placement_prefixes=("a",)) as manager_a,
        TaskManager(workspace, pools=("default",), placement_prefixes=("b",)) as manager_b,
    ):
        eligible_a = {marker.job_id for marker in manager_a._eligible_ready()}
        eligible_b = {marker.job_id for marker in manager_b._eligible_ready()}

    # Each manager scans only its assigned subtree, so the two take disjoint
    # halves of the ready work and neither reaches into the other's placement.
    assert eligible_a == a_ids
    assert eligible_b == b_ids
    assert eligible_a.isdisjoint(eligible_b)


# ---------------------------------------------------------------------------
# The marker index tracks active work only
# ---------------------------------------------------------------------------


def test_the_marker_index_evicts_a_job_that_reaches_a_terminal_kind(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit_ready(workspace, tmp_path / "source", "project/one", tag="one", priority=500)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    # A warm index holds the active job.
    assert workspace._marker_index is not None and job_id in workspace._marker_index

    with workspace.open_journal_writer() as writer:
        workspace.transition(
            writer,
            marker,
            "failed",
            _progress(reason="failed", failure={"code": "x.broken", "message": "no"}),
        )
    # The transition to a terminal kind evicts it: the index never carries the
    # history of a finished job.
    assert workspace._marker_index is not None
    assert job_id not in workspace._marker_index
    assert set(workspace._marker_index) <= _active_ids(workspace)


def test_the_marker_index_respects_its_capacity(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    for index in range(10):
        _submit_ready(workspace, tmp_path / "source", f"project/{index:02d}", tag=f"j{index}", priority=500)

    capped = Workspace(root, marker_index_capacity=3)
    # A lookup that misses rebuilds the index from a full scan, but the rebuild
    # is trimmed to the cap: a huge active working set never grows it without
    # bound.
    assert capped.find_marker_by_id(str(uuid.uuid4())) is None
    assert capped._marker_index is not None
    assert len(capped._marker_index) <= 3


def _active_ids(workspace: Workspace) -> set[str]:
    """Return the job ids currently at an active kind, by an exhaustive scan."""

    return {marker.job_id for marker in workspace.scan_markers(ACTIVE_STATE_KINDS)}
