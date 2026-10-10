"""Scale and handoffs: bounded job lookups, many managers over many jobs, and the guarded revival of a child.

A job is found by listing its placement in each unowned state, never by a
whole-tree scan, so a lookup and a scheduling tick stay bounded however many
finished jobs pile up elsewhere. Many manager processes share one workspace,
each attempt running exactly once. A child a decided join consumed is revived
only on a forced request, which records the hazard it accepted.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _kernel, _requests, _store
from httk.workflow.errors import UnsupportedExtensionError
from test_concurrency import _CLAIM_RECORDING_RUNNER
from test_crash_injection import _all_jobs, _log

if TYPE_CHECKING:
    from conftest import TestProfile as _TestProfile

SOURCE = Path(__file__).resolve().parents[1] / "src"


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "demo")


class _ListingCounter:
    """Record every directory ``os.listdir`` and ``os.scandir`` open while armed."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.listed: list[Path] = []
        real_listdir, real_scandir = os.listdir, os.scandir

        def listdir(path: Any = ".") -> list[str]:
            self.listed.append(Path(os.fsdecode(path)) if not isinstance(path, int) else Path(f"fd:{path}"))
            return real_listdir(path)

        def scandir(path: Any = ".") -> Any:
            self.listed.append(Path(os.fsdecode(path)) if not isinstance(path, int) else Path(f"fd:{path}"))
            return real_scandir(path)

        monkeypatch.setattr(os, "listdir", listdir)
        monkeypatch.setattr(os, "scandir", scandir)

    def reset(self) -> list[Path]:
        seen, self.listed = self.listed, []
        return seen


def _pile_finished_jobs(ws: Workspace, count: int) -> None:
    """Create *count* finished job directories in each terminal state, in placements nobody looks up."""

    for index in range(count):
        for state in ("succeeded", "failed", "cancelled"):
            directory = ws.jobs / state / "done" / f"{index:03d}" / f"job--{uuid.uuid4()}~p500~{'a' * 16}"
            directory.mkdir(parents=True)


# ---------------------------------------------------------------------------
# Bounded lookups
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_a_placement_hint_is_resolved_by_listing_only_that_placement(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"}, placement="project/hinted")
    for index in range(20):
        h.submit(ws, installed, {"start": "succeed"}, placement=f"project/other/{index:02d}")
    _pile_finished_jobs(ws, 20)

    counter = _ListingCounter(monkeypatch)
    found = _kernel.locate(ws, submitted.job_id, placement_hint=PurePosixPath("project/hinted"), include_owned=False)
    listed = counter.reset()
    assert found is not None and found.path == submitted.path
    # At most one listing per unowned state, each of the hinted placement: never a walk of the tree.
    hinted = {ws.jobs / state / "project" / "hinted" for state in _kernel.UNOWNED_STATES}
    assert listed and set(listed) <= hinted and len(listed) <= len(hinted)

    # Absence at a hint is concluded from the same bounded listings.
    assert _kernel.locate(ws, str(uuid.uuid4()), placement_hint=PurePosixPath("project/hinted")) is None
    assert set(counter.reset()) <= hinted | {ws.jobs / "owned"}


@pytest.mark.slow
def test_a_waiting_parents_tick_never_opens_the_terminal_trees(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    spawn = {
        "children": [
            {"label": f"c{index}", "script": {"start": "succeed"}, "placement": f"children/{index}"}
            for index in range(4)
        ]
    }
    parent = h.submit(ws, installed, {"start": "spawn", "gather": "succeed"}, parameters={"spawn": spawn})
    # This manager serves only the parent's subtree: the children stay ready and the parent keeps waiting.
    with TaskManager(ws, heartbeat_interval=600.0, placement_prefixes=("project",)) as manager:
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            manager.tick()
            if h.find(ws, parent.job_id).state == "waiting" and not manager.running_attempts:
                break
            time.sleep(0.02)
        assert h.find(ws, parent.job_id).state == "waiting"
        manager.tick()

        counter = _ListingCounter(monkeypatch)
        manager.tick()
        base = counter.reset()
        # The tick reads its own trees and resolves each child at its exact placement.
        assert base

        # Pile finished jobs below the terminal states. A scheduling tick owns none of them, so it never opens
        # them, and its cost does not move by a single directory however many pile up.
        _pile_finished_jobs(ws, 100)
        counter.reset()
        manager.tick()
        assert sorted(map(str, counter.reset())) == sorted(map(str, base))
        assert not [path for path in base if "done" in path.parts]


_UNHINTED_JOIN_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, uuid

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
child_id = str(uuid.uuid4())
draft = control / "outcome.tmp.x"
draft.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="wait", next_step="gather", join={
    "children": [{"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": "child--" + child_id}],
    "condition": "all_succeeded",
})
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.mark.slow
def test_a_join_child_reference_without_placement_is_a_protocol_error(ws: Workspace, tmp_path: Path) -> None:
    # A reference without its placement could only be resolved by a whole-workspace scan, so it is refused.
    unhinted = h.install(ws, tmp_path / "unhinted", executables={"run": _UNHINTED_JOIN_RUNNER})
    submitted = h.submit(ws, unhinted, {"start": "wait"})
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    assert failed.state == "failed"
    failure = h.state_of(failed).failure
    assert failure is not None and failure["code"] == "protocol_error"
    assert "placement_hint" in str(failure["message"])


def test_unknown_extensions_cannot_be_enabled_or_attached(tmp_path: Path) -> None:
    with pytest.raises(UnsupportedExtensionError) as enabling:
        Workspace.initialize(tmp_path / "unknown", extensions=["unknown-feature"])
    assert "unknown-feature" in str(enabling.value)

    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    stored["extensions"] = ["unknown-feature"]
    (workspace.control / "format.json").write_text(json.dumps(stored), encoding="utf-8")
    with pytest.raises(UnsupportedExtensionError) as attaching:
        Workspace(workspace.root)
    assert "unknown-feature" in str(attaching.value)


def test_a_ready_job_carries_its_exact_priority_in_its_name(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "succeed"}, placement="project/plain", priority=137)
    with TaskManager(ws, pools=("other",)) as manager:
        manager.tick()
    ready = h.find(ws, submitted.job_id)
    assert ready.state == "ready" and ready.priority == 137
    # No band directory: the job sits directly in its placement, its priority in its own name.
    assert ready.path.parent == ws.jobs / "ready" / "project" / "plain"
    assert _kernel.parse_job_name(ready.path.name).priority == 137


def test_a_manager_validates_its_collection_interval(ws: Workspace) -> None:
    # Background collection returns with the collection rewrite; the interval is still validated up front.
    with TaskManager(ws, heartbeat_interval=600.0, gc_interval=3600.0) as manager:
        assert manager.gc_interval == 3600.0
        manager.tick()
    with pytest.raises(ValueError):
        TaskManager(ws, gc_interval=0.0)


# ---------------------------------------------------------------------------
# Many managers over many jobs
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.timing
def test_many_manager_processes_run_many_jobs_exactly_once(
    ws: Workspace, tmp_path: Path, test_profile: "_TestProfile"
) -> None:
    managers = test_profile.scale(normal=4, extended=12)
    jobs = test_profile.scale(normal=40, extended=400)
    log = tmp_path / "claimlog"
    recording = h.install(ws, tmp_path / "recording", name="recording", executables={"run": _CLAIM_RECORDING_RUNNER})
    job_ids = [
        h.submit(
            ws, recording, {"start": "succeed"}, placement=f"project/{index % 7}/{index}", parameters={"log": str(log)}
        ).job_id
        for index in range(jobs)
    ]
    code = (
        "from httk.workflow import TaskManager, Workspace\n"
        f"TaskManager(Workspace({str(ws.root)!r}, durable=False), maximum_workers=3).run_until_idle(timeout=300)\n"
    )
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    processes = [subprocess.Popen([sys.executable, "-c", code], env=environment) for _ in range(managers)]
    for process in processes:
        assert process.wait(timeout=600) == 0
    # Whatever an early-finishing process left claimable is served by one more manager.
    h.run(ws)

    assert sorted(path.name for path in (log / "claims").iterdir()) == sorted(job_ids)
    assert list((log / "duplicates").iterdir()) == []
    finished = _all_jobs(ws)
    assert len(finished) == jobs and {ref.state for ref in finished} == {"succeeded"}
    assert all([line["event"] for line in _log(ref)].count("launched") == 1 for ref in finished)
    # Every owner closed cleanly: nothing is left owned and no owner record remains.
    assert not list((ws.jobs / "owned").glob("*/*"))
    assert not [item for item in _kernel.list_owners(ws) if item.record is not None]


# ---------------------------------------------------------------------------
# The decided-join revival guard
# ---------------------------------------------------------------------------


def _consumed_children(ws: Workspace, installed: _store.Installed) -> tuple[_kernel.JobRef, list[_kernel.JobRef]]:
    """Run a parent whose join on two failing children was decided (rescued by on_impossible)."""

    spawn = {
        "children": [
            {"label": "a", "script": {"start": "fail:vasp.broken"}},
            {"label": "b", "script": {"start": "fail"}},
        ],
        "on_impossible": "rescue",
    }
    # Room for a revived attempt: the children inherit the parent's retry policy.
    parent = h.submit(
        ws,
        installed,
        {"start": "spawn", "rescue": "succeed"},
        parameters={"spawn": spawn},
        retry_policy={"maximum_attempts_per_activation": 3},
    )
    h.run(ws)
    assert h.find(ws, parent.job_id).state == "succeeded"
    children = [ref for ref in _all_jobs(ws) if ref.job_id != parent.job_id]
    assert len(children) == 2 and {ref.state for ref in children} == {"failed"}
    return h.find(ws, parent.job_id), children


def _continue(ws: Workspace, ref: _kernel.JobRef, *, force: bool = False) -> None:
    assert ref.placement is not None
    _requests.post(
        ws,
        action="continue",
        job_id=ref.job_id,
        placement=ref.placement,
        operator="pytest",
        reason="rescue the child",
        force=force,
    )


def _last(ref: _kernel.JobRef, event: str) -> dict[str, Any]:
    entries = [dict(entry) for entry in h.state_of(ref).history_tail if entry["event"] == event]
    assert entries, event
    return entries[-1]


def _serve(ws: Workspace, check: Callable[[TaskManager], None] | None = None) -> None:
    # A pool nothing runs in: the request is applied, the revived child is not run.
    with TaskManager(ws, pools=("other",), heartbeat_interval=600.0) as manager:
        manager.run_until_idle(timeout=60)
        if check is not None:
            check(manager)


@pytest.mark.slow
def test_reviving_a_child_a_decided_join_consumed_is_refused_without_force(
    ws: Workspace, installed: _store.Installed
) -> None:
    parent, (child, _other) = _consumed_children(ws, installed)
    _continue(ws, child)
    _serve(ws)
    still_failed = h.find(ws, child.job_id)
    assert still_failed.state == "failed"
    dropped = _last(still_failed, "request_dropped")
    assert parent.job_key in str(dropped["note"]) and "force" in str(dropped["note"])
    assert not list((ws.control / "requests").iterdir())


@pytest.mark.slow
def test_a_forced_revival_records_the_hazard_it_accepted(ws: Workspace, installed: _store.Installed) -> None:
    parent, (child, _other) = _consumed_children(ws, installed)
    _continue(ws, child, force=True)
    _serve(ws)
    revived = h.find(ws, child.job_id)
    assert revived.state == "ready"
    applied = _last(revived, "request_applied")
    assert applied["to"] == "ready" and parent.job_key in str(applied["revival_hazard"])
    assert h.state_of(revived).failure is None


@pytest.mark.slow
def test_an_ordinary_job_is_continued_without_a_hazard(ws: Workspace, installed: _store.Installed) -> None:
    submitted = h.submit(ws, installed, {"start": "fail"}, retry_policy={"maximum_attempts_per_activation": 3})
    h.run(ws)
    failed = h.find(ws, submitted.job_id)
    assert failed.state == "failed"
    _continue(ws, failed)
    _serve(ws)
    revived = h.find(ws, submitted.job_id)
    assert revived.state == "ready"
    assert "revival_hazard" not in _last(revived, "request_applied")
