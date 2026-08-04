"""Marker-lookup scale, unknown extension handling, and the
cross-wave handoffs: a diagnosable ``cancelling`` state, fsck's live-kind set,
collection of retired requests, background collection inside a manager, and the
guard against reviving a job a decided join already consumed.
"""

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest  # pyright: ignore[reportMissingImports]

from httk.workflow import TaskManager, Workspace
from httk.workflow import _util as util_module
from httk.workflow.errors import UnsupportedExtensionError
from httk.workflow.introspection import explain_job
from httk.workflow.journal import parse_record_ref, segment_path
from httk.workflow.models import CORE_STATE_KINDS
from httk.workflow.workspace import Marker

_DAY = 86400.0

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


def _payload(root: Path, name: str, *, tag: str = "scale", parent: dict[str, object] | None = None) -> tuple[Path, str]:
    """Write one minimal payload whose runner succeeds immediately."""

    job_id = str(uuid.uuid4())
    payload = root / name
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(_SUCCEED_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 1,
                "id": job_id,
                "tag": tag,
                "name": "Scale test job",
                "workflow": "tests.scale",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "run",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
                "parent": parent,
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def _progress(**extra: object) -> dict[str, object]:
    """Return the progress members every scheduling frame of a job carries."""

    return {
        "step": "run",
        "activation_id": str(uuid.uuid4()),
        "activation_ordinal": 1,
        "attempt_ordinal": 1,
        "total_attempts": 1,
        "data_generation": None,
        **extra,
    }


def _chain(workspace: Workspace, marker: Marker, kinds: list[tuple[str, dict[str, object]]]) -> Marker:
    """Advance one job through *kinds* with a single journal writer."""

    with workspace.open_journal_writer() as writer:
        for kind, updates in kinds:
            marker = workspace.transition(writer, marker, kind, updates)
    return marker


class _ScandirCounter:
    """Count every directory ``os.scandir`` opens.

    The streaming walker, ``pathlib`` globbing, and every exhaustive scan reach
    the filesystem through ``os.scandir``, so counting its calls measures how
    many directories a lookup or a tick reads — the cost a whole-tree rescan
    multiplies and the index, the placement probe, and the bounded window each
    avoid. It is the scandir-era successor of the old ``rglob`` counter: there
    is no ``rglob`` left in the scheduling paths to count.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls = 0
        original = os.scandir

        def counting(*args: Any, **kwargs: Any) -> Any:
            self.calls += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(os, "scandir", counting)

    def reset(self) -> int:
        seen = self.calls
        self.calls = 0
        return seen


def _full_scan_cost(workspace: Workspace, counter: _ScandirCounter) -> int:
    """Return the ``os.scandir`` calls one complete scan of ``state/`` performs.

    A lookup that answers from neither the index nor a placement probe rebuilds
    the index from exactly this scan, so this is the cost a whole-workspace
    rescan pays — once per such lookup, and never for an index hit.
    """

    counter.reset()
    list(Workspace.scan_marker_entries(workspace, CORE_STATE_KINDS))
    return counter.reset()


# ---------------------------------------------------------------------------
# The job-id index
# ---------------------------------------------------------------------------


def test_marker_lookup_answers_from_the_index_instead_of_rescanning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    # Only active kinds: the index is now scoped to work a job can still move on
    # from, so a terminal job is deliberately not cached and is covered by its
    # own test below rather than confusing the index-hit accounting here.
    kinds = ("submitted", "ready", "waiting", "committing", "cancelling")
    job_ids: list[str] = []
    for index in range(300):
        job_id = str(uuid.uuid4())
        directory = workspace.control / "state" / kinds[index % len(kinds)] / "project" / f"{index % 10:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"job--{job_id}.p500.g0.init").touch()
        job_ids.append(job_id)

    counter = _ScandirCounter(monkeypatch)
    per_scan = _full_scan_cost(workspace, counter)
    # A full scan reads the whole state tree — many directories, not one per
    # kind — so the index is exactly what a scaling workspace cannot do without.
    assert per_scan > len(kinds)

    # Without the index every lookup rebuilds it from one whole-tree scan, which
    # is what a join child, an operator request, and 'job show' each used to cost.
    for job_id in job_ids[:3]:
        workspace.invalidate_marker_index()
        assert workspace.find_marker_by_id(job_id) is not None
    assert counter.reset() == 3 * per_scan

    # With it warm, 300 lookups read no directory at all.
    for job_id in job_ids:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind in kinds
    assert counter.reset() == 0

    # Absence is never taken from the cache: it costs one rescan, so a job
    # another manager has just published is found rather than denied.
    assert workspace.find_marker_by_id(str(uuid.uuid4())) is None
    assert counter.reset() == per_scan


def test_a_stale_index_entry_falls_back_to_the_placement_probe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    marker = workspace.submit(payload, "project/probe")
    # Populate other placements so a whole-tree rescan would be visibly dearer
    # than a probe of the one placement the stale entry names.
    for index in range(20):
        other_payload, _ = _payload(tmp_path / "source", f"other-{index}")
        workspace.submit(other_payload, f"project/other/{index:02d}")
    assert workspace.find_marker_by_id(job_id) is not None

    # Another actor moves the marker: the cached entry now names nothing.
    other = Workspace(workspace.root)
    moved = _chain(other, other.find_marker_by_id(job_id) or marker, [("ready", _progress(reason="submitted"))])
    counter = _ScandirCounter(monkeypatch)
    found = workspace.find_marker_by_id(job_id)
    assert found is not None and found.kind == "ready" and found.path == moved.path
    probe = counter.reset()
    # An ordinary transition never changes placement, so the finite state set at
    # the remembered placement resolves it by reading only that placement's kind
    # directories — bounded by the number of state kinds, never the whole tree.
    assert 0 < probe <= len(CORE_STATE_KINDS)
    assert probe < _full_scan_cost(workspace, counter)


def test_a_relocated_job_still_resolves_after_the_index_misses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    marker = workspace.submit(payload, "project/first")
    assert workspace.find_marker_by_id(job_id) is not None

    # Nothing in the core profile does this, but a stale entry whose placement
    # is wrong too must still resolve, and only through a full rescan.
    destination = workspace.control / "state" / "ready" / "project" / "second"
    destination.mkdir(parents=True)
    marker.path.rename(destination / marker.path.name)
    counter = _ScandirCounter(monkeypatch)
    per_scan = _full_scan_cost(workspace, counter)
    found = workspace.find_marker_by_id(job_id)
    assert found is not None and found.placement.as_posix() == "project/second"
    # The index entry is stale and its placement is wrong too, so neither the
    # index nor the placement probe resolves it: only a whole-tree rescan does,
    # which costs at least one full scan on top of the fruitless probe.
    assert counter.reset() >= per_scan


def _waiting_campaign(root: Path, children: int, *, hint: bool = True) -> tuple[Workspace, str]:
    """Build one waiting parent whose join names *children* children.

    Every child reference carries its placement, as the protocol now requires:
    the join probes each child's exact state set instead of ever rescanning the
    tree. Passing ``hint=False`` reproduces the withdrawn pre-release shape whose
    reference omits the placement, which a manager now rejects as a protocol
    error rather than resolving by a whole-workspace scan.
    """

    workspace = Workspace.initialize(root / "workspace")
    references: list[dict[str, object]] = []
    for index in range(children):
        payload, _child_id = _payload(root / "source", f"child-{index}", tag="child")
        child = workspace.submit(payload, f"project/children/{index}")
        reference: dict[str, object] = {
            "workspace_id": workspace.workspace_id,
            "job_id": child.job_id,
            "job_key": child.job_key,
        }
        if hint:
            reference["placement_hint"] = child.placement.as_posix()
        references.append(reference)
    payload, parent_id = _payload(root / "source", "parent", tag="parent")
    parent = workspace.submit(payload, "project/parent")
    _chain(
        workspace,
        parent,
        [
            (
                "waiting",
                _progress(
                    reason="waiting_for_children",
                    next_step="aggregate",
                    join={"children": references, "condition": "all_succeeded"},
                ),
            )
        ],
    )
    return workspace, parent_id


def _steady_tick_cost(workspace: Workspace, counter: _ScandirCounter) -> int:
    """Return the directories one steady-state tick of a waiting parent reads."""

    with TaskManager(workspace, pools=("nothing-runs-here",), heartbeat_interval=600.0) as manager:
        manager.tick()
        manager.tick()
        counter.reset()
        manager.tick()
        return counter.reset()


def test_a_waiting_parents_tick_never_opens_the_terminal_trees(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, parent_id = _waiting_campaign(tmp_path / "campaign", 4)
    counter = _ScandirCounter(monkeypatch)
    with TaskManager(workspace, pools=("nothing-runs-here",), heartbeat_interval=600.0) as manager:
        # Reach the steady state: the children are registered to ready but never
        # claimed here, and the parent stays waiting on a still-pending join.
        for _ in range(4):
            manager.tick()
        marker = workspace.find_marker_by_id(parent_id)
        assert marker is not None and marker.kind == "waiting"

        counter.reset()
        manager.tick()
        base = counter.reset()
        # The tick reads the active trees and resolves each child by its exact
        # placement, so it touches the filesystem but never the whole tree.
        assert base > 0

        # Pile finished markers below the terminal kinds. A scheduling tick owns
        # none of succeeded, failed, or cancelled, so it never opens them and its
        # cost does not move by a single directory however many pile up.
        for index in range(100):
            for kind in ("succeeded", "failed", "cancelled"):
                directory = workspace.control / "state" / kind / "project" / "done" / f"{index:03d}"
                directory.mkdir(parents=True, exist_ok=True)
                (directory / f"job--{uuid.uuid4()}.p500.g3.done").touch()
        counter.reset()
        manager.tick()
        assert counter.reset() == base


def test_a_join_child_reference_without_placement_is_a_protocol_error(tmp_path: Path) -> None:
    # A pre-release reference that omits the placement is no longer resolved by a
    # whole-workspace scan; the manager fails the waiting parent as a protocol
    # error rather than rescanning the tree once per unhinted child.
    workspace, parent_id = _waiting_campaign(tmp_path / "campaign", 2, hint=False)
    with TaskManager(workspace, pools=("nothing-runs-here",), heartbeat_interval=600.0) as manager:
        manager.tick()
    parent = workspace.find_marker_by_id(parent_id)
    assert parent is not None and parent.kind == "failed"
    failure = workspace.read_state(parent).get("failure")
    assert isinstance(failure, dict) and failure.get("code") == "protocol_error"


def test_a_placement_hint_is_resolved_before_the_index_or_a_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    marker = workspace.submit(payload, "project/hinted")
    counter = _ScandirCounter(monkeypatch)
    found = workspace.find_marker_at(marker.job_key, marker.placement)
    assert found is not None and found.job_id == job_id
    # The hint resolves by a bounded sweep of the one named placement — at most
    # one directory per state kind, never a recursive walk of the whole tree.
    assert 0 < counter.reset() <= len(CORE_STATE_KINDS)


# ---------------------------------------------------------------------------
# Unknown extensions are refused
# ---------------------------------------------------------------------------


def test_unknown_extensions_cannot_be_enabled_or_attached(tmp_path: Path) -> None:
    with pytest.raises(UnsupportedExtensionError) as enabling:
        Workspace.initialize(tmp_path / "unknown", extensions=["unknown-feature"])
    assert "unknown-feature" in str(enabling.value)

    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    stored["extensions"] = ["unknown-feature"]
    (workspace.control / "format.json").write_text(json.dumps(stored), encoding="utf-8")
    for mutable in (True, False):
        with pytest.raises(UnsupportedExtensionError) as attaching:
            Workspace(workspace.root, mutable=mutable)
        assert "unknown-feature" in str(attaching.value)


def test_registration_accepts_a_submitted_marker_an_operator_already_repriced(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    submitted = workspace.submit(payload, "project/repriced")
    assert submitted.path.name.endswith(".g0.init")

    workspace.publish_request(
        {
            "format": "httk-workflow-request",
            "format_version": 1,
            "request_id": str(uuid.uuid4()),
            "job_id": job_id,
            "job_key": submitted.job_key,
            "placement": submitted.placement.as_posix(),
            "expected_generation": submitted.generation,
            "expected_record_ref": submitted.record_ref,
            "action": "set_priority",
            "priority": 25,
            "operator": "pytest",
            "reason": "urgent",
        }
    )
    # A repriced submitted marker is no longer g0.init, and registration must
    # still accept it and keep the operator's priority rather than job.json's.
    with TaskManager(workspace, pools=("other",)) as manager:
        manager._handle_requests()
        repriced = workspace.find_marker_by_id(job_id)
        assert repriced is not None
        assert repriced.kind == "submitted" and repriced.priority == 25 and repriced.generation == 1
        assert repriced.record_ref != "init"
        manager.tick()
    ready = workspace.find_marker_by_id(job_id)
    assert ready is not None and ready.kind == "ready" and ready.priority == 25


def test_a_ready_marker_carries_its_exact_priority_without_a_band_directory(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    workspace.submit(payload, "project/plain")
    with TaskManager(workspace, pools=("other",)) as manager:
        manager.tick()
    ready = workspace.find_marker_by_id(job_id)
    assert ready is not None and ready.kind == "ready" and ready.priority == 500
    assert ready.path.parent == workspace.control / "state" / "ready" / "project" / "plain"


# ---------------------------------------------------------------------------
# Cross-wave handoffs
# ---------------------------------------------------------------------------


def _cancelling_job(workspace: Workspace, tmp_path: Path, manager_id: str) -> tuple[Marker, str]:
    """Leave one job fenced in ``cancelling`` by a named manager."""

    payload, job_id = _payload(tmp_path / "source", f"payload-{uuid.uuid4()}")
    marker = _chain(
        workspace,
        workspace.submit(payload, "project/cancelled"),
        [
            ("ready", _progress(reason="submitted")),
            ("claimed", _progress(reason="claimed", manager_id=manager_id, lease_seconds=900.0)),
            ("running", _progress(reason="launched", manager_id=manager_id, attempt_id=str(uuid.uuid4()))),
            (
                "cancelling",
                _progress(
                    reason="operator_cancel",
                    manager_id=manager_id,
                    attempt_id=str(uuid.uuid4()),
                    operator="pytest",
                    operator_reason="stop it",
                ),
            ),
        ],
    )
    return marker, job_id


def _heartbeat(workspace: Workspace, manager_id: str, *, updated_at: str) -> Path:
    path = workspace.control / "managers" / manager_id
    path.mkdir(parents=True, exist_ok=True)
    (path / "manager.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-manager",
                "format_version": 1,
                "manager_id": manager_id,
                "hostname": "elsewhere",
                "pid": 1,
                "pools": ["default"],
                "capabilities": [],
                "executors": ["path"],
                "started_at": updated_at,
            }
        ),
        encoding="utf-8",
    )
    (path / "heartbeat.json").write_text(
        json.dumps({"manager_id": manager_id, "updated_at": updated_at}),
        encoding="utf-8",
    )
    return path


def test_job_why_explains_a_cancelling_job_instead_of_calling_it_unknown(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    manager_id = str(uuid.uuid4())
    marker, _ = _cancelling_job(workspace, tmp_path, manager_id)
    _heartbeat(workspace, manager_id, updated_at=util_module.utc_now())

    diagnosis = explain_job(workspace, marker)
    assert diagnosis.state == "cancelling"
    assert "not a core state" not in diagnosis.summary
    assert "fenced" in diagnosis.summary
    names = {check.name for check in diagnosis.checks}
    assert {"fencing", "termination evidence", "owning manager"} <= names
    owner = next(check for check in diagnosis.checks if check.name == "owning manager")
    assert owner.satisfied is True and manager_id in owner.detail
    assert not diagnosis.blocked

    # A cancellation whose owner has died is the one an operator must act on.
    _heartbeat(workspace, manager_id, updated_at="2020-01-01T00:00:00.000000Z")
    stalled = explain_job(workspace, marker)
    assert stalled.blocked
    assert any("host" in hint for hint in stalled.hints)


def test_fsck_never_repoints_a_cancelling_marker_owned_by_a_live_manager(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    manager_id = str(uuid.uuid4())
    marker, _ = _cancelling_job(workspace, tmp_path, manager_id)
    _heartbeat(workspace, manager_id, updated_at=util_module.utc_now())
    writer_id, segment, offset, _, _ = parse_record_ref(marker.record_ref)
    path = segment_path(workspace.control, writer_id, segment)
    with path.open("r+b") as handle:
        handle.seek(offset + 8)
        original = handle.read(1)
        handle.seek(offset + 8)
        handle.write(bytes([original[0] ^ 0xFF]))

    (finding,) = workspace.check(repair=True, quarantine_unrepairable=True).findings
    assert finding.action == "skipped_live" and manager_id in finding.detail
    assert marker.path.is_file()

    # Once nobody owns the fence any more, the same damage is repairable.
    _heartbeat(workspace, manager_id, updated_at="2020-01-01T00:00:00.000000Z")
    (repaired,) = workspace.check(repair=True).findings
    assert repaired.action == "repaired"
    assert repaired.kind == "cancelling"


def test_collection_removes_month_old_retired_requests(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    retired = workspace.control / "requests" / "retired"
    retired.mkdir(parents=True)
    old = retired / "old.json"
    old.write_text("{}", encoding="utf-8")
    (retired / "old.json.retirement").write_text("{}", encoding="utf-8")
    fresh = retired / "fresh.json"
    fresh.write_text("{}", encoding="utf-8")

    # Retired requests are always safe to collect, so no retention is configured.
    report = workspace.collect_garbage(now=time.time() + 31 * _DAY)
    collected = {Path(entry).name for entry in report.category("retired_requests").entries}
    assert collected == {"old.json", "old.json.retirement", "fresh.json"}
    assert not old.exists() and not fresh.exists()

    fresh.write_text("{}", encoding="utf-8")
    assert workspace.collect_garbage().category("retired_requests").removed == 0
    assert fresh.exists()


def test_a_manager_collects_in_the_background_only_when_asked(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")

    def abandoned(name: str) -> Path:
        entry = workspace.control / "tmp" / name
        entry.write_text("interrupted publication", encoding="utf-8")
        when = time.time() - 2 * _DAY
        os.utime(entry, (when, when))
        return entry

    quiet = abandoned("quiet")
    with TaskManager(workspace, heartbeat_interval=600.0) as manager:
        assert manager.gc_interval is None
        manager.tick()
    assert quiet.exists()

    collecting = abandoned("collecting")
    with TaskManager(workspace, heartbeat_interval=600.0, gc_interval=3600.0) as manager:
        manager.tick()
        assert not collecting.exists()
        # The interval is a rate limit, not a per-tick chore.
        again = abandoned("again")
        manager.tick()
        assert again.exists()
        manager._last_gc = 0.0
        manager.tick()
        assert not again.exists()

    with pytest.raises(ValueError):
        TaskManager(workspace, gc_interval=0.0)


# ---------------------------------------------------------------------------
# The decided-join revival guard
# ---------------------------------------------------------------------------


def _consumed_child(tmp_path: Path) -> tuple[Workspace, Marker, Marker]:
    """Return a workspace whose parent already decided a join on a failed child."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    parent_payload, parent_id = _payload(tmp_path / "source", "parent", tag="parent")
    parent = workspace.submit(parent_payload, "project/parent")
    child_payload, child_id = _payload(
        tmp_path / "source",
        "child",
        tag="child",
        parent={
            "workspace_id": workspace.workspace_id,
            "job_id": parent_id,
            "job_key": parent.job_key,
            "placement": parent.placement.as_posix(),
        },
    )
    child = workspace.submit(child_payload, "project/children")
    child = _chain(
        workspace,
        child,
        [
            ("ready", _progress(reason="submitted")),
            ("failed", _progress(reason="failed", failure={"code": "vasp.broken", "message": "no"})),
        ],
    )
    observation = {
        "workspace_id": workspace.workspace_id,
        "job_id": child_id,
        "job_key": child.job_key,
        "label": "child",
        "placement": child.placement.as_posix(),
        "kind": "failed",
        "state_generation": child.generation,
        "record_ref": child.record_ref,
    }
    parent = _chain(
        workspace,
        parent,
        [
            (
                "ready",
                _progress(reason="join_impossible", step="handle_child_failure", join_summary=[observation]),
            )
        ],
    )
    return workspace, parent, child


def _continue_request(workspace: Workspace, marker: Marker, *, force: bool = False) -> None:
    request: dict[str, object] = {
        "format": "httk-workflow-request",
        "format_version": 1,
        "request_id": str(uuid.uuid4()),
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "placement": marker.placement.as_posix(),
        "expected_generation": marker.generation,
        "expected_record_ref": marker.record_ref,
        "action": "continue",
        "operator": "pytest",
        "reason": "rescue the child",
    }
    if force:
        request["force"] = True
    workspace.publish_request(request)


def test_reviving_a_child_a_decided_join_consumed_is_refused_without_force(tmp_path: Path) -> None:
    workspace, parent, child = _consumed_child(tmp_path)
    _continue_request(workspace, child)
    with TaskManager(workspace, pools=("other",)) as manager:
        manager.tick()

    still_failed = workspace.find_marker_by_id(child.job_id)
    assert still_failed is not None and still_failed.kind == "failed"
    retirements = list((workspace.control / "requests" / "retired").glob("*.retirement"))
    assert len(retirements) == 1
    reason = str(json.loads(retirements[0].read_text(encoding="utf-8"))["reason"])
    assert parent.job_key in reason and "force" in reason


def test_a_forced_revival_journals_the_hazard_it_accepted(tmp_path: Path) -> None:
    workspace, parent, child = _consumed_child(tmp_path)
    _continue_request(workspace, child, force=True)
    with TaskManager(workspace, pools=("other",)) as manager:
        manager.tick()

    revived = workspace.find_marker_by_id(child.job_id)
    assert revived is not None and revived.kind == "ready"
    hazard = workspace.read_state(revived)["revival_hazard"]
    assert hazard["parent_job_key"] == parent.job_key
    assert hazard["parent_job_id"] == parent.job_id
    assert hazard["observed_kind"] == "failed"


def test_an_ordinary_child_is_continued_without_a_hazard(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _ = _payload(tmp_path / "source", "solitary")
    marker = _chain(
        workspace,
        workspace.submit(payload, "project/solitary"),
        [
            ("ready", _progress(reason="submitted")),
            ("failed", _progress(reason="failed", failure={"code": "x.broken", "message": "no"})),
        ],
    )
    _continue_request(workspace, marker)
    with TaskManager(workspace, pools=("other",)) as manager:
        manager.tick()
    revived = workspace.find_marker_by_id(marker.job_id)
    assert revived is not None and revived.kind == "ready"
    assert "revival_hazard" not in workspace.read_state(revived)
