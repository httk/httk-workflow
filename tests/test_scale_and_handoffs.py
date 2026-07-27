"""Marker-lookup scale, the withdrawn priority-bands extension, and the
cross-wave handoffs: a diagnosable ``cancelling`` state, fsck's live-kind set,
collection of retired requests, background collection inside a manager, and the
guard against reviving a job a decided join already consumed.
"""

import json
import os
import time
import uuid
from pathlib import Path

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


class _RglobCounter:
    """Count every complete subtree walk the workspace performs."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls = 0
        original = Path.rglob

        def counting(inner_self: Path, pattern: str, *args: object, **kwargs: object):
            self.calls += 1
            return original(inner_self, pattern, *args, **kwargs)  # pyright: ignore[reportCallIssue]

        monkeypatch.setattr(Path, "rglob", counting)

    def reset(self) -> int:
        seen = self.calls
        self.calls = 0
        return seen


def _scan_cost(workspace: Workspace) -> int:
    """Return the tree walks one complete scan of this workspace performs.

    A scan walks the state kinds that exist, so this is what a lookup that
    cannot answer from the index costs — once per lookup, before the index.
    """

    return sum(1 for kind in CORE_STATE_KINDS if (workspace.control / "state" / kind).is_dir())


# ---------------------------------------------------------------------------
# The job-id index
# ---------------------------------------------------------------------------


def test_marker_lookup_answers_from_the_index_instead_of_rescanning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    kinds = ("submitted", "ready", "waiting", "succeeded", "failed")
    job_ids: list[str] = []
    for index in range(300):
        job_id = str(uuid.uuid4())
        directory = workspace.control / "state" / kinds[index % len(kinds)] / "project" / f"{index % 10:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"job--{job_id}.p500.g0.init").touch()
        job_ids.append(job_id)
    scan = _scan_cost(workspace)
    assert scan == len(kinds)

    counter = _RglobCounter(monkeypatch)
    # Without the index every lookup walks every populated state kind, which is
    # what a join child, an operator request, and 'job show' each used to cost.
    for job_id in job_ids[:3]:
        workspace.invalidate_marker_index()
        assert workspace.find_marker_by_id(job_id) is not None
    assert counter.reset() == 3 * scan

    # With it warm, 300 lookups walk nothing at all.
    for job_id in job_ids:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind in kinds
    assert counter.reset() == 0

    # Absence is never taken from the cache: it costs one rescan, so a job
    # another manager has just published is found rather than denied.
    assert workspace.find_marker_by_id(str(uuid.uuid4())) is None
    assert counter.reset() == scan


def test_a_stale_index_entry_falls_back_to_the_placement_probe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    marker = workspace.submit(payload, "project/probe")
    assert workspace.find_marker_by_id(job_id) is not None

    # Another actor moves the marker: the cached entry now names nothing.
    other = Workspace(workspace.root)
    moved = _chain(other, other.find_marker_by_id(job_id) or marker, [("ready", _progress(reason="submitted"))])
    counter = _RglobCounter(monkeypatch)
    found = workspace.find_marker_by_id(job_id)
    assert found is not None and found.kind == "ready" and found.path == moved.path
    # An ordinary transition never changes placement, so the finite state set at
    # the remembered placement resolves it without a full rescan.
    assert counter.reset() == 0


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
    scan = _scan_cost(workspace)
    counter = _RglobCounter(monkeypatch)
    found = workspace.find_marker_by_id(job_id)
    assert found is not None and found.placement.as_posix() == "project/second"
    assert counter.reset() == scan


def _waiting_campaign(root: Path, children: int) -> tuple[Workspace, str]:
    """Build one waiting parent whose join names *children* unhinted children."""

    workspace = Workspace.initialize(root / "workspace")
    references: list[dict[str, object]] = []
    for index in range(children):
        payload, child_id = _payload(root / "source", f"child-{index}", tag="child")
        child = workspace.submit(payload, f"project/children/{index}")
        # Deliberately no placement_hint: the hint is optional, and this is the
        # shape that made a waiting parent rescan the tree once per child.
        references.append({"workspace_id": workspace.workspace_id, "job_id": child.job_id, "job_key": child.job_key})
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


def _steady_tick_cost(workspace: Workspace, counter: _RglobCounter) -> int:
    """Return the tree walks one steady-state tick of a waiting parent costs."""

    with TaskManager(workspace, pools=("nothing-runs-here",), heartbeat_interval=600.0) as manager:
        manager.tick()
        manager.tick()
        counter.reset()
        manager.tick()
        return counter.reset()


def test_a_waiting_parent_costs_the_same_per_tick_however_many_children_it_has(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    three, parent_id = _waiting_campaign(tmp_path / "three", 3)
    six, _ = _waiting_campaign(tmp_path / "six", 6)
    counter = _RglobCounter(monkeypatch)
    with_three = _steady_tick_cost(three, counter)
    with_six = _steady_tick_cost(six, counter)
    # The scheduling passes each walk their own state kind once; resolving the
    # join children adds nothing, so doubling the children changes nothing.
    assert with_three == with_six
    assert with_three <= _scan_cost(three)

    # The parent is still waiting, and the same tick without the index costs one
    # complete scan per child on top of the passes.
    marker = three.find_marker_by_id(parent_id)
    assert marker is not None and marker.kind == "waiting"
    resolve = three.find_marker_by_id

    def rescanning(job_id: str) -> Marker | None:
        three.invalidate_marker_index()
        return resolve(job_id)

    monkeypatch.setattr(three, "find_marker_by_id", rescanning)
    unindexed = _steady_tick_cost(three, counter)
    assert unindexed - with_three >= 3 * _scan_cost(three)


def test_a_placement_hint_is_resolved_before_the_index_or_a_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", "payload")
    marker = workspace.submit(payload, "project/hinted")
    counter = _RglobCounter(monkeypatch)
    found = workspace.find_marker_at(marker.job_key, marker.placement)
    assert found is not None and found.job_id == job_id
    assert counter.reset() == 0


# ---------------------------------------------------------------------------
# priority-bands-v1 is withdrawn
# ---------------------------------------------------------------------------


def test_priority_bands_cannot_be_enabled_and_such_a_workspace_is_refused(tmp_path: Path) -> None:
    with pytest.raises(UnsupportedExtensionError) as enabling:
        Workspace.initialize(tmp_path / "banded", extensions=["priority-bands-v1"])
    assert "priority-bands-v1" in str(enabling.value)

    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    stored["extensions"] = ["priority-bands-v1"]
    (workspace.control / "format.json").write_text(json.dumps(stored), encoding="utf-8")
    for mutable in (True, False):
        with pytest.raises(UnsupportedExtensionError) as attaching:
            Workspace(workspace.root, mutable=mutable)
        assert "re-initialized" in str(attaching.value)


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
                "runner_backends": ["path"],
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
        parent={"workspace_id": workspace.workspace_id, "job_id": parent_id, "job_key": parent.job_key},
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
