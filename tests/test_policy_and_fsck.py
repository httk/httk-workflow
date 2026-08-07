"""Workspace policy, the visibility deadline it configures, durability, and fsck."""

import json
import os
import time
import types
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow import _util as util_module
from httk.workflow.errors import FormatError, WorkspaceUnavailableError
from httk.workflow.journal import (
    iter_journal_frames,
    parse_record_ref,
    read_record,
    segment_path,
)
from httk.workflow.models import DEFAULT_JOURNAL_SEGMENT_BYTES, WorkspacePolicy
from httk.workflow.workflow_cli import command
from httk.workflow.workspace import Marker

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


def _payload(root: Path) -> tuple[Path, str]:
    """Write one minimal payload whose runner succeeds immediately."""

    job_id = str(uuid.uuid4())
    payload = root / f"payload-{job_id[:8]}"
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
                "tag": "policy-test",
                "name": "Policy test job",
                "workflow": "tests.policy",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "run",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {
                    "maximum_attempts_per_activation": 3,
                    "maximum_total_attempts": 10,
                    "maximum_activations": 5,
                    "retry_on": [],
                },
                "resources": {},
                "parent": None,
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def _progress(step: str = "run") -> dict[str, object]:
    """Return the progress members every scheduling frame of a job carries."""

    return {
        "step": step,
        "activation_id": str(uuid.uuid4()),
        "activation_ordinal": 1,
        "attempt_ordinal": 0,
        "total_attempts": 0,
        "data_generation": None,
    }


def _chain(workspace: Workspace, marker: Marker, kinds: list[tuple[str, dict[str, object]]]) -> Marker:
    """Advance one job through *kinds* with a single journal writer."""

    progress = _progress()
    with workspace.open_journal_writer() as writer:
        for kind, updates in kinds:
            marker = workspace.transition(writer, marker, kind, {**progress, **updates})
    return marker


def _corrupt_frame(workspace: Workspace, marker: Marker) -> None:
    """Flip one payload byte of the frame *marker* references."""

    writer_id, segment, offset, _, _ = parse_record_ref(marker.record_ref)
    path = segment_path(workspace.control, writer_id, segment)
    with path.open("r+b") as handle:
        handle.seek(offset + 8)
        original = handle.read(1)
        handle.seek(offset + 8)
        handle.write(bytes([original[0] ^ 0xFF]))


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


def test_policy_is_written_at_initialization_and_round_trips(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    assert stored["policy"] == {
        "visibility_deadline_seconds": 5.0,
        "lease_seconds": 900.0,
        "journal_segment_bytes": DEFAULT_JOURNAL_SEGMENT_BYTES,
        "retention": {},
    }
    assert workspace.policy == WorkspacePolicy()
    updated = workspace.set_policy({"visibility_deadline_seconds": 90, "retention": {"journal_days": 30}})
    assert updated.visibility_deadline_seconds == 90.0
    # Another implementation attaching the same workspace sees the same policy,
    # and the unrelated members of format.json survived the read-modify-write.
    attached = Workspace(tmp_path / "workspace", mutable=False)
    assert attached.policy == updated
    assert attached.visibility_deadline == 90.0
    assert attached.policy.retention.journal_days == 30.0
    assert attached.policy.lease_seconds == 900.0
    assert attached.workspace_id == workspace.workspace_id
    assert attached.format["core_profile"] == "core-v2"


def test_a_workspace_written_before_the_policy_section_reads_as_the_defaults(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    del stored["policy"]
    (workspace.control / "format.json").write_text(json.dumps(stored), encoding="utf-8")
    assert Workspace(tmp_path / "workspace").policy == WorkspacePolicy()


@pytest.mark.parametrize(
    "changes",
    [
        {"visibility_dealine_seconds": 60.0},
        {"unknown": 1},
        {"visibility_deadline_seconds": "soon"},
        {"visibility_deadline_seconds": -1.0},
        {"visibility_deadline_seconds": 999999.0},
        {"lease_seconds": 0.0},
        {"lease_seconds": True},
        {"journal_segment_bytes": 10},
        {"journal_segment_bytes": 4096.5},
        {"retention": 30},
        {"retention": {"journal_hours": 4}},
        {"retention": {"journal_days": "many"}},
    ],
)
def test_policy_refuses_unknown_keys_and_impossible_values(tmp_path: Path, changes: dict[str, object]) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(FormatError):
        workspace.set_policy(changes)
    # A refused change never reaches the workspace.
    assert Workspace(tmp_path / "workspace", mutable=False).policy == WorkspacePolicy()


def test_policy_command_shows_sets_and_refuses(tmp_path: Path, capsys) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, root)
    assert command(["workspace", "policy", "show", ws, "--json"], context) == 0
    assert json.loads(capsys.readouterr().out)["visibility_deadline_seconds"] == 5.0
    assert command(["workspace", "policy", "set", ws, "visibility_deadline_seconds", "60"], context) == 0
    assert command(["workspace", "policy", "set", ws, "retention.trash_days", "14"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "policy", "set", ws, "lease_seconds", "0"], context) == 2
    assert command(["workspace", "policy", "set", ws, "no_such_key", "1"], context) == 2
    assert command(["workspace", "policy", "set", ws, "lease_seconds", "not-json"], context) == 2
    policy = Workspace(root, mutable=False).policy
    assert policy.visibility_deadline_seconds == 60.0
    assert policy.retention.trash_days == 14.0
    assert policy.lease_seconds == 900.0
    assert command(["workspace", "policy", "show", ws], context) == 0
    assert "visibility_deadline_seconds\t60.0" in capsys.readouterr().out


def test_the_manager_takes_its_lease_from_policy_unless_it_is_overridden(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", policy={"lease_seconds": 42.0})
    with TaskManager(workspace) as manager:
        assert manager.lease_seconds == 42.0
    with TaskManager(workspace, lease_seconds=7.0) as manager:
        assert manager.lease_seconds == 7.0


def test_the_journal_rotates_at_the_configured_segment_size(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", policy={"journal_segment_bytes": 4096})
    with workspace.open_journal_writer() as writer:
        assert writer.maximum_segment_bytes == 4096
        references = [writer.append({"filler": "x" * 900, "index": index}) for index in range(10)]
    segments = {parse_record_ref(reference)[1] for reference in references}
    assert len(segments) > 1
    assert all(
        read_record(workspace.control, reference)["index"] == index for index, reference in enumerate(references)
    )


# ---------------------------------------------------------------------------
# The configured visibility deadline
# ---------------------------------------------------------------------------


def test_the_visibility_schedule_spends_exactly_the_configured_deadline() -> None:
    schedule = util_module.visibility_schedule(60.0)
    assert schedule[:3] == (0.01, 0.02, 0.04)
    assert sum(schedule) == pytest.approx(60.0)
    # Long deadlines keep probing rather than sleeping through one huge wait.
    assert max(schedule) <= util_module.MAXIMUM_RETRY_DELAY_SECONDS
    assert sum(util_module.visibility_schedule()) == pytest.approx(util_module.DEFAULT_VISIBILITY_DEADLINE_SECONDS)
    assert sum(util_module.visibility_schedule(0.0)) == 0.0


def test_a_state_read_retries_until_the_configured_deadline(tmp_path: Path, monkeypatch) -> None:
    """The old fixed budget of seven probes was ~0.63s; policy now decides."""

    workspace = Workspace.initialize(tmp_path / "workspace", policy={"visibility_deadline_seconds": 45.0})
    payload, _ = _payload(tmp_path)
    marker = _chain(workspace, workspace.submit(payload, "jobs"), [("ready", {"reason": "submitted"})])
    writer_id, segment, _, _, _ = parse_record_ref(marker.record_ref)
    hidden = segment_path(workspace.control, writer_id, segment)
    hidden.rename(hidden.with_suffix(".hidden"))

    slept: list[float] = []
    monkeypatch.setattr(util_module, "time", types.SimpleNamespace(sleep=slept.append))
    with pytest.raises(WorkspaceUnavailableError):
        workspace.read_state(marker)
    assert sum(slept) == pytest.approx(45.0)
    assert len(slept) > 7


def test_a_short_visibility_deadline_is_honored_in_real_time(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", policy={"visibility_deadline_seconds": 0.75})
    payload, _ = _payload(tmp_path)
    marker = _chain(workspace, workspace.submit(payload, "jobs"), [("ready", {"reason": "submitted"})])
    writer_id, segment, _, _, _ = parse_record_ref(marker.record_ref)
    segment_path(workspace.control, writer_id, segment).unlink()
    started = time.monotonic()
    with pytest.raises(WorkspaceUnavailableError):
        workspace.read_state(marker)
    assert 0.6 <= time.monotonic() - started < 5.0


def test_a_visible_frame_costs_no_backoff_at_all(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", policy={"visibility_deadline_seconds": 3600.0})
    payload, _ = _payload(tmp_path)
    marker = _chain(workspace, workspace.submit(payload, "jobs"), [("ready", {"reason": "submitted"})])
    slept: list[float] = []
    monkeypatch.setattr(util_module, "time", types.SimpleNamespace(sleep=slept.append))
    assert workspace.read_state(marker)["kind"] == "ready"
    assert slept == []


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_workspaces_are_durable_by_default_and_opt_out_explicitly(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    assert workspace.durable is True
    assert Workspace(tmp_path / "workspace").durable is True
    assert Workspace(tmp_path / "workspace", durable=False).durable is False


def test_the_default_write_path_synchronizes_and_no_durable_does_not(tmp_path: Path, monkeypatch) -> None:
    counted: list[int] = []
    real_fsync = os.fsync

    def counting_fsync(descriptor: int) -> None:
        counted.append(descriptor)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", counting_fsync)
    durable = Workspace.initialize(tmp_path / "durable")
    payload, _ = _payload(tmp_path)
    _chain(durable, durable.submit(payload, "jobs"), [("ready", {"reason": "submitted"})])
    assert counted

    counted.clear()
    relaxed = Workspace.initialize(tmp_path / "relaxed", durable=False)
    payload, _ = _payload(tmp_path)
    _chain(relaxed, relaxed.submit(payload, "jobs"), [("ready", {"reason": "submitted"})])
    assert not counted


# ---------------------------------------------------------------------------
# fsck
# ---------------------------------------------------------------------------


def test_fsck_reports_nothing_about_a_healthy_workspace(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _ = _payload(tmp_path)
    _chain(
        workspace,
        workspace.submit(payload, "jobs"),
        [("ready", {"reason": "submitted"}), ("paused", {"reason": "operator"})],
    )
    # The submitted marker of a second job references no frame at all and is
    # just as healthy.
    other, _ = _payload(tmp_path)
    workspace.submit(other, "jobs")
    report = workspace.check()
    assert report.ok and report.markers_checked == 2 and report.unresolved == 0
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "fsck", register_ws(context, tmp_path / "workspace"), "--json"], context) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []


def test_fsck_detects_a_corrupted_frame_and_a_deleted_segment(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    corrupt_payload, corrupt_id = _payload(tmp_path)
    corrupt = _chain(
        workspace,
        workspace.submit(corrupt_payload, "jobs"),
        [("ready", {"reason": "submitted"}), ("paused", {"reason": "operator"})],
    )
    _corrupt_frame(workspace, corrupt)

    lost_payload, lost_id = _payload(tmp_path)
    lost = _chain(workspace, workspace.submit(lost_payload, "jobs"), [("ready", {"reason": "submitted"})])
    # A second writer, whose whole segment then disappears: exactly what a node
    # that lost its unsynchronized journal leaves behind.
    lost = _chain(workspace, lost, [("paused", {"reason": "operator"})])
    writer_id, segment, _, _, _ = parse_record_ref(lost.record_ref)
    segment_path(workspace.control, writer_id, segment).unlink()

    report = workspace.check()
    problems = {finding.job_id: finding.problem for finding in report.findings}
    assert problems == {corrupt_id: "checksum_mismatch", lost_id: "missing_segment"}
    assert all(finding.action == "reported" for finding in report.findings)
    assert report.markers_checked == 2 and report.unresolved == 2


def test_fsck_reports_a_marker_that_names_the_wrong_frame(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _ = _payload(tmp_path)
    marker = _chain(
        workspace,
        workspace.submit(payload, "jobs"),
        [("ready", {"reason": "submitted"}), ("paused", {"reason": "operator"})],
    )
    older = next(entry for entry in iter_journal_frames(workspace.control) if entry.frame["kind"] == "ready")
    marker.path.rename(marker.path.with_name(marker.path.name.replace(marker.record_ref, older.record_ref)))
    report = workspace.check()
    assert [finding.problem for finding in report.findings] == ["identity_mismatch"]


def test_fsck_repairs_a_damaged_marker_and_the_job_runs_again(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path)
    marker = _chain(
        workspace,
        workspace.submit(payload, "jobs"),
        [
            ("ready", {"reason": "submitted"}),
            ("paused", {"reason": "operator"}),
            ("ready", {"reason": "continue"}),
        ],
    )
    _corrupt_frame(workspace, marker)
    with pytest.raises(Exception):  # noqa: B017 - this API deliberately raises plain Exception subclasses
        workspace.read_state(marker)

    report = workspace.check(repair=True)
    (finding,) = report.findings
    assert finding.action == "repaired" and finding.problem == "checksum_mismatch"
    assert report.unresolved == 0

    repaired = workspace.find_marker_by_id(job_id)
    assert repaired is not None
    assert repaired.kind == "ready" and repaired.generation == marker.generation + 1
    state = workspace.read_state(repaired)
    assert state["reason"] == "fsck_repair"
    assert state["fsck_repair"]["replaced_record_ref"] == marker.record_ref
    assert state["step"] == "run"
    # The chain still leads back into the readable history of the job.
    assert read_record(workspace.control, str(state["previous_record_ref"]))["kind"] == "paused"
    assert workspace.check().ok

    # A repaired job is not merely readable: a manager schedules and finishes it.
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=60.0, poll_interval=0.05)
    finished = workspace.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"


def test_fsck_leaves_an_unrepairable_marker_alone_and_can_quarantine_it(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path)
    marker = _chain(workspace, workspace.submit(payload, "jobs"), [("ready", {"reason": "submitted"})])
    # The only frame of this job is its first: nothing older can be recovered.
    _corrupt_frame(workspace, marker)
    (finding,) = workspace.check(repair=True).findings
    assert finding.action == "reported"
    assert marker.path.is_file()

    (finding,) = workspace.check(repair=True, quarantine_unrepairable=True).findings
    assert finding.action == "quarantined"
    assert not marker.path.exists()
    assert workspace.find_marker_by_id(job_id) is None
    assert any((workspace.control / "quarantine").iterdir())
    with pytest.raises(ValueError):
        workspace.check(quarantine_unrepairable=True)


def test_fsck_never_touches_a_marker_whose_manager_is_still_alive(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path)
    manager_id = str(uuid.uuid4())
    marker = _chain(
        workspace,
        workspace.submit(payload, "jobs"),
        [
            ("ready", {"reason": "submitted"}),
            ("claimed", {"reason": "claimed", "manager_id": manager_id, "lease_seconds": 900.0}),
            ("running", {"reason": "launched", "manager_id": manager_id}),
        ],
    )
    _corrupt_frame(workspace, marker)
    heartbeat = workspace.control / "managers" / manager_id / "heartbeat.json"
    heartbeat.parent.mkdir(parents=True)
    heartbeat.write_text(
        json.dumps({"manager_id": manager_id, "updated_at": util_module.utc_now()}),
        encoding="utf-8",
    )

    (finding,) = workspace.check(repair=True, quarantine_unrepairable=True).findings
    assert finding.action == "skipped_live" and manager_id in finding.detail
    assert marker.path.is_file()

    # Once that manager stops heartbeating, the same damage is repairable.
    heartbeat.write_text(
        json.dumps({"manager_id": manager_id, "updated_at": "2020-01-01T00:00:00.000000Z"}),
        encoding="utf-8",
    )
    (finding,) = workspace.check(repair=True).findings
    assert finding.action == "repaired"
    repaired = workspace.find_marker_by_id(job_id)
    assert repaired is not None and repaired.kind == "running"
    assert workspace.read_state(repaired)["manager_id"] == manager_id


def test_fsck_reports_an_uninterpretable_state_entry(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _ = _payload(tmp_path)
    marker = workspace.submit(payload, "jobs")
    broken = marker.path.with_name("not-a-job-key.p500.g1.init")
    broken.write_text("", encoding="utf-8")
    report = workspace.check()
    assert [finding.problem for finding in report.findings] == ["unparseable_name"]
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "fsck", register_ws(context, tmp_path / "workspace")], context) == 1
    assert "unparseable_name" in capsys.readouterr().out
