"""Retention-driven garbage collection of a workflow workspace.

The workspace under test is built by running real jobs through a real task
manager, so the attempt-control directories, journal segments, manager
directories, and empty placement mirrors are the ones the engine actually
leaves behind. Only the artefacts a short test cannot produce honestly — a
retired transfer bundle, a second journal writer with two segments, a manager
that is still heartbeating — are crafted in place, and everything is aged with
``os.utime`` rather than by waiting.
"""

import json
import os
import shutil
import time
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow import gc as gc_module
from httk.workflow._util import read_json, utc_now, write_json_atomic
from httk.workflow.gc import GcReport
from httk.workflow.journal import (
    SEGMENT_HEADER,
    encode_record_ref,
    iter_journal_frames,
    parse_record_ref,
    read_record,
    segment_path,
)
from httk.workflow.models import Marker
from httk.workflow.workflow_cli import command

_DAY = 86400.0

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
(workdir / "kept.txt").write_text("persistent application data\\n")
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
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


def _payload(root: Path, name: str) -> tuple[Path, str]:
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
                "format_version": 2,
                "id": job_id,
                "tag": "gc-test",
                "name": "Collection test job",
                "workflow": "tests.gc",
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


def _age(path: Path, days: float) -> None:
    """Backdate one entry, and everything below it, by *days*."""

    when = time.time() - days * _DAY
    if path.is_dir() and not path.is_symlink():
        for child in sorted(path.rglob("*"), reverse=True):
            os.utime(child, (when, when), follow_symlinks=False)
    os.utime(path, (when, when), follow_symlinks=False)


def _attempt_directory(payload: Path, *, days: float) -> Path:
    """Add one aged attempt-control directory to a payload."""

    control = payload / f"attempts/{uuid.uuid4()}"
    (control / "outcome.ready").mkdir(parents=True)
    logs = payload / "logs"
    logs.mkdir(exist_ok=True)
    (logs / "stdio.out").write_text("older attempt\n", encoding="utf-8")
    _age(control, days)
    return control


def _stale_heartbeat(manager_dir: Path, *, days: float) -> None:
    """Rewrite one manager heartbeat so its lease is long expired."""

    heartbeat = read_json(manager_dir / "heartbeat.json")
    heartbeat["updated_at"] = _timestamp(days)
    write_json_atomic(manager_dir / "heartbeat.json", heartbeat)


def _timestamp(days_ago: float) -> str:
    """Return a protocol timestamp *days_ago* days in the past."""

    from datetime import UTC, datetime, timedelta

    moment = datetime.now(UTC) - timedelta(days=days_ago)
    return moment.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _manager_directory(workspace: Workspace, writer_id: str, *, live: bool, days: float) -> Path:
    """Craft one manager directory naming *writer_id*."""

    manager_id = str(uuid.uuid4())
    manager_dir = workspace.control / "managers" / manager_id
    manager_dir.mkdir(parents=True)
    write_json_atomic(
        manager_dir / "manager.json",
        {
            "format": "httk-workflow-manager",
            "format_version": 2,
            "manager_id": manager_id,
            "writer_id": writer_id,
            "hostname": "test",
            "pid": 1,
        },
    )
    write_json_atomic(
        manager_dir / "heartbeat.json",
        {"manager_id": manager_id, "updated_at": utc_now() if live else _timestamp(days)},
    )
    _age(manager_dir, days)
    return manager_dir


@pytest.mark.timing
def test_succeeded_attempt_control_waits_for_lease_grace_while_runner_lingers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GC must not remove a published attempt while its runner may live."""

    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    workspace.set_policy({"lease_seconds": 1.0, "retention": {"attempt_control_days": 0.0}})
    payload, job_id = _payload(tmp_path / "source", "lingering")
    runner = payload / "files" / "runner"
    runner.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
trap '' TERM
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.gc run
step_run() {
    httk_workflow_succeed
    sleep 1.5
}
httk_workflow_main
""",
        encoding="utf-8",
    )
    runner.chmod(0o755)
    workspace.submit(payload, "project/lingering")
    # The real committing manager is allowed to retain the process, but this
    # test deliberately leaves the control tree for GC to exercise its grace.
    monkeypatch.setattr("httk.workflow._manager_commit._remove_committed_attempt_control", lambda *args: None)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            manager.tick()
            marker = workspace.find_marker_by_id(job_id)
            if marker is not None and marker.kind == "succeeded":
                break
            time.sleep(0.01)
        else:
            raise AssertionError("job did not succeed")
        root = workspace.payload_path(marker.placement, marker.job_key)
        controls = list((root / "attempts").iterdir())
        assert len(controls) == 1
        assert manager._running and next(iter(manager._running.values())).process.poll() is None

        assert workspace.collect_garbage().category("attempt_control").removed == 0
        assert controls[0].is_dir()

        process = next(iter(manager._running.values())).process
        process_deadline = time.monotonic() + 10.0
        while process.poll() is None and time.monotonic() < process_deadline:
            time.sleep(0.01)
        assert process.poll() is not None
        time.sleep(0.1)
        report = workspace.collect_garbage()

    assert report.category("attempt_control").removed == 1
    assert not controls[0].exists()


def _two_segment_writer(workspace: Workspace, *, days: float) -> tuple[str, list[Path], str]:
    """Open one writer, force it to rotate, and age both of its segments."""

    with workspace.open_journal_writer() as writer:
        writer.append({"filler": "x" * 2000, "index": 0})
        second = writer.append({"filler": "x" * 2000, "index": 1})
    writer_id, segment, _offset, _length, _checksum = parse_record_ref(second)
    assert segment == 1, "the writer was expected to rotate onto a second segment"
    writer_dir = workspace.control / "journal" / writer_id
    segments = sorted(writer_dir.glob("*.hwj"))
    assert len(segments) == 2
    _age(writer_dir, days)
    return writer_id, segments, second


def _sealed_ledger(workspace: Workspace, record_ref: str) -> Path:
    """Publish a sealed transfer ledger whose marker names *record_ref*."""

    transfer_id = str(uuid.uuid4())
    ledger = workspace.control / "transfers" / f"{transfer_id}.json"
    write_json_atomic(
        ledger,
        {
            "format": "httk-workflow-transfer",
            "format_version": 2,
            "transfer_id": transfer_id,
            "status": "sealed",
            "sealed_marker": f"{uuid.uuid4()}.p500.g1.{record_ref}",
        },
    )
    return ledger


def _finished_job(workspace: Workspace, root: Path, name: str) -> tuple[str, Marker, Path]:
    """Submit and finish one job, returning its id, marker, and payload."""

    payload, job_id = _payload(root, name)
    workspace.submit(payload, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    workspace.set_policy({"visibility_deadline_seconds": 0.01})
    return job_id, marker, workspace.payload_path(marker.placement, marker.job_key)


def test_removed_terminal_jobs_are_collected_and_audited(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    job_id, marker, payload = _finished_job(workspace, tmp_path / "source", "removed")
    shutil.rmtree(payload)

    dry_run = workspace.collect_garbage(dry_run=True)
    removed = dry_run.category("removed_jobs")
    assert removed.candidates == 1 and removed.removed == 0
    assert marker.path.as_posix() in removed.entries
    assert marker.path.is_file()

    report = workspace.collect_garbage()
    assert report.category("removed_jobs").removed == 1
    assert dry_run.category("placement_directories").candidates == report.category("placement_directories").candidates
    assert report.removed_jobs == (marker.job_key,)
    assert job_id not in {item.job_id for item in workspace.scan_markers()}
    assert not marker.path.exists()
    assert report.record_ref is not None
    frame = read_record(workspace.control, report.record_ref)
    assert frame["removed_jobs"] == [marker.job_key]

    second = workspace.collect_garbage()
    assert second.category("removed_jobs").candidates == 0
    assert second.removed_jobs == ()
    assert second.record_ref is None


def test_existing_terminal_payload_is_not_a_removed_job_candidate(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    job_id, _marker, payload = _finished_job(workspace, tmp_path / "source", "kept")
    report = workspace.collect_garbage()
    assert report.category("removed_jobs").candidates == 0
    assert workspace.find_marker_by_id(job_id) is not None
    assert payload.is_dir()


def test_removed_submitted_and_ready_jobs_are_collected(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    markers: list[Marker] = []
    for name in ("submitted", "ready"):
        payload, job_id = _payload(tmp_path / "source", name)
        marker = workspace.submit(payload, "jobs")
        if name == "ready":
            with workspace.open_journal_writer() as writer:
                marker = workspace.transition(writer, marker, "ready", {"reason": "submitted"})
        assert marker.job_id == job_id
        markers.append(marker)
        shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))

    workspace.set_policy({"visibility_deadline_seconds": 0.01})
    report = workspace.collect_garbage()
    category = report.category("removed_jobs")
    assert category.candidates == 2 and category.removed == 2
    assert report.removed_jobs == tuple(marker.job_key for marker in markers)
    assert all(not marker.path.exists() for marker in markers)


def test_manager_run_idle_collects_removed_submitted_job(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", "pending")
    marker = workspace.submit(payload, "jobs")
    shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))

    def stop_after_start(self, *, poll_interval, drain_timeout):
        assert workspace.find_marker_by_id(job_id) is None

    monkeypatch.setattr(TaskManager, "serve", stop_after_start)
    context = CLIContext("httk", tmp_path)
    assert command(["manager", "run", "--workspace", str(workspace.root), "--by-path", "--idle"], context) == 0


@pytest.mark.parametrize("kind", ("claimed", "waiting", "paused"))
def test_non_terminal_marker_without_payload_is_not_collected(kind: str, tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", kind)
    marker = workspace.submit(payload, "jobs")
    with workspace.open_journal_writer() as writer:
        marker = workspace.transition(writer, marker, kind, {})
    shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))

    report = workspace.collect_garbage()
    assert report.category("removed_jobs").candidates == 0
    assert workspace.find_marker_by_id(job_id) is not None


def test_ready_marker_claimed_between_selection_and_unlink_is_kept(tmp_path: Path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", "racing")
    marker = workspace.submit(payload, "jobs")
    with workspace.open_journal_writer() as writer:
        marker = workspace.transition(writer, marker, "ready", {"reason": "submitted"})
    shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))

    new_ready: Marker | None = None

    def claim_during_visibility_wait(paths, *, deadline_seconds):
        nonlocal new_ready
        absent = tuple(paths)
        with workspace.open_journal_writer() as writer:
            claimed = workspace.transition(writer, marker, "claimed", {"attempt_id": str(uuid.uuid4())})
            new_ready = workspace.transition(
                writer,
                claimed,
                "ready",
                {"attempt_id": str(uuid.uuid4()), "reason": "retry"},
            )
        assert not marker.path.exists()
        assert new_ready.path.is_file()
        return absent

    monkeypatch.setattr(gc_module, "wait_for_paths", claim_during_visibility_wait)
    report = workspace.collect_garbage()

    assert report.category("removed_jobs").candidates == 0
    assert report.removed_jobs == ()
    current = workspace.find_marker_by_id(job_id)
    assert current is not None and current.kind == "ready"
    assert new_ready is not None
    assert current.generation == new_ready.generation > marker.generation
    assert current.path == new_ready.path and current.path.is_file()
    assert current.path != marker.path
    assert not workspace.payload_path(current.placement, current.job_key).exists()


def test_removed_join_child_is_kept_until_its_non_terminal_parent_is_terminal(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    child_id, child_marker, child_payload = _finished_job(workspace, tmp_path / "source", "child")
    parent_payload, parent_id = _payload(tmp_path / "source", "parent")
    parent_marker = workspace.submit(parent_payload, "jobs")
    with workspace.open_journal_writer() as writer:
        workspace.transition(
            writer,
            parent_marker,
            "waiting",
            {
                "join": {
                    "children": [
                        {
                            "workspace_id": workspace.workspace_id,
                            "job_id": child_id,
                            "job_key": child_marker.job_key,
                            "placement_hint": child_marker.placement.as_posix(),
                        }
                    ],
                    "condition": "all_terminal",
                }
            },
        )
    shutil.rmtree(child_payload)

    report = workspace.collect_garbage()
    category = report.category("removed_jobs")
    assert category.candidates == 1 and category.removed == 0
    assert workspace.find_marker_by_id(child_id) is not None
    assert workspace.find_marker_by_id(parent_id) is not None
    assert any(child_marker.job_key in reason and "non-terminal parent" in reason for reason in report.skipped)


def test_removed_jobs_stands_down_when_a_non_terminal_state_is_unreadable(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    _job_id, terminal_marker, terminal_payload = _finished_job(workspace, tmp_path / "source", "candidate")
    shutil.rmtree(terminal_payload)
    parent_payload, _parent_id = _payload(tmp_path / "source", "unreadable-parent")
    parent_marker = workspace.submit(parent_payload, "jobs")
    with workspace.open_journal_writer() as writer:
        parent_marker = workspace.transition(writer, parent_marker, "ready", {})
    writer_id, segment, _offset, _length, _checksum = parse_record_ref(parent_marker.record_ref)
    segment_path(workspace.control, writer_id, segment).unlink()

    report = workspace.collect_garbage()
    assert report.category("removed_jobs").removed == 0
    assert workspace.find_marker_by_id(terminal_marker.job_id) is not None
    assert any(reason.startswith("removed_jobs:") for reason in report.skipped)


def test_removed_jobs_stands_down_while_a_marker_is_committing(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    _job_id, terminal_marker, terminal_payload = _finished_job(workspace, tmp_path / "source", "candidate")
    shutil.rmtree(terminal_payload)
    committing_payload, _committing_id = _payload(tmp_path / "source", "committing")
    committing_marker = workspace.submit(committing_payload, "jobs")
    with workspace.open_journal_writer() as writer:
        committing_marker = workspace.transition(writer, committing_marker, "committing", {})

    report = workspace.collect_garbage()
    assert report.category("removed_jobs").removed == 0
    assert workspace.find_marker_by_id(terminal_marker.job_id) is not None
    assert workspace.find_marker_by_id(committing_marker.job_id) is not None
    assert any(reason.startswith("removed_jobs:") for reason in report.skipped)


def _journal_projection_workspace(root: Path) -> tuple[Workspace, Marker]:
    """Build a workspace where an aged segment is referenced only by a removed job."""

    workspace = Workspace.initialize(root / "workspace", durable=False)
    workspace.set_policy({"visibility_deadline_seconds": 0.01, "retention": {"journal_days": 0.0}})
    payload, _job_id = _payload(root / "source", "journal-projection")
    marker = workspace.submit(payload, "jobs")
    with workspace.open_journal_writer() as writer:
        marker = workspace.transition(writer, marker, "succeeded", {})
    writer_id, segment, _offset, _length, _checksum = parse_record_ref(marker.record_ref)
    segment_file = segment_path(workspace.control, writer_id, segment)
    _age(segment_file, 1)
    shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))
    return workspace, marker


def test_removed_jobs_dry_run_projects_journal_candidates_like_a_real_run(tmp_path: Path) -> None:
    dry_workspace, dry_marker = _journal_projection_workspace(tmp_path / "dry")
    real_workspace, real_marker = _journal_projection_workspace(tmp_path / "real")

    dry_report = dry_workspace.collect_garbage(dry_run=True)
    real_report = real_workspace.collect_garbage()
    assert dry_report.category("journal_segments").candidates == real_report.category("journal_segments").candidates
    assert dry_report.category("journal_segments").candidates == 1
    assert dry_report.removed_jobs == ()
    assert real_report.removed_jobs == (real_marker.job_key,)
    assert dry_marker.path.is_file()


class _Fixture:
    """One aged workspace and the paths every assertion refers to."""

    def __init__(self, tmp_path: Path) -> None:
        self.workspace = Workspace.initialize(
            tmp_path / "workspace",
            durable=False,
        )
        control = self.workspace.control
        self.control = control

        # Two jobs run to completion by a real manager, at a deep placement so
        # that every state kind they pass through leaves an empty mirror.
        self.job_ids: list[str] = []
        for index in range(2):
            payload, job_id = _payload(tmp_path / "source", f"job-{index}")
            self.workspace.submit(payload, f"project/deep/branch-{index}")
            self.job_ids.append(job_id)
        with TaskManager(self.workspace, heartbeat_interval=0.01) as manager:
            manager.run_until_idle()
        self.manager_writer = manager.writer.writer_id
        self.manager_directory = _manager_directory(self.workspace, self.manager_writer, live=False, days=60)
        _stale_heartbeat(self.manager_directory, days=60)

        markers = {marker.job_id: marker for marker in self.workspace.scan_markers()}
        assert all(markers[job_id].kind == "succeeded" for job_id in self.job_ids)
        for kind in ("submitted", "ready", "claimed", "running", "committing"):
            (control / "state" / kind / "project" / "deep" / "leftover").mkdir(parents=True, exist_ok=True)
        self.payloads = [
            self.workspace.payload_path(markers[job_id].placement, markers[job_id].job_key) for job_id in self.job_ids
        ]
        self.workdirs = [payload / "run" for payload in self.payloads]

        # Per terminal job: a stale post-commit leftover plus a fresh one that
        # supplies transaction-trash evidence for the category test.
        self.old_attempts = [_attempt_directory(payload, days=30) for payload in self.payloads]
        self.newest_attempts: list[Path] = []
        for payload in self.payloads:
            self.newest_attempts.append(_attempt_directory(payload, days=0))
        # A quiescent job that never ran may collect aged attempt directories
        # too; the lease grace still protects a recent one.
        pending_payload, pending_id = _payload(tmp_path / "source", "job-pending")
        self.workspace.submit(pending_payload, "project/deep/pending")
        self.pending_id = pending_id
        pending_marker = self.workspace.find_marker_by_id(pending_id)
        assert pending_marker is not None
        self.pending_payload = self.workspace.payload_path(pending_marker.placement, pending_marker.job_key)
        self.pending_attempts = [_attempt_directory(self.pending_payload, days=90) for _ in range(2)]

        # Transaction trash inside the attempt directory that survives.
        self.trash = self.newest_attempts[0] / "outcome.ready" / "transaction" / "trash" / "replace"
        (self.trash / "old").mkdir(parents=True)
        (self.trash / "old" / "data.txt").write_text("replaced tree\n", encoding="utf-8")
        _age(self.trash, 30)
        self.fresh_trash = self.newest_attempts[1] / "outcome.ready" / "transaction" / "trash" / "replace"
        self.fresh_trash.mkdir(parents=True)
        (self.fresh_trash / "keep.txt").write_text("today\n", encoding="utf-8")

        # Detached-transfer leftovers.
        self.retired_bundle = control / "transfers" / "retired" / str(uuid.uuid4())
        (self.retired_bundle / "bundle" / "files").mkdir(parents=True)
        (self.retired_bundle / "bundle" / "job.json").write_text("{}", encoding="utf-8")
        _age(self.retired_bundle, 30)
        self.fresh_bundle = control / "transfers" / "retired" / str(uuid.uuid4())
        (self.fresh_bundle / "bundle").mkdir(parents=True)
        self.ack = control / "transfers" / "acks" / f"{uuid.uuid4()}.json"
        write_json_atomic(self.ack, {"format": "httk-workflow-transfer-acknowledgement"})
        _age(self.ack, 30)
        self.imported = control / "transfers" / "imported" / f"{uuid.uuid4()}.json"
        write_json_atomic(self.imported, {"status": "imported"})
        _age(self.imported, 30)

        # A sealed bundle is never collected, however old it looks.
        self.sealed_payload = self.workspace.root / "project" / "deep" / "sealed" / str(uuid.uuid4())
        (self.sealed_payload / ".httk-transfer").mkdir(parents=True)
        (self.sealed_payload / ".httk-transfer" / "manifest.json").write_text("{}", encoding="utf-8")
        _age(self.sealed_payload, 400)

        # Staging entries and request receipts.
        self.stale_tmp = control / "tmp" / f"submit.{uuid.uuid4()}"
        self.stale_tmp.mkdir(parents=True)
        (self.stale_tmp / "job.json").write_text("{}", encoding="utf-8")
        _age(self.stale_tmp, 3)
        self.fresh_tmp = control / "tmp" / f"submit.{uuid.uuid4()}"
        self.fresh_tmp.mkdir(parents=True)
        self.stale_request_tmp = control / "requests" / "tmp" / f"{uuid.uuid4()}.json"
        write_json_atomic(self.stale_request_tmp, {"format": "httk-workflow-request"})
        _age(self.stale_request_tmp, 3)
        self.claimed_request = control / "requests" / "claimed" / str(uuid.uuid4()) / f"{uuid.uuid4()}.json"
        write_json_atomic(self.claimed_request, {"format": "httk-workflow-request"})
        _age(self.claimed_request.parent, 40)

        # The quarantine is outside generic collection entirely.
        self.quarantined = control / "quarantine" / f"{int(time.time())}-{uuid.uuid4()}"
        self.quarantined.mkdir(parents=True)
        (self.quarantined / "entry").write_text("malformed\n", encoding="utf-8")
        _age(self.quarantined, 400)

        # A published runner is never collected in this version of the tool.
        self.runner = control / "runners" / "shared-runner"
        self.runner.parent.mkdir(parents=True, exist_ok=True)
        self.runner.write_text("#!/bin/sh\n", encoding="utf-8")
        _age(self.runner, 400)

        # Three more journal writers: one wholly collectable with its manager,
        # one holding a segment a sealed bundle references, and one owned by a
        # manager that is still heartbeating.
        self.workspace.set_policy({"journal_segment_bytes": 4096})
        self.dead_writer, self.dead_segments, _ = _two_segment_writer(self.workspace, days=60)
        self.dead_manager = _manager_directory(self.workspace, self.dead_writer, live=False, days=60)
        self.referenced_writer, self.referenced_segments, reference = _two_segment_writer(self.workspace, days=60)
        self.sealed_ledger = _sealed_ledger(self.workspace, reference)
        self.live_writer, self.live_segments, _ = _two_segment_writer(self.workspace, days=60)
        self.live_manager = _manager_directory(self.workspace, self.live_writer, live=True, days=0)

        # The segments the succeeded markers reference, which must survive.
        self.marker_segments = sorted((control / "journal" / self.manager_writer).glob("*.hwj"))
        _age(control / "journal" / self.manager_writer, 60)
        _age(self.manager_directory, 60)

        self.workspace.set_policy(
            {"retention": {"attempt_control_days": 7.0, "journal_days": 30.0, "trash_days": 14.0}}
        )


@pytest.fixture
def aged(tmp_path: Path) -> _Fixture:
    return _Fixture(tmp_path)


def test_a_dry_run_reports_everything_and_touches_nothing(aged: _Fixture) -> None:
    before = tree_digest(aged.workspace.root)
    report = aged.workspace.collect_garbage(dry_run=True)
    assert tree_digest(aged.workspace.root) == before
    assert report.dry_run is True
    assert report.removed == 0
    assert report.candidates > 0
    assert report.bytes_reclaimed > 0
    assert report.record_ref is None
    # Every candidate is still exactly where the report found it.
    for category in report.categories:
        for entry in category.entries:
            assert Path(entry).exists()
    # A real run afterwards removes precisely what the dry run predicted.
    real = aged.workspace.collect_garbage()
    assert real.removed == report.candidates
    assert {entry for category in real.categories for entry in category.entries} == {
        entry for category in report.categories for entry in category.entries
    }


def test_each_category_collects_only_the_aged_entries(aged: _Fixture) -> None:
    report = aged.workspace.collect_garbage()
    assert report.candidates == report.removed

    # 1. Aged attempt-control directories of quiescent jobs, never the newest
    # failed/cancelled evidence.
    assert [directory.exists() for directory in aged.old_attempts] == [False, False]
    assert all(directory.is_dir() for directory in aged.newest_attempts)
    assert report.category("attempt_control").removed == 4
    assert all(not directory.exists() for directory in aged.pending_attempts)

    # 2. Transaction trash of a job that has left committing.
    assert not aged.trash.exists()
    assert aged.fresh_trash.is_dir()
    assert report.category("transaction_trash").removed == 1

    # 3. Retired transfer bundles and the receipts of completed imports.
    assert not aged.retired_bundle.exists()
    assert aged.fresh_bundle.is_dir()
    assert report.category("retired_bundles").removed == 1
    assert not aged.ack.exists() and not aged.imported.exists()
    assert report.category("transfer_records").removed == 2
    # The ledger survives its bundle: it is the audit record of the transfer.
    assert aged.sealed_ledger.is_file()

    # 4. Journal segments: unreferenced, aged, and written by a dead writer.
    assert not any(path.exists() for path in aged.dead_segments)
    assert not (aged.control / "journal" / aged.dead_writer).exists()
    # The segment a sealed bundle's marker references survives; the older
    # segment of the same writer does not.
    assert not aged.referenced_segments[0].exists()
    assert aged.referenced_segments[1].is_file()
    # Everything a live manager wrote is untouchable.
    assert all(path.is_file() for path in aged.live_segments)
    # As is every segment a current marker references.
    assert all(path.is_file() for path in aged.marker_segments)
    assert report.category("journal_segments").removed == 3

    # 5. Manager directories of dead incarnations whose segments are gone.
    assert not aged.dead_manager.exists()
    assert aged.live_manager.is_dir()
    # The real manager's directory stays: its segments are still referenced.
    assert aged.manager_directory.is_dir()
    assert report.category("manager_directories").removed == 1

    # 6 and 7. Always-safe categories.
    assert report.category("placement_directories").removed > 0
    assert not aged.stale_tmp.exists()
    assert aged.fresh_tmp.is_dir()
    assert not aged.stale_request_tmp.exists()
    assert report.category("tmp_entries").removed == 2
    assert not aged.claimed_request.exists()
    assert report.category("retired_requests").removed == 1

    # Nothing outside the collector's remit was touched.
    assert (aged.quarantined / "entry").is_file()
    assert (aged.sealed_payload / ".httk-transfer" / "manifest.json").is_file()
    assert aged.runner.is_file()
    for workdir in aged.workdirs:
        assert (workdir / "kept.txt").is_file()
    for job_id in aged.job_ids:
        marker = aged.workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "succeeded"
        # The state of every job still resolves through the journal.
        assert aged.workspace.read_state(marker)["kind"] == "succeeded"


def test_a_second_collection_is_a_no_op(aged: _Fixture) -> None:
    first = aged.workspace.collect_garbage()
    assert first.removed > 0
    settled = tree_digest(aged.workspace.root)
    second = aged.workspace.collect_garbage()
    assert second.removed == 0
    assert second.candidates == 0
    assert second.record_ref is None
    # A run that removes nothing writes nothing, not even a journal writer.
    assert tree_digest(aged.workspace.root) == settled


def test_a_collection_can_select_only_named_categories(aged: _Fixture) -> None:
    report = aged.workspace.collect_garbage(categories=("placement_directories", "tmp_entries"))

    assert [category.name for category in report.categories] == list(gc_module.GC_CATEGORIES)
    assert report.category("tmp_entries").removed == 2
    assert report.category("placement_directories").removed > 0
    for name in set(gc_module.GC_CATEGORIES) - {"placement_directories", "tmp_entries"}:
        category = report.category(name)
        assert category.skipped and category.skip_reason == "not selected"


def test_manager_directory_selection_also_checks_surviving_journal_segments(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    workspace.set_policy({"retention": {"journal_days": 30.0}})
    with workspace.open_journal_writer() as writer:
        writer.append({"format": "test-frame"})
        writer_id = writer.writer_id
    manager_dir = _manager_directory(workspace, writer_id, live=False, days=60)

    report = workspace.collect_garbage(categories=("manager_directories",))

    assert manager_dir.is_dir()
    assert (workspace.control / "journal" / writer_id).is_dir()
    assert report.category("journal_segments").skipped is False
    assert report.category("manager_directories").removed == 0


def test_manager_gc_frames_use_its_writer_and_clean_idle_removes_empty_writer(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, _job_id = _payload(tmp_path / "source", "processed")
    workspace.submit(payload, "project/processed")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
        writer_id = manager.writer.writer_id

    writer_dirs = [path for path in (workspace.control / "journal").iterdir() if path.is_dir()]
    assert [path.name for path in writer_dirs] == [writer_id]
    gc_frames = [
        frame for frame in iter_journal_frames(workspace.control) if frame.frame.get("format") == "httk-workflow-gc"
    ]
    assert gc_frames
    assert all(parse_record_ref(frame.record_ref)[0] == writer_id for frame in gc_frames)

    idle = Workspace.initialize(tmp_path / "idle", durable=False)
    with TaskManager(idle, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
        idle_writer_id = manager.writer.writer_id
    assert not (idle.control / "journal" / idle_writer_id).exists()


def test_header_only_writer_stays_when_a_marker_names_it(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, _job_id = _payload(tmp_path / "source", "marker")
    submitted = workspace.submit(payload, "project/marker")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        reference = encode_record_ref(manager.writer.writer_id, 0, len(SEGMENT_HEADER), 1, b"0" * 32)
        marker = workspace.marker_path(
            "succeeded",
            submitted.placement,
            submitted.job_key,
            submitted.priority,
            submitted.generation + 1,
            reference,
        )
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
        relocating = workspace.marker_path(
            "relocating",
            submitted.placement,
            submitted.job_key,
            submitted.priority,
            submitted.generation + 2,
            reference,
        )
        relocating.parent.mkdir(parents=True, exist_ok=True)
        relocating.touch()
        manager_id = manager.writer.writer_id

    assert (workspace.control / "journal" / manager_id).is_dir()
    assert any(item.record_ref == reference for item in workspace.scan_markers())


def test_manager_startup_removes_a_terminal_orphan_without_cli_gc(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", "processed")
    workspace.submit(payload, "project/processed")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    assert workspace.find_marker_by_id(job_id) is None


def test_clean_writer_segment_waits_for_journal_days(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    workspace.set_policy({"retention": {"journal_days": 30.0}})
    stale_tmp = workspace.control / "tmp" / "stale"
    stale_tmp.mkdir(parents=True)
    _age(stale_tmp, 2)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
        writer_id = manager.writer.writer_id

    segment = next((workspace.control / "journal" / writer_id).glob("*.hwj"))
    before = workspace.collect_garbage(categories=("journal_segments",))
    assert before.category("journal_segments").removed == 0
    assert segment.is_file()

    _age(segment, 31)
    after = workspace.collect_garbage(categories=("journal_segments",))
    assert after.category("journal_segments").removed == 1
    assert not segment.exists()


def test_clean_manager_exit_collects_aged_dead_writer_and_manager(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    workspace.set_policy({"journal_segment_bytes": 4096, "retention": {"journal_days": 0.0}})
    writer_id, segments, _ = _two_segment_writer(workspace, days=60)
    crashed_dir = _manager_directory(workspace, writer_id, live=False, days=60)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
        exiting_writer_id = manager.writer.writer_id

    assert not crashed_dir.exists()
    assert not any(segment.exists() for segment in segments)
    gc_frames = [
        frame for frame in iter_journal_frames(workspace.control) if frame.frame.get("format") == "httk-workflow-gc"
    ]
    assert gc_frames
    assert all(parse_record_ref(frame.record_ref)[0] == exiting_writer_id for frame in gc_frames)


def test_supplied_writer_is_protected_even_with_a_stale_self_heartbeat(tmp_path: Path) -> None:
    workspace = Workspace.initialize(
        tmp_path / "workspace",
        durable=False,
        policy={"journal_segment_bytes": 4096, "retention": {"journal_days": 0.0}},
    )
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
        own_writer_id = manager.writer.writer_id
        own_manager_dir = manager.manager_directory
        own_segment = next((workspace.control / "journal" / own_writer_id).glob("*.hwj"))
        _age(own_segment, 60)
        heartbeat = read_json(own_manager_dir / "heartbeat.json")
        heartbeat["updated_at"] = _timestamp(60)
        write_json_atomic(own_manager_dir / "heartbeat.json", heartbeat)

        with workspace.open_journal_writer() as writer:
            writer.append({"format": "unreferenced-external-frame"})
            external_writer_id = writer.writer_id
        external_segment = next((workspace.control / "journal" / external_writer_id).glob("*.hwj"))
        _age(external_segment, 60)

        report = workspace.collect_garbage(journal_writer=manager.writer)

        assert own_segment.is_file()
        assert own_manager_dir.is_dir()
        assert not external_segment.exists()
        assert report.record_ref is not None
        gc_writer_id, gc_segment, _offset, _length, _checksum = parse_record_ref(report.record_ref)
        assert gc_writer_id == own_writer_id
        assert segment_path(workspace.control, gc_writer_id, gc_segment).is_file()
        assert read_record(workspace.control, report.record_ref)["format"] == "httk-workflow-gc"


def test_non_terminal_frame_chain_protects_aged_history_until_terminal(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False, policy={"journal_segment_bytes": 4096})
    workspace.set_policy({"retention": {"journal_days": 0.0}})
    payload, _job_id = _payload(tmp_path / "source", "chain")
    marker = workspace.submit(payload, "jobs")
    with workspace.open_journal_writer() as writer:
        marker = workspace.transition(writer, marker, "ready", {})
        writer.append({"format": "chain-filler", "filler": "x" * 4000})
        marker = workspace.transition(writer, marker, "paused", {})
        writer_id = writer.writer_id
    current_writer_segments = sorted((workspace.control / "journal" / writer_id).glob("*.hwj"))
    assert len(current_writer_segments) == 3
    _age(workspace.control / "journal" / writer_id, 60)

    report = workspace.collect_garbage()
    assert report.category("journal_segments").removed == 1
    assert current_writer_segments[0].is_file()
    assert not current_writer_segments[1].exists()
    assert current_writer_segments[2].is_file()

    with workspace.open_journal_writer() as writer:
        workspace.transition(writer, marker, "succeeded", {})
    _age(workspace.control / "journal" / writer_id, 60)
    report = workspace.collect_garbage()
    old_writer_entries = set(report.category("journal_segments").entries)
    assert sum(str(segment) in old_writer_entries for segment in current_writer_segments) == 2
    assert not current_writer_segments[0].exists()
    assert not current_writer_segments[2].exists()


def test_keep_disables_a_retention_category(aged: _Fixture) -> None:
    aged.workspace.set_policy({"retention": {"journal_days": "keep"}})
    report = aged.workspace.collect_garbage()

    assert report.category("journal_segments").skipped is True
    assert report.category("journal_segments").skip_reason == "retention.journal_days is not configured"
    assert all(path.is_file() for path in aged.dead_segments)


def test_default_retention_collects_policy_gated_categories(aged: _Fixture) -> None:
    aged.workspace.set_policy({"retention": {}})
    report = aged.workspace.collect_garbage()
    collected = {category.name for category in report.categories if category.candidates}
    assert {"transaction_trash", "retired_bundles", "transfer_records", "journal_segments"} <= collected
    assert report.category("attempt_control").skipped is True
    assert report.category("attempt_control").skip_reason == "retention.attempt_control_days is not configured"
    assert report.category("transaction_trash").removed == 1
    assert report.category("retired_bundles").removed == 1
    assert report.category("transfer_records").removed == 2
    assert report.category("journal_segments").removed == 3
    assert not any(path.exists() for path in aged.dead_segments)
    assert not aged.dead_manager.exists()
    assert not aged.referenced_segments[0].exists()
    assert aged.referenced_segments[1].exists()
    assert aged.live_manager.is_dir()


def test_unset_attempt_control_retention_keeps_aged_attempts(aged: _Fixture) -> None:
    aged.workspace.set_policy({"retention": {"journal_days": "keep", "trash_days": "keep"}})
    report = aged.workspace.collect_garbage()

    assert report.category("attempt_control").skipped is True
    assert all(directory.is_dir() for directory in aged.old_attempts)


def test_a_collection_journals_one_frame_summarizing_what_it_removed(aged: _Fixture) -> None:
    from httk.workflow.journal import read_record

    report = aged.workspace.collect_garbage()
    assert report.record_ref is not None
    frame = read_record(aged.workspace.control, report.record_ref)
    assert frame["format"] == "httk-workflow-gc"
    assert frame["workspace_id"] == aged.workspace.workspace_id
    assert frame["removed"] == report.removed
    assert frame["retention"] == {"attempt_control_days": 7.0, "journal_days": 30.0, "trash_days": 14.0}
    categories = frame["categories"]
    assert isinstance(categories, dict)
    assert categories["attempt_control"]["removed"] == 4
    # The frame is not a state frame, so it never disturbs a workspace check.
    assert aged.workspace.check().ok


def test_empty_placement_mirrors_are_pruned_below_every_state_kind(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", "job")
    workspace.submit(payload, "project/deep/nested/leaf")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    state = workspace.control / "state"
    for kind in ("submitted", "ready", "claimed", "running", "committing"):
        (state / kind / "project" / "deep" / "nested" / "leaf").mkdir(parents=True, exist_ok=True)
    assert (state / "ready" / "project" / "deep" / "nested" / "leaf").is_dir()
    report = workspace.collect_garbage()
    assert report.category("placement_directories").removed >= 4
    for kind in ("submitted", "ready", "claimed", "running", "committing"):
        assert not (state / kind / "project").exists()
        # The state kind itself is never removed.
        assert (state / kind).is_dir()
    # The one kind that still holds a marker keeps its whole hierarchy.
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.path.is_file()
    assert (state / "succeeded" / "project" / "deep" / "nested" / "leaf").is_dir()


def test_tree_ownership_preflight_counts_nested_foreign_entries_and_keeps_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "candidate"
    foreign = tree / "nested" / "foreign"
    foreign.parent.mkdir(parents=True)
    foreign.write_text("keep", encoding="utf-8")
    sibling = tree / "sibling"
    sibling.write_text("keep", encoding="utf-8")
    seen: list[Path] = []

    def owner(path: Path) -> bool:
        return path != foreign

    monkeypatch.setattr(gc_module, "_owned_by_current_user", owner)
    assert not gc_module._remove_tree(tree, on_foreign=lambda: seen.append(foreign))
    assert len(seen) == 1
    assert foreign.is_file() and sibling.is_file()


def test_a_concurrent_removal_or_repopulation_is_tolerated(aged: _Fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    """Another actor may remove or recreate a directory under the collector."""

    vanished: list[Path] = []
    real_tree_bytes = gc_module._tree_bytes

    def racing_tree_bytes(path: Path) -> int:
        size = real_tree_bytes(path)
        if not vanished and path.is_dir():
            # Somebody else removed this tree between the walk and the removal.
            vanished.append(path)
            gc_module._remove_tree(path)
        return size

    real_rmdir = Path.rmdir
    repopulated: list[Path] = []

    def racing_rmdir(self: Path) -> None:
        if "state" in self.parts and not repopulated:
            # A transition recreated a marker in this placement mirror just as
            # the collector reached it.
            repopulated.append(self)
            (self / "concurrent.marker").write_text("", encoding="utf-8")
        real_rmdir(self)

    monkeypatch.setattr(gc_module, "_tree_bytes", racing_tree_bytes)
    monkeypatch.setattr(Path, "rmdir", racing_rmdir)
    report = aged.workspace.collect_garbage()
    assert vanished and repopulated
    # The entry another actor removed still counts as collected, and the
    # placement directory that was repopulated under the collector does not.
    assert report.removed >= 1
    placement = report.category("placement_directories")
    assert placement.removed < placement.candidates
    assert repopulated[0].is_dir()


def test_the_gc_command_prints_a_table_and_json(aged: _Fixture, capsys: pytest.CaptureFixture[str]) -> None:
    context = CLIContext("httk", aged.workspace.root)
    root = register_ws(context, aged.workspace.root)
    assert command(["workspace", "gc", "--dry-run", root], context) == 0
    printed = capsys.readouterr().out
    assert "category" in printed and "candidates" in printed
    assert "attempt_control" in printed and "removed_jobs" in printed and "total" in printed
    assert "dry run: nothing was removed" in printed
    assert aged.old_attempts[0].is_dir()

    assert command(["workspace", "gc", "--json", root], context) == 0
    document = json.loads(capsys.readouterr().out)[0]
    assert document["format"] == "httk-workflow-gc"
    assert document["dry_run"] is False
    assert document["removed"] > 0
    assert "removed_jobs" in {category["name"] for category in document["categories"]}
    assert not aged.old_attempts[0].exists()
    # Without a name the command resolves the enclosing workspace: a second
    # collection from inside it is a clean no-op.
    assert command(["workspace", "gc", "--json"], context) == 0
    document = json.loads(capsys.readouterr().out)
    assert document[0]["removed"] == 0


def test_a_symlinked_control_is_skipped_by_gc(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", "job")
    workspace.submit(payload, "project/symlinked-control")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    installed = workspace.payload_path(marker.placement, marker.job_key)
    target = tmp_path / "outside-control"
    target.mkdir()
    sentinel = target / "must-survive"
    sentinel.write_text("outside\n", encoding="utf-8")
    (installed / "attempts").mkdir()
    (installed / "attempts" / "genuine").mkdir()
    linked = installed / "attempts" / "symlinked-control"
    linked.symlink_to(target, target_is_directory=True)
    _age(linked, 30)
    genuine = next(path for path in (installed / "attempts").iterdir() if not path.is_symlink())
    _age(genuine, 30)
    workspace.set_policy({"lease_seconds": 1.0, "retention": {"attempt_control_days": 0.0}})

    report = workspace.collect_garbage()

    assert linked.is_symlink()
    assert not genuine.exists()
    assert sentinel.read_text(encoding="utf-8") == "outside\n"
    assert report.category("attempt_control").removed == 1


def test_a_collection_report_round_trips_through_its_mapping(aged: _Fixture) -> None:
    report: GcReport = aged.workspace.collect_garbage(dry_run=True)
    document = report.as_mapping()
    assert json.loads(json.dumps(document)) == document
    assert [category.name for category in report.categories] == list(gc_module.GC_CATEGORIES)
    assert report.category("no_such_category").candidates == 0
    assert document["candidates"] == report.candidates
