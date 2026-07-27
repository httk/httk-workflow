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
import time
import uuid
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from httk.core import CLIContext

from httk.workflow import TaskManager, Workspace
from httk.workflow import gc as gc_module
from httk.workflow._util import read_json, tree_digest, utc_now, write_json_atomic
from httk.workflow.gc import GcReport
from httk.workflow.journal import parse_record_ref
from httk.workflow.workflow_cli import command

_DAY = 86400.0

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
(workdir / "kept.txt").write_text("persistent application data\\n")
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
                "format_version": 1,
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
    if path.is_dir():
        for child in sorted(path.rglob("*"), reverse=True):
            os.utime(child, (when, when), follow_symlinks=False)
    os.utime(path, (when, when), follow_symlinks=False)


def _attempt_directory(payload: Path, *, days: float) -> Path:
    """Add one aged attempt-control directory to a payload."""

    control = payload / f".httk-attempt.{uuid.uuid4()}"
    (control / "outcome.ready").mkdir(parents=True)
    (control / "stdout.log").write_text("older attempt\n", encoding="utf-8")
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
            "format_version": 1,
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
            "format_version": 1,
            "transfer_id": transfer_id,
            "status": "sealed",
            "sealed_marker": f"{uuid.uuid4()}.p500.g1.{record_ref}",
        },
    )
    return ledger


class _Fixture:
    """One aged workspace and the paths every assertion refers to."""

    def __init__(self, tmp_path: Path) -> None:
        self.workspace = Workspace.initialize(
            tmp_path / "workspace",
            extensions=["detached-transfer-v1"],
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
        self.manager_directory = manager.manager_directory
        self.manager_writer = manager.writer.writer_id
        _stale_heartbeat(self.manager_directory, days=60)

        markers = {marker.job_id: marker for marker in self.workspace.scan_markers()}
        assert all(markers[job_id].kind == "succeeded" for job_id in self.job_ids)
        self.payloads = [
            self.workspace.payload_path(markers[job_id].placement, markers[job_id].job_key) for job_id in self.job_ids
        ]
        self.workdirs = [payload / "run" for payload in self.payloads]

        # Per terminal job: the attempt the manager ran, plus one older one.
        self.old_attempts = [_attempt_directory(payload, days=30) for payload in self.payloads]
        self.newest_attempts: list[Path] = []
        for payload in self.payloads:
            existing = sorted(payload.glob(".httk-attempt.*"))
            newest = max(existing, key=lambda path: path.stat().st_mtime)
            self.newest_attempts.append(newest)
        # A job that never ran keeps every attempt directory it has, whatever
        # its age, because it is not terminal.
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

    # 1. Aged attempt-control directories of terminal jobs, never the newest.
    assert [directory.exists() for directory in aged.old_attempts] == [False, False]
    assert all(directory.is_dir() for directory in aged.newest_attempts)
    assert report.category("attempt_control").removed == 2
    # A job that is not terminal keeps every attempt directory it has.
    assert all(directory.is_dir() for directory in aged.pending_attempts)

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
    assert categories["attempt_control"]["removed"] == 2
    # The frame is not a state frame, so it never disturbs a workspace check.
    assert aged.workspace.check().ok


def test_unset_retention_collects_only_the_always_safe_categories(aged: _Fixture) -> None:
    aged.workspace.set_policy({"retention": {}})
    report = aged.workspace.collect_garbage()
    collected = {category.name for category in report.categories if category.candidates}
    assert collected <= {"placement_directories", "tmp_entries", "retired_requests"}
    assert "tmp_entries" in collected
    assert {reason.split(":")[0] for reason in report.skipped} == {
        "attempt_control",
        "transaction_trash",
        "retired_bundles",
        "transfer_records",
        "journal_segments",
        "manager_directories",
    }
    # Everything a configured retention would have collected is still there.
    assert all(directory.is_dir() for directory in aged.old_attempts)
    assert aged.retired_bundle.is_dir()
    assert all(path.is_file() for path in aged.dead_segments)
    assert aged.dead_manager.is_dir()


def test_empty_placement_mirrors_are_pruned_below_every_state_kind(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    payload, job_id = _payload(tmp_path / "source", "job")
    workspace.submit(payload, "project/deep/nested/leaf")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    state = workspace.control / "state"
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
    root = str(aged.workspace.root)
    assert command(["workspace", "gc", root, "--dry-run"], context) == 0
    printed = capsys.readouterr().out
    assert "category" in printed and "candidates" in printed
    assert "attempt_control" in printed and "total" in printed
    assert "dry run: nothing was removed" in printed
    assert aged.old_attempts[0].is_dir()

    assert command(["workspace", "gc", root, "--json"], context) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["format"] == "httk-workflow-gc"
    assert document["dry_run"] is False
    assert document["removed"] > 0
    assert not aged.old_attempts[0].exists()

    assert command(["workspace", "gc"], context) == 2


def test_a_collection_report_round_trips_through_its_mapping(aged: _Fixture) -> None:
    report: GcReport = aged.workspace.collect_garbage(dry_run=True)
    document = report.as_mapping()
    assert json.loads(json.dumps(document)) == document
    assert [category.name for category in report.categories] == list(gc_module.GC_CATEGORIES)
    assert report.category("no_such_category").candidates == 0
    assert document["candidates"] == report.candidates
