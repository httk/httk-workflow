"""The workspace-daemon exchange pass: staged adoption, auto-ejection, records and status."""

import json
import os
import shutil
import time
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace, _exchange_staging, transfers, workflow_cli
from httk.workflow._exchange_staging import exchange_pass
from httk.workflow._job_tree import record_spawns
from httk.workflow._util import timestamp_seconds
from httk.workflow.models import Marker, make_job_key
from httk.workflow.transfers import exchange_staging
from httk.workflow.workflow_cli import _manager
from test_eject_adopt import _pair, _payload

#: A pass time well beyond the eject grace period of jobs finished in a test.
_LATER = time.time() + 3600.0


def _finish(workspace: Workspace, marker: Marker, kind: str = "succeeded") -> Marker:
    with workspace.open_journal_writer() as writer:
        return workspace.transition(writer, marker, kind, {"reason": "test"})


def _staging(workspace: Workspace) -> Path:
    staging = exchange_staging(workspace)
    for name in ("inbox", "outbox/rejected", "records"):
        (staging / name).mkdir(parents=True, exist_ok=True)
    return staging


def _stage(source: Workspace, workspace: Workspace, tmp_path: Path, tag: str) -> tuple[Marker, Path]:
    """Eject a fresh job of *source* and stage it in *workspace*'s inbox, as the broker would."""

    marker = source.submit(_payload(tmp_path / "payloads", tag), "jobs")
    loose = source.eject(marker.job_id, tmp_path / f"loose-{tag}")
    staged = _staging(workspace) / "inbox" / marker.job_key
    os.rename(loose, staged)
    return marker, staged


def _status(workspace: Workspace) -> dict[str, object]:
    return json.loads((exchange_staging(workspace) / "outbox" / "status.json").read_text(encoding="utf-8"))


def test_a_staged_bundle_is_adopted_by_one_pass(tmp_path: Path) -> None:
    source, workspace = _pair(tmp_path)
    marker, staged = _stage(source, workspace, tmp_path, "a")
    exchange_pass(workspace, now=1000.0)
    adopted = workspace.find_marker_by_id(marker.job_id)
    assert adopted is not None and adopted.kind == marker.kind
    assert not staged.exists() and not os.listdir(staged.parent)
    # A submitted job is not finished, so it stays to be run.
    assert sorted(os.listdir(staged.parent.parent / "outbox")) == ["rejected", "status.json"]
    assert _status(workspace)["jobs"] == [{"job_id": marker.job_id, "job_key": marker.job_key, "state": "submitted"}]


def test_a_corrupt_bundle_is_rejected_with_a_record_until_its_name_adopts(tmp_path: Path) -> None:
    source, workspace = _pair(tmp_path)
    staging = _staging(workspace)
    (staging / "inbox" / "broken").mkdir()
    (staging / "inbox" / "broken" / "junk").write_text("not a job", encoding="utf-8")
    exchange_pass(workspace, now=1000.0)
    assert (staging / "outbox" / "rejected" / "broken" / "junk").is_file()
    assert not (staging / "inbox" / "broken").exists()
    record = json.loads((staging / "records" / "rejected-broken.json").read_text(encoding="utf-8"))
    assert set(record) == {"name", "reason", "at"} and record["name"] == "broken" and record["reason"]
    assert _status(workspace)["rejected"] == [{"name": "broken", "reason": record["reason"]}]

    # The client fixes it and sends a good job under the same name.
    marker = source.submit(_payload(tmp_path / "payloads", "fixed"), "jobs")
    os.rename(source.eject(marker.job_id, tmp_path / "fixed"), staging / "inbox" / "broken")
    exchange_pass(workspace, now=1001.0)
    assert workspace.find_marker_by_id(marker.job_id) is not None
    assert not (staging / "records" / "rejected-broken.json").exists()


def test_a_rejection_whose_name_is_taken_stays_in_the_inbox(tmp_path: Path) -> None:
    _source, workspace = _pair(tmp_path)
    staging = _staging(workspace)
    (staging / "inbox" / "broken").mkdir()
    (staging / "outbox" / "rejected" / "broken").mkdir()
    exchange_pass(workspace, now=1000.0)
    assert (staging / "inbox" / "broken").is_dir()
    record = staging / "records" / "rejected-broken.json"
    written = record.read_bytes()
    # An unchanged refusal is not written again on every pass.
    exchange_pass(workspace, now=1001.0)
    assert (staging / "inbox" / "broken").is_dir() and record.read_bytes() == written


def test_a_staged_non_directory_is_removed_and_never_pins_its_name(tmp_path: Path) -> None:
    _source, workspace = _pair(tmp_path)
    staging = _staging(workspace)
    inbox, rejected = staging / "inbox", staging / "outbox" / "rejected"
    (inbox / "job").symlink_to(tmp_path)
    exchange_pass(workspace, now=1000.0)
    assert not os.path.lexists(inbox / "job") and not os.path.lexists(rejected / "job")
    assert tmp_path.is_dir()  # the link is removed, not what it points at
    (inbox / "job").write_text("not a job", encoding="utf-8")
    exchange_pass(workspace, now=1001.0)
    assert not os.path.lexists(inbox / "job") and not os.path.lexists(rejected / "job")
    record = json.loads((staging / "records" / "rejected-job.json").read_text(encoding="utf-8"))
    assert record["name"] == "job" and "not a job directory" in record["reason"]
    # A refused directory of the same name still goes to rejected/.
    (inbox / "job").mkdir()
    exchange_pass(workspace, now=1002.0)
    assert (rejected / "job").is_dir() and not os.path.lexists(inbox / "job")


def test_a_vanished_entry_is_skipped_silently(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, workspace = _pair(tmp_path)
    marker, staged = _stage(source, workspace, tmp_path, "a")
    taken = tmp_path / "taken"

    def adopt(directory: Path) -> None:
        os.rename(directory, taken)  # another manager took it first
        raise FileNotFoundError(directory)

    monkeypatch.setattr(workspace, "adopt", adopt)
    exchange_pass(workspace, now=1000.0)
    staging = exchange_staging(workspace)
    assert taken.is_dir() and not staged.exists()
    assert not os.listdir(staging / "outbox" / "rejected") and not os.listdir(staging / "records")
    assert workspace.find_marker_by_id(marker.job_id) is None


def test_reserved_dot_and_ill_formed_names_are_skipped(tmp_path: Path) -> None:
    _source, workspace = _pair(tmp_path)
    staging = _staging(workspace)
    names = [".partial", "status.json", "rejected", "records", "outbox", "bad name", "-dash"]
    for name in names:
        (staging / "inbox" / name).mkdir()
    exchange_pass(workspace, now=1000.0)
    assert sorted(os.listdir(staging / "inbox")) == sorted(names)
    assert not os.listdir(staging / "outbox" / "rejected") and not os.listdir(staging / "records")


def test_a_finished_job_is_ejected_and_an_unfinished_one_is_not(tmp_path: Path) -> None:
    workspace, _other = _pair(tmp_path)
    done = _finish(workspace, workspace.submit(_payload(tmp_path / "payloads", "done"), "jobs"), "failed")
    waiting = workspace.submit(_payload(tmp_path / "payloads", "todo"), "jobs")
    exchange_pass(workspace, now=_LATER)
    outbox = exchange_staging(workspace) / "outbox"
    assert transfers.validate_bundle(outbox / done.job_key)["job_id"] == done.job_id
    assert workspace.find_marker_by_id(done.job_id) is None
    assert not workspace.payload_path(done.placement, done.job_key).exists()
    current = workspace.find_marker_by_id(waiting.job_id)
    assert current is not None and current.kind == "submitted"
    assert not (outbox / waiting.job_key).exists()


def test_a_just_finished_job_waits_out_the_grace_period(tmp_path: Path) -> None:
    workspace, _other = _pair(tmp_path)
    done = _finish(workspace, workspace.submit(_payload(tmp_path / "payloads", "done"), "jobs"))
    finished = timestamp_seconds(str(workspace.read_state(done)["created_at"]))
    exchange_pass(workspace, now=finished + 1)
    assert workspace.find_marker_by_id(done.job_id) is not None
    assert not os.listdir(exchange_staging(workspace) / "records")
    exchange_pass(workspace, now=finished + 61)
    assert workspace.find_marker_by_id(done.job_id) is None
    assert (exchange_staging(workspace) / "outbox" / done.job_key).is_dir()


def test_a_finished_parent_with_a_live_bound_child_is_pending(tmp_path: Path) -> None:
    workspace, _other = _pair(tmp_path)
    root = workspace.submit(_payload(tmp_path / "payloads", "root"), "jobs")
    child_payload = _payload(tmp_path / "payloads", "kid")
    definition = json.loads((child_payload / "job.json").read_text(encoding="utf-8"))
    definition["parent"] = {"job_id": root.job_id, "job_key": root.job_key, "placement": "jobs", "spawn_id": None}
    (child_payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")
    record_spawns(
        workspace.payload_path(root.placement, root.job_key),
        str(uuid.uuid4()),
        [{"job_key": make_job_key(definition["id"], "kid"), "label": "kid", "placement": "jobs/kids"}],
        durable=False,
    )
    child = workspace.submit(child_payload, "jobs/kids")
    root = _finish(workspace, root)
    exchange_pass(workspace, now=_LATER)
    staging = exchange_staging(workspace)
    assert workspace.find_marker_by_id(root.job_id) is not None
    assert workspace.find_marker_by_id(child.job_id) is not None
    assert not os.listdir(staging / "records")
    assert _status(workspace)["eject_errors"] == []
    # Once the child ends too, the whole tree leaves as one directory, the child inside its root.
    _finish(workspace, child, "cancelled")
    exchange_pass(workspace, now=_LATER + 10)
    assert workspace.find_marker_by_id(root.job_id) is None and workspace.find_marker_by_id(child.job_id) is None
    assert sorted(os.listdir(staging / "outbox")) == sorted(["rejected", "status.json", root.job_key])


def test_an_occupied_outbox_name_is_retried_silently(tmp_path: Path) -> None:
    workspace, _other = _pair(tmp_path)
    done = _finish(workspace, workspace.submit(_payload(tmp_path / "payloads", "done"), "jobs"))
    staging = _staging(workspace)
    (staging / "outbox" / done.job_key).mkdir()  # the broker has not yet moved the previous copy
    exchange_pass(workspace, now=_LATER)
    assert workspace.find_marker_by_id(done.job_id) is not None
    assert not os.listdir(staging / "records")
    (staging / "outbox" / done.job_key).rmdir()
    exchange_pass(workspace, now=_LATER + 10)
    assert workspace.find_marker_by_id(done.job_id) is None
    assert transfers.validate_bundle(staging / "outbox" / done.job_key)["job_id"] == done.job_id


def test_an_eject_error_is_recorded_until_the_eject_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _other = _pair(tmp_path)
    done = _finish(workspace, workspace.submit(_payload(tmp_path / "payloads", "done"), "jobs"))
    staging = _staging(workspace)

    def broken(_job_id: str, _target: Path) -> None:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(workspace, "eject", broken)
    exchange_pass(workspace, now=_LATER)
    record = json.loads((staging / "records" / f"eject-{done.job_id}.json").read_text(encoding="utf-8"))
    assert set(record) == {"job_id", "reason", "at"} and record["reason"] == "RuntimeError: disk on fire"
    assert _status(workspace)["eject_errors"] == [{"job_id": done.job_id, "reason": "RuntimeError: disk on fire"}]
    monkeypatch.undo()
    exchange_pass(workspace, now=_LATER + 10)
    assert workspace.find_marker_by_id(done.job_id) is None
    assert not (staging / "records" / f"eject-{done.job_id}.json").exists()


def test_the_status_document_is_sorted_rate_limited_and_atomic(tmp_path: Path) -> None:
    workspace, _other = _pair(tmp_path)
    markers = [workspace.submit(_payload(tmp_path / "payloads", f"j{index}"), "jobs") for index in range(3)]
    exchange_pass(workspace, now=1000.0)
    status = _status(workspace)
    assert set(status) == {
        "format",
        "format_version",
        "workspace_id",
        "generated_at",
        "jobs",
        "rejected",
        "eject_errors",
        "truncated",
    }
    assert (status["format"], status["format_version"]) == ("httk-workspace-daemon-status", 1)
    assert status["workspace_id"] == workspace.workspace_id
    assert status["generated_at"] == "1970-01-01T00:16:40.000000Z"
    assert status["jobs"] == [
        {"job_id": marker.job_id, "job_key": marker.job_key, "state": "submitted"}
        for marker in sorted(markers, key=lambda item: item.job_key)
    ]
    assert (status["rejected"], status["eject_errors"], status["truncated"]) == ([], [], False)
    outbox = exchange_staging(workspace) / "outbox"
    assert sorted(os.listdir(outbox)) == ["rejected", "status.json"]  # no leftover temporary
    (outbox / "status.json").unlink()
    exchange_pass(workspace, now=1009.0)
    assert not (outbox / "status.json").exists()
    exchange_pass(workspace, now=1010.0)
    assert _status(workspace)["generated_at"] == "1970-01-01T00:16:50.000000Z"


def test_the_status_document_is_truncated_to_its_bound(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, _other = _pair(tmp_path)
    markers = sorted(
        (workspace.submit(_payload(tmp_path / "payloads", f"j{index}"), "jobs") for index in range(4)),
        key=lambda item: item.job_key,
    )
    monkeypatch.setattr(_exchange_staging, "_STATUS_LIMIT", 600)
    exchange_pass(workspace, now=1000.0)
    path = exchange_staging(workspace) / "outbox" / "status.json"
    status = json.loads(path.read_text(encoding="utf-8"))
    assert status["truncated"] is True and path.stat().st_size <= 600
    assert 0 < len(status["jobs"]) < 4
    assert [job["job_id"] for job in status["jobs"]] == [marker.job_id for marker in markers[: len(status["jobs"])]]


def test_garbage_records_are_ignored(tmp_path: Path) -> None:
    _source, workspace = _pair(tmp_path)
    records = _staging(workspace) / "records"
    (records / "rejected-text.json").write_text("not json", encoding="utf-8")
    (records / "rejected-list.json").write_text("[1, 2]", encoding="utf-8")
    (records / "rejected-deep.json").write_text("[" * 10000 + "]" * 10000, encoding="utf-8")
    (records / "rejected-typed.json").write_text(json.dumps({"name": 1, "reason": "x"}), encoding="utf-8")
    (records / "rejected-huge.json").write_text(json.dumps({"name": "h", "reason": "x" * 20000}), encoding="utf-8")
    os.mkfifo(records / "rejected-fifo.json")
    (records / "rejected-link.json").symlink_to(records / "rejected-good.json")
    (records / "rejected-good.json").write_text(json.dumps({"name": "good", "reason": "y" * 1500}), encoding="utf-8")
    exchange_pass(workspace, now=1000.0)
    assert _status(workspace)["rejected"] == [{"name": "good", "reason": "y" * 1000}]


def test_recovery_runs_even_when_nothing_is_eligible(tmp_path: Path) -> None:
    workspace, _other = _pair(tmp_path)
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"
    # Fence and seal exactly as eject does, then stop before the move.
    transfers._detach_job(
        workspace,
        marker.job_id,
        marker=marker,
        destination_workspace_id=None,
        transfer_id=str(uuid.uuid4()),
        eject_to=target,
    )
    exchange_pass(workspace, now=1000.0)
    assert transfers.validate_bundle(target)["job_id"] == marker.job_id


def test_an_auto_eject_interrupted_before_retirement_is_settled_by_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _other = _pair(tmp_path)
    done = _finish(workspace, workspace.submit(_payload(tmp_path / "payloads", "done"), "jobs"))
    staging = _staging(workspace)
    retire = transfers._retire_sealed_bundle

    def crash(*args: object, **kwargs: object) -> Path:
        monkeypatch.setattr(transfers, "_retire_sealed_bundle", retire)
        raise RuntimeError("killed before the ledger retire")

    monkeypatch.setattr(transfers, "_retire_sealed_bundle", crash)
    exchange_pass(workspace, now=_LATER)
    bundle = staging / "outbox" / done.job_key
    assert transfers.validate_bundle(bundle)["job_id"] == done.job_id
    # Sealed and moved out, but its ledger is not yet retired.
    assert [ledger["job_id"] for ledger in transfers._ledgers(workspace) if ledger.get("status") == "sealed"] == [
        done.job_id
    ]
    assert workspace.find_marker_by_id(done.job_id) is None
    # The broker moves the bundle on before any recovery runs.
    delivered = tmp_path / "exchange-outbox" / done.job_key
    delivered.parent.mkdir()
    os.rename(bundle, delivered)
    exchange_pass(workspace, now=_LATER + 10)
    assert not list(workspace.scan_markers(("transferring",))) and workspace.find_marker_by_id(done.job_id) is None
    assert all(ledger.get("status") == "retired" for ledger in transfers._ledgers(workspace))
    assert not bundle.exists() and transfers.validate_bundle(delivered)["job_id"] == done.job_id
    assert not os.listdir(staging / "records")
    assert _status(workspace)["jobs"] == [] and _status(workspace)["eject_errors"] == []


def test_a_tampered_staging_directory_never_raises(tmp_path: Path) -> None:
    _source, workspace = _pair(tmp_path)
    staging = exchange_staging(workspace)
    staging.mkdir(parents=True)
    os.mkfifo(staging / "inbox")
    (staging / "outbox").write_text("not a directory", encoding="utf-8")
    exchange_pass(workspace, now=1000.0)
    shutil.rmtree(staging)
    exchange_pass(workspace, now=1010.0)  # the directories are recreated
    assert (staging / "outbox" / "status.json").is_file()


def test_a_manager_with_exchange_adopts_runs_and_ejects_a_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, workspace = _pair(tmp_path)
    monkeypatch.setattr(_exchange_staging, "_EJECT_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(_exchange_staging, "_STEP_INTERVAL", 0.0)
    marker, _staged = _stage(source, workspace, tmp_path, "a")
    with TaskManager(workspace, heartbeat_interval=0.01, exchange=True) as manager:
        manager.run_until_idle(timeout=120.0)
        manager.tick()
    bundle = exchange_staging(workspace) / "outbox" / marker.job_key
    manifest = transfers.validate_bundle(bundle)
    assert (manifest["job_id"], manifest["prior_kind"]) == (marker.job_id, "succeeded")
    assert workspace.find_marker_by_id(marker.job_id) is None


def test_the_exchange_flag_is_parsed_and_forwarded(tmp_path: Path) -> None:
    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    arguments = parser.parse_args(["manager", "run", "--exchange"])
    assert arguments.exchange is True
    assert "--exchange" in _manager.manager_argv_tail(arguments)
    assert "--exchange" not in _manager.manager_argv_tail(parser.parse_args(["manager", "run"]))
