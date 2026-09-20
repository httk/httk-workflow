"""Acknowledged sources reclaim bytes and journal files without losing recovery."""

import json

import pytest

from httk.workflow import gc, transfers
from httk.workflow._util import utc_now, write_json_atomic
from httk.workflow.journal import parse_record_ref, segment_path
from test_transfer_hardening import _pair, _payload


def _segments(workspace):
    return set((workspace.control / "journal").glob("*/*.hwj"))


def _transfer(tmp_path):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    marker = source.submit(payload, "jobs")
    # Include history in another writer, not just the final transferring frame.
    with source.open_journal_writer() as writer:
        source.transition(writer, marker, "succeeded", {})
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    acknowledgement = destination.import_bundle(bundle)
    return source, destination, bundle, acknowledgement


def test_completed_transfers_leave_no_source_journal_or_retired_payload(tmp_path):
    source, destination = _pair(tmp_path)
    for index in range(12):
        payload, job_id = _payload(tmp_path / str(index))
        source.submit(payload, "jobs")
        bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
        acknowledgement = destination.import_bundle(bundle)
        assert bundle.exists() and _segments(source)  # Import alone cannot reclaim the source.
        retired = source.acknowledge_transfer(acknowledgement)
        assert not bundle.exists() and not retired.exists()
        assert source.acknowledge_transfer(acknowledgement) == retired
        assert not _segments(source)
        assert not list((source.control / "journal").iterdir())
        assert not (source.control / "transfers" / f"{acknowledgement['transfer_id']}.json").exists()
    assert not list((source.control / "transfers" / "retired").iterdir())
    assert source.check().ok and destination.check().ok


@pytest.mark.parametrize("keep", [None, "keep"])
def test_unlimited_trash_retains_bundle_and_source_history(tmp_path, keep):
    source, destination, bundle, acknowledgement = _transfer(tmp_path)
    source.set_policy({"retention": {"trash_days": keep}})
    segments = _segments(source)
    retired = source.acknowledge_transfer(acknowledgement)
    assert not bundle.exists()
    assert transfers.validate_bundle(retired)["payload_sha256"] == acknowledgement["payload_sha256"]
    assert source.acknowledge_transfer(acknowledgement) == retired
    assert _segments(source) == segments
    assert destination.import_bundle(retired) == acknowledgement


@pytest.mark.parametrize("keep", [None, "keep"])
def test_unlimited_journal_retains_history_without_retaining_payload(tmp_path, keep):
    source, _, _, acknowledgement = _transfer(tmp_path)
    source.set_policy({"retention": {"journal_days": keep}})
    segments = _segments(source)
    assert not source.acknowledge_transfer(acknowledgement).exists()
    assert _segments(source) == segments


@pytest.mark.parametrize("protection", ["current", "chain", "sealed"])
def test_retirement_protects_segments_shared_with_another_job(tmp_path, protection):
    source, destination = _pair(tmp_path)
    markers = []
    for index in range(2):
        payload, _ = _payload(tmp_path / str(index))
        markers.append(source.submit(payload, "jobs"))
    with source.open_journal_writer() as writer:
        first = source.transition(writer, markers[0], "succeeded", {})
        other = source.transition(writer, markers[1], "succeeded" if protection == "current" else "ready", {})
    shared = _segments(source)
    if protection == "chain":
        with source.open_journal_writer() as writer:
            other = source.transition(writer, other, "paused", {})
    elif protection == "sealed":
        source.detach(other.job_id, destination_workspace_id=destination.workspace_id)
    protected = _segments(source)
    bundle = source.detach(first.job_id, destination_workspace_id=destination.workspace_id)
    source.acknowledge_transfer(destination.import_bundle(bundle))
    assert shared <= _segments(source) and _segments(source) == protected
    assert source.check().ok and destination.check().ok


@pytest.mark.parametrize("opaque", [False, True])
def test_retirement_keeps_live_manager_segments(tmp_path, opaque):
    source, _, _, acknowledgement = _transfer(tmp_path)
    protected = next(iter(_segments(source)))
    manager = source.control / "managers" / "live"
    write_json_atomic(manager / "heartbeat.json", {"updated_at": utc_now()})
    write_json_atomic(manager / "manager.json", {} if opaque else {"writer_id": protected.parent.name})
    before = _segments(source)
    source.acknowledge_transfer(acknowledgement)
    assert protected.exists()
    if opaque:
        assert _segments(source) == before
    (manager / "heartbeat.json").unlink()
    source.acknowledge_transfer(acknowledgement)
    assert not _segments(source)


def test_retirement_leaves_unrelated_history_and_quarantine_untouched(tmp_path):
    source, _, _, acknowledgement = _transfer(tmp_path)
    with source.open_journal_writer() as writer:
        reference = writer.append({"unrelated": True})
    writer_id, segment, *_ = parse_record_ref(reference)
    unrelated = segment_path(source.control, writer_id, segment)
    quarantine = source.control / "quarantine" / "evidence"
    quarantine.parent.mkdir(exist_ok=True)
    quarantine.write_bytes(b"evidence")
    source.acknowledge_transfer(acknowledgement)
    assert _segments(source) == {unrelated}
    assert quarantine.read_bytes() == b"evidence"


def test_ledger_failure_after_rename_preserves_complete_bundle_and_history(tmp_path, monkeypatch):
    source, _, _, acknowledgement = _transfer(tmp_path)
    before = _segments(source)
    original = transfers.write_json_atomic

    def fail_ledger(path, value, **kwargs):
        if value.get("status") == "retired":
            raise OSError("ledger publication interrupted")
        return original(path, value, **kwargs)

    monkeypatch.setattr(transfers, "write_json_atomic", fail_ledger)
    with pytest.raises(OSError, match="ledger publication interrupted"):
        source.acknowledge_transfer(acknowledgement)
    retired = source.control / "transfers" / "retired" / acknowledgement["transfer_id"] / "bundle"
    assert transfers.validate_bundle(retired)["payload_sha256"] == acknowledgement["payload_sha256"]
    assert _segments(source) == before
    ledger = source.control / "transfers" / f"{acknowledgement['transfer_id']}.json"
    assert json.loads(ledger.read_text())["status"] == "sealed"
    monkeypatch.setattr(transfers, "write_json_atomic", original)
    source.acknowledge_transfer(acknowledgement)
    assert not retired.exists() and not _segments(source)


def test_partial_payload_cleanup_retries_only_after_retired_ledger(tmp_path, monkeypatch):
    source, destination, _, acknowledgement = _transfer(tmp_path)
    original = transfers._remove_tree

    def interrupt(path):
        ledger = source.control / "transfers" / f"{acknowledgement['transfer_id']}.json"
        assert json.loads(ledger.read_text())["status"] == "retired"
        (path / "job.json").unlink()
        raise OSError("payload cleanup interrupted")

    monkeypatch.setattr(transfers, "_remove_tree", interrupt)
    with pytest.raises(OSError, match="payload cleanup interrupted"):
        source.acknowledge_transfer(acknowledgement)
    assert source.check().ok and destination.check().ok
    monkeypatch.setattr(transfers, "_remove_tree", original)
    assert not source.acknowledge_transfer(acknowledgement).exists()
    assert not _segments(source)


def test_partial_journal_cleanup_retries_from_saved_segment_inventory(tmp_path, monkeypatch):
    source, destination, _, acknowledgement = _transfer(tmp_path)
    assert len(_segments(source)) == 2
    original = gc._Collection._collect

    def interrupt(self, category, path, **kwargs):
        result = original(self, category, path, **kwargs)
        if category == "journal_segments":
            raise OSError("journal cleanup interrupted")
        return result

    monkeypatch.setattr(gc._Collection, "_collect", interrupt)
    with pytest.raises(OSError, match="journal cleanup interrupted"):
        source.acknowledge_transfer(acknowledgement)
    assert len(_segments(source)) == 1
    assert source.check().ok and destination.check().ok
    monkeypatch.setattr(gc._Collection, "_collect", original)
    source.acknowledge_transfer(acknowledgement)
    assert not _segments(source)
    assert not list((source.control / "journal").iterdir())


def test_retry_of_legacy_retired_ledger_inventories_history_before_cleanup(tmp_path):
    source, _, _, acknowledgement = _transfer(tmp_path)
    source.set_policy({"retention": {"trash_days": "keep"}})
    retired = source.acknowledge_transfer(acknowledgement)
    ledger_path = source.control / "transfers" / f"{acknowledgement['transfer_id']}.json"
    ledger = json.loads(ledger_path.read_text())
    del ledger["retired_journal_refs"]
    write_json_atomic(ledger_path, ledger)
    source.set_policy({"retention": {"trash_days": 1.0}})
    source.acknowledge_transfer(acknowledgement)
    assert not retired.exists() and not _segments(source)
