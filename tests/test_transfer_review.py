"""Regression tests for sequence reuse and amortized transfer sweep work."""

import json
import shutil
import time
import uuid

import pytest

from httk.workflow import Workspace, WorkspaceCorruptionError, gc, transfers
from httk.workflow import _transfer_receipts as receipts
from httk.workflow.workflow_cli._transfer import _skipped_report, _transfer_local_to_local
from test_transfer_hardening import _pair, _payload


def _move(root, source, destination):
    payload, job_id = _payload(root)
    marker = source.submit(payload, 'jobs')
    bundle = source.detach(job_id, marker=marker, destination_workspace_id=destination.workspace_id)
    manifest = transfers.validate_bundle(bundle)
    source.acknowledge_transfer(destination.import_bundle(bundle))
    return job_id, manifest


@pytest.mark.parametrize('damage', ['delete', 'reset', 'rollback', 'clone'])
def test_allocator_reset_never_acknowledges_a_different_job(tmp_path, damage):
    source, destination = _pair(tmp_path)
    sink = Workspace.initialize(tmp_path / 'sink')
    first, manifest = _move(tmp_path / 'first', source, destination)
    issued = source.control / 'transfers/protocol/issued.json'
    old_bytes = issued.read_bytes()
    _transfer_local_to_local(destination, sink, [first], quiet=True)
    if damage == 'rollback':
        second, _ = _move(tmp_path / 'second', source, destination)
        _transfer_local_to_local(destination, sink, [second], quiet=True)
        issued.write_bytes(old_bytes)
    elif damage == 'delete':
        issued.unlink()
    elif damage == 'reset':
        issued.write_text('{}')
    else:
        clone = tmp_path / 'clone'
        shutil.copytree(source.root, clone)
        source = Workspace(clone)
    new_job, new_manifest = _move(tmp_path / 'new', source, destination)
    assert new_manifest['transfer_epoch'] != manifest['transfer_epoch']
    assert new_manifest['transfer_sequence'] == 1
    assert source.find_marker_by_id(new_job) is None
    arrived = destination.find_marker_by_id(new_job)
    assert arrived is not None  # The pre-fix bug silently deleted this job.
    assert destination.read_state(arrived)['transfer']['transfer_id'] == new_manifest['transfer_id']
    assert destination.find_marker_by_id(first) is None
    assert source.check().ok and destination.check().ok and sink.check().ok


@pytest.mark.parametrize('field', ['transfer_id', 'payload_sha256'])
def test_replay_checks_existing_job_provenance_before_signing(tmp_path, field):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    destination.import_bundle(bundle)
    manifest_path = bundle / transfers.TRANSFER_DIRECTORY / transfers.TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text())
    if field == 'transfer_id':
        manifest[field] = str(uuid.uuid4())
    else:
        (bundle / 'files/runner').write_text('#!/bin/sh\nexit 17\n')
        manifest[field] = transfers._payload_digest(bundle)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(WorkspaceCorruptionError, match='provenance'):
        destination.import_bundle(bundle)
    assert bundle.exists()
    assert destination.find_marker_by_id(job_id) is not None


def test_old_completion_cannot_pop_a_newer_reservation(tmp_path):
    source, destination = _pair(tmp_path)
    job_id = str(uuid.uuid4())
    old_id, new_id = str(uuid.uuid4()), str(uuid.uuid4())
    receipts.reserve(source, destination.workspace_id, job_id, old_id)
    receipts.sealed(source, destination.workspace_id, old_id)
    receipts.reserve(source, destination.workspace_id, job_id, new_id)
    receipts.sealed(source, destination.workspace_id, old_id)
    issued = json.loads((source.control / 'transfers/protocol/issued.json').read_text())
    assert new_id in issued[destination.workspace_id]['pending']


def test_absent_retirement_does_not_scan_or_create_journal_inventory(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    ack = destination.import_bundle(bundle)
    source.acknowledge_transfer(ack)
    monkeypatch.setattr(gc._Collection, 'markers', lambda self: pytest.fail('no-op scanned markers'))
    source.acknowledge_transfer(ack)
    source.recover_transfers()
    assert not (source.control / 'transfers/protocol/journal.json').exists()


def test_missing_job_reports_a_skip_instead_of_silent_success():
    identifier = str(uuid.uuid4())
    assert _skipped_report([identifier], []) == {
        'skipped': [{'job_id': identifier, 'reason': 'not found or already completed'}]
    }


@pytest.mark.timing
def test_retirement_sweep_scans_once_independent_of_unrelated_population(tmp_path, monkeypatch):
    measurements = []
    batch_size = 128
    original_markers = gc._Collection.markers
    original_ack = transfers._acknowledge_transfer
    scans = 0
    durations = []

    def markers(self):
        nonlocal scans
        scans += 1
        return original_markers(self)

    def timed_ack(*args, **kwargs):
        start = time.perf_counter()
        result = original_ack(*args, **kwargs)
        durations.append(time.perf_counter() - start)
        return result

    monkeypatch.setattr(gc._Collection, 'markers', markers)
    monkeypatch.setattr(transfers, '_acknowledge_transfer', timed_ack)
    for count in (0, 1000, 4000):
        root = tmp_path / str(count)
        source, destination = _pair(root)
        # Unrelated markers share one writer, just as a manager's jobs do.
        with source.open_journal_writer() as writer:
            for index in range(count):
                payload, _ = _payload(root / 'unrelated' / str(index))
                marker = source.submit(payload, 'unrelated')
                source.transition(writer, marker, 'succeeded', {})
        bundles = []
        for index in range(batch_size):
            payload, job_id = _payload(root / 'moving' / str(index))
            marker = source.submit(payload, 'moving')
            bundles.append(
                source.detach(
                    job_id, marker=marker, waiting_parent_map={}, destination_workspace_id=destination.workspace_id
                )
            )
        acknowledgements = transfers.import_bundles(destination, bundles)
        scans = 0
        durations.clear()
        transfers.acknowledge_transfers(source, acknowledgements)
        assert scans == 1
        mean_ms = 1000 * sum(durations) / batch_size
        warm_ms = 1000 * sorted(durations[1:])[len(durations[1:]) // 2]
        measurements.append(mean_ms)
        print(
            f'unrelated={count} batch={batch_size} scans={scans} mean_ack_ms={mean_ms:.3f} warm_median_ms={warm_ms:.3f}'
        )
        assert not list((source.control / 'transfers').glob('*.json'))
    # Include the initial index scan in amortized timing, rather than hiding it.
    assert measurements[-1] < max(measurements[0] * 6, 12)


def test_missing_allocator_with_outstanding_ledger_starts_a_new_epoch(tmp_path):
    source, destination = _pair(tmp_path)
    bundles = []
    for index in range(2):
        payload, job_id = _payload(tmp_path / str(index))
        marker = source.submit(payload, 'jobs')
        bundles.append(source.detach(job_id, marker=marker, destination_workspace_id=destination.workspace_id))
        if index == 0:
            (source.control / 'transfers/protocol/issued.json').unlink()
    manifests = [transfers.validate_bundle(bundle) for bundle in bundles]
    assert manifests[0]['transfer_epoch'] != manifests[1]['transfer_epoch']
    assert [manifest['transfer_sequence'] for manifest in manifests] == [1, 1]
    transfers.acknowledge_transfers(source, transfers.import_bundles(destination, bundles))
    assert len(list(destination.scan_markers())) == 2
    assert source.check().ok and destination.check().ok


def test_batch_retry_after_second_import_preserves_both_jobs(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    ids = []
    for index in range(2):
        payload, job_id = _payload(tmp_path / str(index))
        source.submit(payload, 'jobs')
        ids.append(job_id)
    original = receipts.remember
    calls = 0

    def interrupt(workspace, manifest):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('second receipt interrupted')
        return original(workspace, manifest)

    monkeypatch.setattr(receipts, 'remember', interrupt)
    with pytest.raises(OSError, match='second receipt interrupted'):
        _transfer_local_to_local(source, destination, ids, quiet=True)
    assert {marker.job_id for marker in destination.scan_markers()} == set(ids)
    assert len(list((source.control / 'transfers').glob('*.json'))) == 2
    monkeypatch.undo()
    _transfer_local_to_local(source, destination, ids, quiet=True)
    assert {marker.job_id for marker in destination.scan_markers()} == set(ids)
    assert not list((source.control / 'transfers').glob('*.json'))
    assert not list((source.control / 'journal').iterdir())
    assert source.check().ok and destination.check().ok


def test_retirement_batch_keeps_shared_segment_until_last_owner(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    markers = []
    with source.open_journal_writer() as writer:
        for index in range(3):
            payload, _ = _payload(tmp_path / str(index))
            marker = source.submit(payload, 'jobs')
            markers.append(source.transition(writer, marker, 'succeeded', {}))
    shared = source.control / 'journal' / writer.writer_id / '0.hwj'
    bundles = [
        source.detach(marker.job_id, marker=marker, destination_workspace_id=destination.workspace_id)
        for marker in markers
    ]
    acknowledgements = transfers.import_bundles(destination, bundles)
    original = transfers._acknowledge_transfer
    remaining = 3

    def check_owner(*args):
        nonlocal remaining
        result = original(*args)
        remaining -= 1
        assert shared.exists() == (remaining > 0)
        return result

    monkeypatch.setattr(transfers, '_acknowledge_transfer', check_owner)
    transfers.acknowledge_transfers(source, acknowledgements)
    assert not list((source.control / 'journal').iterdir())
    assert source.check().ok and destination.check().ok


@pytest.mark.parametrize('direction', ['local-local', 'local-remote', 'remote-local', 'remote-remote'])
def test_every_transfer_direction_reports_requested_ids_that_produced_no_work(tmp_path, monkeypatch, direction):
    import argparse
    from types import SimpleNamespace

    from httk.core.cli import CLIContext

    from httk.workflow.workflow_cli import _transfer as cli

    source, destination = _pair(tmp_path)
    first, last = direction.split('-')
    bindings = {
        'source': SimpleNamespace(
            remote=cli.LOCAL_REMOTE if first == 'local' else 'remote', path=source.root, name='remote:source'
        ),
        'destination': SimpleNamespace(
            remote=cli.LOCAL_REMOTE if last == 'local' else 'remote', path=destination.root, name='remote:destination'
        ),
    }
    monkeypatch.setattr(cli, 'resolve_workspace', lambda name, **kwargs: bindings[name])
    monkeypatch.setattr(cli, 'resolve_remote', lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(cli, '_remote_workspace_settings', lambda *args, **kwargs: {})
    monkeypatch.setattr(cli, '_send_jobs_to_remote', lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, '_fetch_jobs_from_remote', lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(cli, '_transfer_remote_to_remote', lambda *args, **kwargs: ([], []))
    identifier = str(uuid.uuid4())
    arguments = argparse.Namespace(
        source='source',
        destination='destination',
        jobs=[identifier],
        adapter_timeout=None,
        strict_environment=False,
        destination_placement=None,
        state=None,
        placement=None,
    )
    report = cli.run_transfer_verb_result(arguments, CLIContext('httk', tmp_path), quiet=True)
    assert report['moved'] == []
    assert report['skipped'] == [{'job_id': identifier, 'reason': 'not found or already completed'}]
