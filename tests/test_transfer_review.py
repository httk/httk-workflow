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


@pytest.mark.parametrize('damage', ['delete', 'reset', 'allocator_rollback', 'rollback', 'clone'])
@pytest.mark.parametrize('moved_on', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_allocator_reset_never_acknowledges_a_different_job(tmp_path, monkeypatch, damage, moved_on, restart):
    source, destination = _pair(tmp_path)
    sink = Workspace.initialize(tmp_path / 'sink')
    first, manifest = _move(tmp_path / 'first', source, destination)
    issued = source.control / 'transfers/protocol/issued.json'
    checkpoint = issued.with_name('issued-checkpoint.json')
    old_bytes = issued.read_bytes()
    old_checkpoint = checkpoint.read_bytes()
    control_inode = source.control.stat().st_ino
    if moved_on:
        _transfer_local_to_local(destination, sink, [first], quiet=True)
    if damage in {'rollback', 'allocator_rollback'}:
        second, _ = _move(tmp_path / 'second', source, destination)
        if moved_on:
            _transfer_local_to_local(destination, sink, [second], quiet=True)
        allocator_inodes = (issued.stat().st_ino, checkpoint.stat().st_ino)
        issued.write_bytes(old_bytes)
        if damage == 'rollback':
            checkpoint.write_bytes(old_checkpoint)
            assert (issued.stat().st_ino, checkpoint.stat().st_ino) == allocator_inodes
            assert source.control.stat().st_ino == control_inode
    elif damage == 'delete':
        issued.unlink()
    elif damage == 'reset':
        issued.write_text('{}')
    else:
        clone = tmp_path / 'clone'
        shutil.copytree(source.root, clone)
        source = Workspace(clone)
    if restart:
        monkeypatch.setattr(receipts, '_session_streams', {})
    new_job, new_manifest = _move(tmp_path / 'new', source, destination)
    assert new_manifest['transfer_epoch'] != manifest['transfer_epoch']
    assert new_manifest['transfer_sequence'] == 1
    assert source.find_marker_by_id(new_job) is None
    arrived = destination.find_marker_by_id(new_job)
    assert arrived is not None  # The pre-fix bug silently deleted this job.
    assert destination.read_state(arrived)['transfer']['transfer_id'] == new_manifest['transfer_id']
    assert (destination.find_marker_by_id(first) is None) == moved_on
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


@pytest.mark.parametrize('boundary', ['sealed', 'imported', 'moved_on'])
def test_sealed_transfer_resumes_across_process_restart(tmp_path, monkeypatch, boundary):
    source, destination = _pair(tmp_path)
    sink = Workspace.initialize(tmp_path / 'sink')
    payload, job_id = _payload(tmp_path / 'old')
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = transfers.validate_bundle(bundle)
    saved = tmp_path / 'delayed'
    shutil.copytree(bundle, saved)
    if boundary != 'sealed':
        destination.import_bundle(bundle)
    if boundary == 'moved_on':
        _transfer_local_to_local(destination, sink, [job_id], quiet=True)
    monkeypatch.setattr(receipts, '_session_streams', {})
    # Recovery/offer must reuse the ledger, even if new allocations have begun
    # in the new process. It must not reallocate this sealed transfer's epoch.
    _, new_manifest = _move(tmp_path / 'new', source, destination)
    assert new_manifest['transfer_epoch'] != manifest['transfer_epoch']
    offers = transfers.offer_transfers(
        source, destination_workspace_id=destination.workspace_id, states=('submitted',), job_ids=[job_id]
    )
    assert len(offers) == 1
    assert transfers.validate_bundle(offers[0]['bundle_path']) == manifest
    _transfer_local_to_local(source, destination, [job_id], quiet=True)
    ack = destination.import_bundle(saved)
    source.acknowledge_transfer(ack)
    assert ack['transfer_epoch'] == manifest['transfer_epoch']
    assert ack['transfer_sequence'] == manifest['transfer_sequence']
    assert source.find_marker_by_id(job_id) is None
    assert (destination.find_marker_by_id(job_id) is None) == (boundary == 'moved_on')
    assert sum(m.job_id == job_id for ws in (source, destination, sink) for m in ws.scan_markers()) == 1
    from test_transfer_residuals import _no_transfer_files

    for workspace in (source, destination, sink):
        _no_transfer_files(workspace)
        assert workspace.check().ok
    assert not list((source.control / 'journal').iterdir())


def test_pending_unfenced_reservation_is_reissued_after_restart(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    original = source.transition

    def interrupt(*args, **kwargs):
        raise OSError('before fence')

    monkeypatch.setattr(source, 'transition', interrupt)
    with pytest.raises(OSError, match='before fence'):
        source.detach(job_id, destination_workspace_id=destination.workspace_id)
    issued = json.loads((source.control / 'transfers/protocol/issued.json').read_text())[destination.workspace_id]
    monkeypatch.setattr(receipts, '_session_streams', {})
    monkeypatch.setattr(source, 'transition', original)
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = transfers.validate_bundle(bundle)
    assert manifest['transfer_epoch'] != issued['epoch']
    assert manifest['transfer_id'] not in issued['pending']
    assert manifest['transfer_sequence'] == 1
    source.acknowledge_transfer(destination.import_bundle(bundle))
    assert len(list(destination.scan_markers())) == 1
    assert source.check().ok and destination.check().ok
    assert not list((source.control / 'transfers').glob('*.json'))
    assert not list((source.control / 'journal').iterdir())


def test_fenced_unsealed_transfer_keeps_epoch_across_restart(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path / 'old')
    source.submit(payload, 'jobs')
    original_seal = transfers._seal_transferring

    def interrupt(*args, **kwargs):
        raise OSError('after fence')

    monkeypatch.setattr(transfers, '_seal_transferring', interrupt)
    with pytest.raises(OSError, match='after fence'):
        source.detach(job_id, destination_workspace_id=destination.workspace_id)
    fenced = source.read_state(next(source.scan_markers(('transferring',))))
    monkeypatch.setattr(receipts, '_session_streams', {})
    monkeypatch.setattr(transfers, '_seal_transferring', original_seal)
    source.recover_transfers()
    offers = transfers.offer_transfers(
        source, destination_workspace_id=destination.workspace_id, states=('submitted',), job_ids=[job_id]
    )
    manifest = transfers.validate_bundle(offers[0]['bundle_path'])
    for field in ('transfer_id', 'transfer_epoch', 'transfer_sequence'):
        assert manifest[field] == fenced[field]
    _transfer_local_to_local(source, destination, [job_id], quiet=True)
    _, fresh = _move(tmp_path / 'new', source, destination)
    assert fresh['transfer_epoch'] != manifest['transfer_epoch']
    assert fresh['transfer_sequence'] == 1
    assert len(list(destination.scan_markers())) == 2
    assert source.check().ok and destination.check().ok


def test_fork_cannot_inherit_allocation_epoch(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    _, old = _move(tmp_path / 'old', source, destination)
    process_id = receipts.os.getpid()
    # Simulate fork: the allocator memory is inherited intact, but PID changes.
    monkeypatch.setattr(receipts.os, 'getpid', lambda: process_id + 1)
    _, new = _move(tmp_path / 'new', source, destination)
    assert new['transfer_epoch'] != old['transfer_epoch']
    assert new['transfer_sequence'] == 1


def test_interleaved_process_sessions_coalesce_their_own_ranges(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    process_id = receipts.os.getpid()
    manifests = [[], []]
    for index in range(6):
        session = index % 2
        monkeypatch.setattr(receipts.os, 'getpid', lambda session=session: process_id + session)
        _, manifest = _move(tmp_path / str(index), source, destination)
        manifests[session].append(manifest)
    assert manifests[0][0]['transfer_epoch'] != manifests[1][0]['transfer_epoch']
    for values in manifests:
        assert len({value['transfer_epoch'] for value in values}) == 1
        assert [value['transfer_sequence'] for value in values] == [1, 2, 3]
    received = json.loads((destination.control / 'transfers/protocol/received.json').read_text())
    assert len(received) == 2
    assert list(received.values()) == [[[1, 3]], [[1, 3]]]


@pytest.mark.parametrize('restore_pending', [False, True])
def test_other_process_completion_cannot_reuse_cached_reservation_for_returned_job(
    tmp_path, monkeypatch, restore_pending
):
    source, destination = _pair(tmp_path)
    process_id = receipts.os.getpid()
    payload, job_id = _payload(tmp_path / 'returning')
    source.submit(payload, 'jobs')
    original_seal = transfers._seal_transferring

    def interrupt(*args, **kwargs):
        raise OSError('after fence')

    monkeypatch.setattr(transfers, '_seal_transferring', interrupt)
    with pytest.raises(OSError, match='after fence'):
        source.detach(job_id, destination_workspace_id=destination.workspace_id)
    old = source.read_state(next(source.scan_markers(('transferring',))))
    allocator = source.control / 'transfers/protocol/issued.json'
    checkpoint = allocator.with_name('issued-checkpoint.json')
    old_files = (allocator.read_bytes(), checkpoint.read_bytes())
    monkeypatch.setattr(transfers, '_seal_transferring', original_seal)
    monkeypatch.setattr(receipts.os, 'getpid', lambda: process_id + 1)
    # This second process finishes the transfer, overwrites issued.json with
    # its own stream, and brings the original job back home.
    _transfer_local_to_local(source, destination, [job_id], quiet=True)
    _move(tmp_path / 'other', source, destination)
    _transfer_local_to_local(destination, source, [job_id], quiet=True)
    if restore_pending:
        allocator.write_bytes(old_files[0])
        checkpoint.write_bytes(old_files[1])
    monkeypatch.setattr(receipts.os, 'getpid', lambda: process_id)
    result = _transfer_local_to_local(source, destination, [job_id], quiet=True)
    assert len(result) == 1
    assert result[0]['transfer_id'] != old['transfer_id']
    assert (result[0]['transfer_epoch'], result[0]['transfer_sequence']) != (
        old['transfer_epoch'],
        old['transfer_sequence'],
    )
    assert destination.find_marker_by_id(job_id) is not None
    assert source.find_marker_by_id(job_id) is None
    assert source.check().ok and destination.check().ok


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


@pytest.mark.timing
def test_retirement_sweep_does_not_read_terminal_history(tmp_path, monkeypatch):
    original_chain = gc.iter_record_chain
    frames = 0

    def counted_chain(*args, **kwargs):
        nonlocal frames
        for item in original_chain(*args, **kwargs):
            frames += 1
            yield item

    monkeypatch.setattr(gc, 'iter_record_chain', counted_chain)
    measurements = []
    batch_size = 32
    for count, depth in ((4, 1), (4000, 1), (4000, 6)):
        root = tmp_path / f'{count}-{depth}'
        source, destination = _pair(root)
        with source.open_journal_writer() as writer:
            for index in range(count):
                payload, _ = _payload(root / 'unrelated' / str(index))
                marker = source.submit(payload, 'unrelated')
                for _ in range(depth - 1):
                    marker = source.transition(writer, marker, 'succeeded', {})
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
        frames = 0
        start = time.perf_counter()
        transfers.acknowledge_transfers(source, acknowledgements)
        mean_ms = 1000 * (time.perf_counter() - start) / batch_size
        print(f'terminal={count} depth={depth} batch={batch_size} frames={frames} mean_ack_ms={mean_ms:.3f}')
        measurements.append(frames)
        assert source.check().ok and destination.check().ok
    # Deterministic complexity assertion: includes sealed-transfer protection,
    # but no frame reads for unrelated terminal heads or their histories.
    assert measurements[0] == measurements[1] == measurements[2]


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


def test_terminal_history_does_not_pin_retired_segment(tmp_path):
    source, destination = _pair(tmp_path)
    markers = []
    with source.open_journal_writer() as shared_writer:
        for index in range(2):
            payload, _ = _payload(tmp_path / str(index))
            markers.append(source.transition(shared_writer, source.submit(payload, 'jobs'), 'succeeded', {}))
    with source.open_journal_writer() as head_writer:
        terminal = source.transition(head_writer, markers[0], 'succeeded', {})
    shared = source.control / 'journal' / shared_writer.writer_id / '0.hwj'
    head = source.control / 'journal' / head_writer.writer_id / '0.hwj'
    bundle = source.detach(markers[1].job_id, marker=markers[1], destination_workspace_id=destination.workspace_id)
    assert shared.exists() and head.exists()
    transfers.acknowledge_transfers(source, [destination.import_bundle(bundle)])
    assert not shared.exists()
    assert head.exists() and source.find_marker_by_id(terminal.job_id) is not None
    assert not (source.control / 'transfers/protocol/journal.json').exists()
    assert source.check().ok and destination.check().ok


def test_legacy_replay_without_live_job_explains_manual_retirement(tmp_path):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest_path = bundle / transfers.TRANSFER_DIRECTORY / transfers.TRANSFER_MANIFEST
    manifest = json.loads(manifest_path.read_text())
    del manifest['transfer_epoch']
    manifest_path.write_text(json.dumps(manifest))
    receipts.remember(destination, manifest)
    with pytest.raises(WorkspaceCorruptionError, match=rf'httk workflow transfer retire \. {job_id}'):
        destination.import_bundle(bundle)
    assert bundle.exists()
    assert destination.find_marker_by_id(job_id) is None


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
    monkeypatch.setattr(cli, 'resolve_remote', lambda *args, **kwargs: SimpleNamespace(name='remote', bundle='adapter'))
    monkeypatch.setattr(cli, '_remote_workspace_settings', lambda *args, **kwargs: {})
    workspaces = {'source': source, 'destination': destination}
    monkeypatch.setattr(
        cli,
        '_remote_workspace_probe',
        lambda target, name, **kwargs: (workspaces[name].workspace_id, str(workspaces[name].root)),
    )
    monkeypatch.setattr(cli, '_protocol_workspace', lambda name, context: workspaces[name])
    offered = []

    def adapter(bundle, action, parameters, **kwargs):
        # Run the real remote offer handler and missing-ID selection. Only the
        # transport is substituted; any attempted payload operation is a bug.
        import contextlib
        import io

        assert action == 'invoke'
        argv = parameters['argv']
        assert argv[: len(cli.REMOTE_OFFER_COMMAND)] == list(cli.REMOTE_OFFER_COMMAND)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = cli._dispatch_transfer_protocol(
                ['offer', *argv[len(cli.REMOTE_OFFER_COMMAND) :]], CLIContext('httk', source.root)
            )
        offered.append(json.loads(output.getvalue()))
        return {'returncode': result, 'stdout': output.getvalue(), 'stderr': ''}

    monkeypatch.setattr(cli, 'run_adapter', adapter)
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
    if first == 'remote':
        assert len(offered) == 1
        assert offered[0]['offers'] == []
        assert offered[0]['skipped'] == report['skipped']
    assert list(source.scan_markers()) == list(destination.scan_markers()) == []
    assert not list((source.control / 'transfers').glob('*.json'))
