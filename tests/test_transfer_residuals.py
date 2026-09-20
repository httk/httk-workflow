"""Bounded transfer metadata and crash/replay tests, without age-gated GC."""

import json
import shutil
from pathlib import Path

import pytest

from httk.workflow import _transfer_receipts as receipts
from httk.workflow import transfers
from httk.workflow.workflow_cli._transfer import _transfer_local_to_local
from test_transfer_hardening import _pair, _payload


def _metadata(workspace):
    return sorted(str(path.relative_to(workspace.control)) for path in workspace.control.rglob('*') if path.is_file())


def _no_transfer_files(workspace):
    for pattern in ('acks/*.json', 'imported/*.json', '*.json', 'incoming/**/*', 'retired/**/*'):
        assert not [path for path in (workspace.control / 'transfers').glob(pattern) if path.is_file()]


def _roundtrip(root, count):
    source, destination = _pair(root)
    job_ids = []
    for index in range(count):
        payload, job_id = _payload(root / str(index))
        source.submit(payload, 'jobs')
        _transfer_local_to_local(source, destination, [job_id], quiet=True)
        job_ids.append(job_id)
    # A shared writer models the manager's terminal transitions and verifies
    # collection of a segment protected by other jobs until the last return.
    with destination.open_journal_writer() as writer:
        for marker in list(destination.scan_markers()):
            destination.transition(writer, marker, 'succeeded', {})
    for job_id in job_ids:
        _transfer_local_to_local(destination, source, [job_id], quiet=True)
    for workspace in (source, destination):
        _no_transfer_files(workspace)
        assert workspace.check().ok
    assert not list((destination.control / 'journal').iterdir())
    # The home workspace necessarily holds N live job markers and journal
    # states. Compare transfer bookkeeping there; compare ALL HPC metadata.
    source_protocol = [name for name in _metadata(source) if name.startswith('transfers/')]
    return source_protocol, _metadata(destination)


def test_five_and_fifty_roundtrips_have_identical_residual_files(tmp_path):
    small = _roundtrip(tmp_path / 'five', 5)
    large = _roundtrip(tmp_path / 'fifty', 50)
    assert small == large


@pytest.mark.parametrize('boundary', ['import', 'ack', 'receipt', 'partial_prune', 'retire', 'ledger_prune'])
def test_interrupted_roundtrip_is_exactly_once_and_reclaims_files(tmp_path, monkeypatch, boundary):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    saved = tmp_path / 'delayed-bundle'
    shutil.copytree(bundle, saved)
    real_write = transfers.write_json_atomic
    real_remember = receipts.remember
    real_prune = transfers._prune_import_receipts
    real_reclaim = transfers._reclaim_retired_transfer
    real_unlink = Path.unlink
    triggered = False

    def interrupt():
        nonlocal triggered
        if not triggered:
            triggered = True
            raise OSError('injected interruption')

    def write(path, value, **kwargs):
        if boundary == 'import' and path.parent.name == 'acks':
            interrupt()
        result = real_write(path, value, **kwargs)
        if boundary == 'ack' and path.parent.name == 'acks':
            interrupt()
        return result

    def remember(workspace, manifest):
        real_remember(workspace, manifest)
        if boundary == 'receipt':
            interrupt()

    def prune(workspace, identifier):
        if boundary == 'partial_prune':
            transfers._ack_path(workspace, identifier).unlink(missing_ok=True)
            interrupt()
        real_prune(workspace, identifier)

    def reclaim(workspace, ledger):
        if boundary == 'retire':
            interrupt()
        real_reclaim(workspace, ledger)

    def unlink(path, *args, **kwargs):
        result = real_unlink(path, *args, **kwargs)
        if boundary == 'ledger_prune' and path.parent == source.control / 'transfers':
            interrupt()
        return result

    monkeypatch.setattr(transfers, 'write_json_atomic', write)
    monkeypatch.setattr(receipts, 'remember', remember)
    monkeypatch.setattr(transfers, '_prune_import_receipts', prune)
    monkeypatch.setattr(transfers, '_reclaim_retired_transfer', reclaim)
    monkeypatch.setattr(Path, 'unlink', unlink)
    with pytest.raises(OSError, match='injected interruption'):
        _transfer_local_to_local(source, destination, [job_id], quiet=True)
    assert triggered
    # Once import became authoritative, there is exactly one destination marker.
    assert len([m for m in destination.scan_markers() if m.job_id == job_id]) == 1
    monkeypatch.undo()
    _transfer_local_to_local(source, destination, [job_id], quiet=True)
    # Explicit duplicate transfer of the now missing source is a safe no-op.
    assert _transfer_local_to_local(source, destination, [job_id], quiet=True) == []
    assert len([m for m in destination.scan_markers() if m.job_id == job_id]) == 1
    _no_transfer_files(source)
    _no_transfer_files(destination)
    assert source.check().ok and destination.check().ok
    _transfer_local_to_local(destination, source, [job_id], quiet=True)
    # An ancient intact replay must not recreate a job after it left this end.
    acknowledgement = destination.import_bundle(saved)
    assert destination.find_marker_by_id(job_id) is None
    assert source.find_marker_by_id(job_id) is not None
    source.acknowledge_transfer(acknowledgement)
    _no_transfer_files(source)
    _no_transfer_files(destination)
    assert not list((destination.control / 'journal').iterdir())
    assert source.check().ok and destination.check().ok


def test_out_of_order_imports_compact_only_after_holes_are_filled(tmp_path):
    source, destination = _pair(tmp_path)
    bundles = []
    for index in range(8):
        payload, job_id = _payload(tmp_path / str(index))
        source.submit(payload, 'jobs')
        bundles.append(source.detach(job_id, destination_workspace_id=destination.workspace_id))
    for index in (7, 5, 3, 1, 6, 4, 2, 0):
        bundle = bundles[index]
        first = destination.import_bundle(bundle)
        assert destination.import_bundle(bundle) == first
        source.acknowledge_transfer(first)
    received = json.loads((destination.control / 'transfers/protocol/received.json').read_text())
    epoch = first["transfer_epoch"]
    assert received == {f"{source.workspace_id}/{epoch}": [[1, 8]]}
    _no_transfer_files(source)
    _no_transfer_files(destination)


def test_reservation_interrupted_before_fencing_reissues_fresh_epoch(tmp_path, monkeypatch):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    original = source.transition

    def interrupt(*args, **kwargs):
        raise OSError('before fence')

    monkeypatch.setattr(source, 'transition', interrupt)
    with pytest.raises(OSError, match='before fence'):
        source.detach(job_id, destination_workspace_id=destination.workspace_id)
    interrupted = json.loads((source.control / 'transfers/protocol/issued.json').read_text())[destination.workspace_id]
    monkeypatch.setattr(source, 'transition', original)
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = transfers.validate_bundle(bundle)
    assert manifest['transfer_sequence'] == 1
    assert manifest['transfer_epoch'] != interrupted['epoch']
    assert manifest['transfer_id'] not in interrupted['pending']
    source.acknowledge_transfer(destination.import_bundle(bundle))
    issued = json.loads((source.control / 'transfers/protocol/issued.json').read_text())
    assert issued == {
        destination.workspace_id: {'epoch': issued[destination.workspace_id]['epoch'], 'last': 1, 'pending': {}}
    }
    assert len(issued[destination.workspace_id]['epoch']) == 36

    assert not list((source.control / 'journal').iterdir())


def test_job_can_move_on_before_interrupted_receiver_retries(tmp_path, monkeypatch):
    from httk.workflow import Workspace

    source, destination = _pair(tmp_path)
    sink = Workspace.initialize(tmp_path / 'sink')
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    saved = tmp_path / 'delayed'
    shutil.copytree(bundle, saved)

    def interrupt(*args):
        raise OSError('receipt interrupted')

    monkeypatch.setattr(receipts, 'remember', interrupt)
    with pytest.raises(OSError, match='receipt interrupted'):
        destination.import_bundle(bundle)
    monkeypatch.undo()
    _transfer_local_to_local(destination, sink, [job_id], quiet=True)
    ack = destination.import_bundle(saved)
    source.acknowledge_transfer(ack)
    assert source.find_marker_by_id(job_id) is None
    assert destination.find_marker_by_id(job_id) is None
    assert sink.find_marker_by_id(job_id) is not None
    _no_transfer_files(destination)
    assert not list((destination.control / 'journal').iterdir())
    assert all(workspace.check().ok for workspace in (source, destination, sink))


def test_concurrent_receivers_import_once(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    with ThreadPoolExecutor(max_workers=8) as pool:
        acknowledgements = list(pool.map(destination.import_bundle, [bundle] * 16))
    assert all(ack == acknowledgements[0] for ack in acknowledgements)
    assert len(list(destination.scan_markers())) == 1
    source.acknowledge_transfer(acknowledgements[0])
    _no_transfer_files(source)
    _no_transfer_files(destination)
    assert source.check().ok and destination.check().ok


def test_remote_receive_discards_staging_and_retirement_uses_transfer_id(tmp_path, capsys):
    import argparse

    from httk.core.cli import CLIContext

    from httk.workflow.workflow_cli._transfer import handle_transfer_receive, handle_transfer_retire

    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = transfers.validate_bundle(bundle)
    staging = destination.control / 'transfers/incoming' / manifest['transfer_id']
    shutil.copytree(bundle, staging)
    context = CLIContext('httk', tmp_path)
    args = argparse.Namespace(workspace=str(destination.root), bundle=str(staging))
    assert handle_transfer_receive(args, context) == 0
    ack = json.loads(capsys.readouterr().out)
    assert not staging.exists()
    retire = argparse.Namespace(
        workspace=str(source.root),
        jobs=[job_id],
        destination_workspace_id=destination.workspace_id,
        acknowledgements_json=json.dumps([ack]),
        json=True,
    )
    assert handle_transfer_retire(retire, context) == 0
    capsys.readouterr()
    _transfer_local_to_local(destination, source, [job_id], quiet=True)
    newer = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    newer_manifest = transfers.validate_bundle(newer)
    assert newer_manifest['transfer_id'] != ack['transfer_id']
    # Replaying the old remote retirement must leave the NEW sealed bundle intact.
    assert handle_transfer_retire(retire, context) == 0
    capsys.readouterr()
    assert transfers.validate_bundle(newer) == newer_manifest
    source.acknowledge_transfer(destination.import_bundle(newer))
    _no_transfer_files(source)
    _no_transfer_files(destination)


def test_exact_offer_of_a_completed_job_is_an_empty_success(tmp_path):
    source, destination = _pair(tmp_path)
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    _transfer_local_to_local(source, destination, [job_id], quiet=True)
    assert (
        transfers.offer_transfers(
            source, destination_workspace_id=destination.workspace_id, states=('submitted',), job_ids=[job_id]
        )
        == []
    )
