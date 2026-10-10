"""Bounded transfer metadata and crash/replay tests: what a transfer leaves behind, and when it goes."""

import json
import shutil
import time

import pytest

from httk.workflow import _txn, transfers
from httk.workflow._receipts import receipt_path
from httk.workflow.workflow_cli._transfer import _transfer_local_to_local
from test_transfer_hardening import _pair, _payload


def _metadata(workspace):
    """Every metadata file, with journal writer ids (random per writer) left out of the comparison."""

    names = []
    for path in workspace.control.rglob('*'):
        if path.is_file():
            parts = path.relative_to(workspace.control).parts
            names.append('/'.join(('journal', '*', *parts[2:]) if parts[0] == 'journal' else parts))
    return sorted(names)


#: Far enough ahead that every acknowledgement, retired bundle and 13.1 receipt has expired.
_LATER = 30 * 24 * 3600


def _no_transfer_files(workspace):
    """Once retention and the freshness window have passed, a transfer leaves no file behind."""

    workspace.collect_garbage(now=time.time() + _LATER)
    assert not [path for path in (workspace.control / 'transfers').rglob('*') if path.is_file()]


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
    # Only the collection's own summary frame is left in the journal.
    assert len(list((destination.control / 'journal').iterdir())) <= 1
    assert not list((destination.control / 'tmp').iterdir())
    # The home workspace necessarily holds N live job markers and journal
    # states. Compare transfer bookkeeping there; compare ALL HPC metadata.
    source_protocol = [name for name in _metadata(source) if name.startswith('transfers/')]
    return source_protocol, _metadata(destination)


def test_five_and_fifty_roundtrips_have_identical_residual_files(tmp_path):
    small = _roundtrip(tmp_path / 'five', 5)
    large = _roundtrip(tmp_path / 'fifty', 50)
    assert small == large


def test_out_of_order_imports_and_replays_keep_one_receipt_per_transfer(tmp_path):
    source, destination = _pair(tmp_path)
    bundles = []
    for index in range(8):
        payload, job_id = _payload(tmp_path / str(index))
        source.submit(payload, 'jobs')
        bundles.append(source.detach(job_id, destination_workspace_id=destination.workspace_id))
    copies = []
    for index, bundle in enumerate(bundles):
        copies.append(tmp_path / f'copy-{index}')
        shutil.copytree(bundle, copies[-1], symlinks=True)
    for index in (7, 5, 3, 1, 6, 4, 2, 0):
        first = destination.import_bundle(bundles[index])
        # A second copy of the same transfer is a replay with the same acknowledgement.
        assert destination.import_bundle(copies[index]) == first
        source.acknowledge_transfer(first)
        assert receipt_path(destination, str(first['transfer_id'])).is_file()
    assert len(list((destination.control / 'transfers' / 'received').iterdir())) == 8
    _no_transfer_files(source)
    _no_transfer_files(destination)


def test_job_can_move_on_before_interrupted_receiver_retries(tmp_path, monkeypatch):
    from httk.workflow import Workspace

    source, destination = _pair(tmp_path)
    sink = Workspace.initialize(tmp_path / 'sink')
    payload, job_id = _payload(tmp_path)
    source.submit(payload, 'jobs')
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    saved = tmp_path / 'delayed'
    shutil.copytree(bundle, saved)

    def interrupt(step):
        if step == 'V9.published':
            _txn._HOOK = None
            raise OSError('receipt interrupted')

    _txn._HOOK = interrupt
    with pytest.raises(OSError, match='receipt interrupted'):
        destination.import_bundle(bundle)
    # The receiver's recovery finishes the import (the owner of the lineage is gone) ...
    monkeypatch.setattr(_txn, 'owner_gone', lambda *_args, **_kwargs: True)
    # ... and the job moves on, keeping the fact that it was received here.
    _transfer_local_to_local(destination, sink, [job_id], quiet=True)
    # A delayed copy of the original transfer is a replay, never a second import.
    ack = destination.import_bundle(saved)
    source.acknowledge_transfer(ack)
    assert source.find_marker_by_id(job_id) is None
    assert destination.find_marker_by_id(job_id) is None
    assert sink.find_marker_by_id(job_id) is not None
    _no_transfer_files(destination)
    # Only the collection's own summary frame is left in the journal.
    assert len(list((destination.control / 'journal').iterdir())) <= 1
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
    [result] = json.loads(capsys.readouterr().out)['results']
    assert result['status'] == 'imported'
    ack = result['acknowledgement']
    assert not staging.exists()
    retire = argparse.Namespace(
        words=[str(source.root), job_id],
        operator_workspace=None,
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
