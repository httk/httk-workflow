"""Regression tests for sequence reuse and amortized transfer sweep work."""

import json
import uuid

import pytest

from httk.workflow import WorkspaceCorruptionError, transfers
from httk.workflow._bundle import _payload_digest
from httk.workflow.workflow_cli._transfer import _skipped_report
from test_transfer_hardening import _pair, _payload


def _move(root, source, destination):
    payload, job_id = _payload(root)
    marker = source.submit(payload, 'jobs')
    bundle = source.detach(job_id, marker=marker, destination_workspace_id=destination.workspace_id)
    manifest = transfers.validate_bundle(bundle)
    source.acknowledge_transfer(destination.import_bundle(bundle))
    return job_id, manifest


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
        refusal: type[Exception] = FileExistsError
    else:
        (bundle / 'files/runner').write_text('#!/bin/sh\nexit 17\n')
        manifest[field] = _payload_digest(bundle)
        refusal = WorkspaceCorruptionError
    manifest_path.write_text(json.dumps(manifest))
    # A copy that is not exactly the transfer that arrived is refused, never acknowledged.
    with pytest.raises(refusal, match='already holds job|provenance'):
        destination.import_bundle(bundle)
    assert bundle.exists()
    assert destination.find_marker_by_id(job_id) is not None


def test_missing_job_reports_a_skip_instead_of_silent_success():
    identifier = str(uuid.uuid4())
    assert _skipped_report([identifier], []) == {
        'skipped': [{'job_id': identifier, 'reason': 'not found or already completed'}]
    }


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
