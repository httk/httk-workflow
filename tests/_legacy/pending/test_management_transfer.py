"""Transfer tests split out of ``test_management.py``: they drive the removed transfer stack."""

import json
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace
from httk.workflow.adapters import add_remote
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _common as workflow_common
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command


def _payload(root: Path) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": "test",
                "name": "test",
                "workflow": "tests",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "start",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def test_transfer_round_trip_is_idempotent(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    # A new transfer always mints its own id; a caller-supplied one only resumes.
    with pytest.raises(ValueError, match="mints its own id"):
        source.detach(job_id, destination_workspace_id=destination.workspace_id, transfer_id=str(uuid.uuid4()))
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    transfer_id = bundle.name
    assert source.detach(job_id, destination_workspace_id=destination.workspace_id, transfer_id=transfer_id) == bundle
    assert source.recover_transfers()[0]["transfer_id"] == transfer_id
    acknowledgement = destination.import_bundle(bundle)
    assert destination.import_bundle(bundle) == acknowledgement
    retired = source.acknowledge_transfer(acknowledgement)
    # The retired bundle is kept for retention.trash_days.
    assert retired.is_dir()
    assert source.acknowledge_transfer(acknowledgement) == retired
    marker = destination.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "submitted"


def test_tasks_send_uses_adapter_status_push_import_and_ack(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    Workspace.initialize(source_root / "workspace")
    Workspace.initialize(destination_root / "workspace")
    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(destination_root / "workspace")
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload, job_id = _payload(tmp_path)
    Workspace(source_root / "workspace").submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root / "workspace", "home")
    register_ws(context, destination_root / "workspace", "station", remote="cluster")
    assert command(["job", "transfer", "--job", job_id, "home", "cluster:station"], context) == 0
    imported = Workspace(destination_root / "workspace").find_marker_by_id(job_id)
    assert imported is not None and imported.kind == "submitted"
    assert Workspace(source_root / "workspace").find_marker_by_id(job_id) is None


def test_transfer_send_resumes_after_copy_before_import(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    Workspace.initialize(source_root / "workspace")
    Workspace.initialize(destination_root / "workspace")
    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(destination_root / "workspace")
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload, job_id = _payload(tmp_path)
    Workspace(source_root / "workspace").submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root / "workspace", "home")
    register_ws(context, destination_root / "workspace", "station", remote="cluster")

    real_run_adapter = transfer_cli.run_adapter
    failed = False

    def interrupt_import(bundle, operation, request, *, timeout=None):
        nonlocal failed
        if operation == "invoke" and not failed:
            failed = True
            raise RuntimeError("simulated interruption after push")
        return real_run_adapter(bundle, operation, request, timeout=timeout)

    monkeypatch.setattr(transfer_cli, "run_adapter", interrupt_import)
    monkeypatch.setattr(workflow_common, "run_adapter", interrupt_import)
    # A resumed transfer: an interrupted send, retyped, must pick up where it
    # stopped rather than start a second copy.
    arguments = ["job", "transfer", "--job", job_id, "home", "cluster:station"]
    assert command(arguments, context) == 2
    assert Workspace(source_root / "workspace").find_marker_by_id(job_id) is None
    monkeypatch.setattr(transfer_cli, "run_adapter", real_run_adapter)
    monkeypatch.setattr(workflow_common, "run_adapter", real_run_adapter)
    assert command(arguments, context) == 0
    assert Workspace(destination_root / "workspace").find_marker_by_id(job_id) is not None
