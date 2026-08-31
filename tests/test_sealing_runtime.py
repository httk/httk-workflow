"""Runtime enforcement of seals: refused mutations, auto-sealing, and transfers.

These tests exercise the seal library wired into the write funnels, the
manager's auto-seal of succeeded jobs, garbage collection's protection of
sealed jobs, and the seal that travels with a detached transfer. The seal
library itself is covered by ``test_seals``; here every seal is produced or
enforced through the ordinary runtime paths an operator drives.
"""

import json
import logging
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from httk.core.identity import ensure_identity_key
from httk.core.project.sealing import seal_project

from httk.workflow import TaskManager, Workspace, _manager_commit
from httk.workflow.errors import SealedError
from httk.workflow.journal import JournalWriter
from httk.workflow.projects import initialize_project
from httk.workflow.removal import remove_jobs
from httk.workflow.seals import (
    is_job_sealed,
    is_workspace_sealed,
    job_seal_path,
    seal_job,
    seal_workspace,
    verify_job_seal,
    verify_tree,
    verify_workspace_seal,
)
from httk.workflow.transfers import TRANSFER_DIRECTORY, TRANSFER_MANIFEST, validate_bundle

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def _payload(root: Path, name: str = "job", *, runner_source: str = "#!/bin/sh\nexit 0\n") -> tuple[Path, str]:
    """Write one minimal, valid job payload and return its path and job id."""

    job_id = str(uuid.uuid4())
    payload = root / name
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": name,
                "name": f"seal runtime {name}",
                "workflow": "tests.sealing",
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


# -- auto-seal of succeeded jobs ---------------------------------------------


def test_manager_seals_a_succeeded_job(tmp_path: Path) -> None:
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source", runner_source=_SUCCEED_RUNNER)
    workspace.submit(payload, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert is_job_sealed(workspace, marker.job_key)
    assert verify_job_seal(workspace, marker).valid


def test_manager_does_not_seal_when_disabled(tmp_path: Path) -> None:
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("seal.succeeded", "false")
    payload, job_id = _payload(tmp_path / "source", runner_source=_SUCCEED_RUNNER)
    workspace.submit(payload, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert not is_job_sealed(workspace, marker.job_key)


def test_manager_keeps_a_job_succeeded_when_sealing_cannot_sign(tmp_path: Path, caplog) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    # No project and a project-only key ref: no signing key resolves, so sealing
    # fails, but the job that ran cleanly must stay succeeded regardless.
    workspace.set_setting("seal.keys", "project")
    payload, job_id = _payload(tmp_path / "source", runner_source=_SUCCEED_RUNNER)
    workspace.submit(payload, "jobs")
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow.manager"),
        TaskManager(workspace, heartbeat_interval=0.01) as manager,
    ):
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert not is_job_sealed(workspace, marker.job_key)
    assert any(getattr(record, "event", None) == "job_seal_failed" for record in caplog.records)


# -- crash-injection idempotency ---------------------------------------------


def test_reseal_is_idempotent_and_recreated_after_a_lost_seal(tmp_path: Path) -> None:
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _job_id = _payload(tmp_path / "source")
    marker = workspace.submit(payload, "jobs")

    seal_job(workspace, marker)
    path = job_seal_path(workspace, marker.job_key)
    first = path.read_bytes()
    # Re-sealing while the seal is present returns the same document byte for byte.
    seal_job(workspace, marker)
    assert path.read_bytes() == first

    # Simulate a crash between the succeed transition and the seal: the seal is
    # gone, and the commit-side helper recreates a valid seal of the same payload.
    path.unlink()
    assert not is_job_sealed(workspace, marker.job_key)
    _manager_commit._auto_seal_succeeded(SimpleNamespace(workspace=workspace), marker)
    assert is_job_sealed(workspace, marker.job_key)
    verification = verify_job_seal(workspace, marker)
    assert verification.valid
    assert verification.discrepancies == ()


# -- refused mutation of a sealed job ----------------------------------------


def test_sealed_job_refuses_transition_and_removal(tmp_path: Path) -> None:
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace")
    sealed_marker = workspace.submit(_payload(tmp_path / "source", "sealed")[0], "jobs")
    open_marker = workspace.submit(_payload(tmp_path / "source", "open")[0], "jobs")
    seal_job(workspace, sealed_marker)

    with pytest.raises(SealedError, match="sealed"), JournalWriter(workspace.control) as writer:
        workspace.transition(writer, sealed_marker, "running", {"reason": "test"})

    # All-or-nothing: the removable sibling is not removed while a sealed job is
    # in the same batch.
    report = remove_jobs(workspace, [sealed_marker, open_marker])
    assert report.removed_count == 0
    assert {marker.job_id for marker in workspace.scan_markers()} == {sealed_marker.job_id, open_marker.job_id}
    reasons = {outcome.job_key: outcome.reason or "" for outcome in report.outcomes}
    assert "sealed" in reasons[sealed_marker.job_key]
    assert "batch preflight refused" in reasons[open_marker.job_key]

    # The unsealed sibling removes cleanly on its own.
    solo = remove_jobs(workspace, [open_marker])
    assert solo.removed_count == 1
    assert {marker.job_id for marker in workspace.scan_markers()} == {sealed_marker.job_id}


# -- refused mutation of a sealed workspace and project ----------------------


def test_sealed_workspace_refuses_writes_but_stays_readable(tmp_path: Path) -> None:
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace")
    marker = workspace.submit(_payload(tmp_path / "source")[0], "jobs")
    seal_job(workspace, marker)
    seal_workspace(workspace)
    assert is_workspace_sealed(workspace)

    with pytest.raises(SealedError):
        workspace.submit(_payload(tmp_path / "later", "later")[0], "jobs")
    with pytest.raises(SealedError):
        workspace.publish_runner(_payload(tmp_path / "runner-src", "r")[0] / "files" / "runner")
    with pytest.raises(SealedError):
        workspace.set_policy({"visibility_deadline_seconds": 0.5})
    with pytest.raises(SealedError):
        workspace.set_setting("vasp.command", "vasp")

    # Removal refuses as an all-or-nothing report (exit 1), never a raise.
    from httk.workflow.removal import remove_jobs

    report = remove_jobs(workspace, [marker])
    assert report.removed_count == 0
    assert "workspace is sealed" in (report.outcomes[0].reason or "")
    assert {m.job_id for m in workspace.scan_markers()} == {marker.job_id}

    # Attach, scan, gc, and fsck all keep working on a sealed workspace.
    reattached = Workspace(workspace.root)
    assert {m.job_id for m in reattached.scan_markers()} == {marker.job_id}
    workspace.collect_garbage()
    from httk.workflow.fsck import check_workspace

    check_workspace(workspace)


def test_sealed_project_refuses_new_workspace(tmp_path: Path) -> None:
    ensure_identity_key()
    project = tmp_path / "project"
    initialize_project(project, name="sealed")
    workspace = Workspace.initialize(project / "work")
    marker = workspace.submit(_payload(tmp_path / "source")[0], "jobs")
    seal_job(workspace, marker)
    seal_workspace(workspace)
    seal_project(project)

    with pytest.raises(SealedError, match="sealed"):
        Workspace.initialize(project / "work2")


# -- garbage collection protects sealed jobs and the seal store --------------


def test_gc_keeps_a_sealed_job_and_the_seal_store(tmp_path: Path) -> None:
    ensure_identity_key()
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    marker = workspace.submit(_payload(tmp_path / "source")[0], "jobs")
    seal_job(workspace, marker)
    # Removing the payload would ordinarily make the job a removed-jobs candidate.
    shutil.rmtree(workspace.payload_path(marker.placement, marker.job_key))
    report = workspace.collect_garbage()
    assert report.category("removed_jobs").removed == 0
    assert marker.path.is_file()
    assert is_job_sealed(workspace, marker.job_key)
    assert (workspace.control / "seals").is_dir()


# -- a seal travels with a detached transfer ---------------------------------


def _pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    return Workspace.initialize(tmp_path / "source"), Workspace.initialize(tmp_path / "destination")


def test_a_seal_survives_a_detached_transfer_round_trip(tmp_path: Path) -> None:
    ensure_identity_key()
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "src-payload")[0], "jobs")
    seal_job(source, marker)

    bundle = source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)
    assert (bundle / TRANSFER_DIRECTORY / "seal.json").is_file()

    acknowledgement = destination.import_bundle(bundle)
    imported = destination.find_marker_by_id(marker.job_id)
    assert imported is not None
    assert is_job_sealed(destination, imported.job_key)
    assert verify_job_seal(destination, imported).valid

    # Retiring the source drops its now-orphaned seal; the destination keeps its own.
    assert is_job_sealed(source, marker.job_key)
    source.acknowledge_transfer(acknowledgement)
    assert not is_job_sealed(source, marker.job_key)
    assert is_job_sealed(destination, imported.job_key)


def test_a_tampered_bundled_seal_is_refused(tmp_path: Path) -> None:
    ensure_identity_key()
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "src-payload")[0], "jobs")
    seal_job(source, marker)
    bundle = source.detach(marker.job_id, destination_workspace_id=destination.workspace_id)

    seal_in_bundle = bundle / TRANSFER_DIRECTORY / "seal.json"
    seal_in_bundle.write_text(seal_in_bundle.read_text(encoding="utf-8") + " ", encoding="utf-8")

    with pytest.raises(Exception, match="seal digest mismatch"):
        validate_bundle(bundle)
    with pytest.raises(Exception, match="seal digest mismatch"):
        destination.import_bundle(bundle)
    # The manifest still declares the transfer; only the seal bytes were altered.
    assert (bundle / TRANSFER_DIRECTORY / TRANSFER_MANIFEST).is_file()


def test_verify_tree_on_an_unsealed_subject_reports_not_sealed(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.submit(_payload(tmp_path / "source")[0], "jobs")
    # No seal exists; verification reports it rather than leaking a file error.
    report = verify_tree(workspace.root)
    assert not report.ok
    assert [(entry["level"], entry["reason"]) for entry in report.entries] == [("workspace", "not sealed")]
    assert verify_workspace_seal(workspace).reason == "not sealed"


def _seal_root_project(tmp_path: Path):
    """Build a root-as-workspace project with one sealed job, workspace, project."""
    ensure_identity_key()
    project = tmp_path / "project"
    initialize_project(project, name="pp")
    workspace = Workspace.initialize(project)
    marker = workspace.submit(_payload(tmp_path / "source")[0], "jobs")
    return project, workspace, marker


def test_project_seal_excludes_the_default_postprocess_tree(tmp_path: Path) -> None:
    project, workspace, marker = _seal_root_project(tmp_path)
    seal_job(workspace, marker)
    seal_workspace(workspace)
    seal_project(project)

    # Postprocess output lands under <root>/postprocess after sealing; it must not
    # count as a loose project file, or the project seal would break.
    out = workspace.root / "postprocess" / "jobs" / marker.job_key / "plot"
    out.mkdir(parents=True)
    (out / "chart.svg").write_text("<svg/>\n", encoding="utf-8")
    assert verify_tree(project).ok


def test_project_seal_excludes_a_configured_postprocess_dir(tmp_path: Path) -> None:
    project, workspace, marker = _seal_root_project(tmp_path)
    # The setting must be stored before sealing (a sealed workspace refuses writes).
    workspace.set_setting("postprocess.directory", "reports")
    seal_job(workspace, marker)
    seal_workspace(workspace)
    seal_project(project)

    out = workspace.root / "reports" / "jobs" / marker.job_key / "plot"
    out.mkdir(parents=True)
    (out / "chart.svg").write_text("<svg/>\n", encoding="utf-8")
    assert verify_tree(project).ok


def test_detach_refuses_a_sealed_workspace(tmp_path: Path) -> None:
    ensure_identity_key()
    source, destination = _pair(tmp_path)
    marker = source.submit(_payload(tmp_path / "src-payload")[0], "jobs")
    seal_job(source, marker)
    seal_workspace(source)

    # allow_sealed carries a job's own seal through a transfer, but a sealed
    # workspace still refuses: detaching would break the workspace seal.
    from httk.workflow.transfers import detach_job

    with pytest.raises(SealedError, match="workspace"):
        detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    current = source.find_marker_by_id(marker.job_id)
    assert current is not None and current.kind == marker.kind
