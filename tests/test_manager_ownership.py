import json
import os
import shutil
import uuid
from dataclasses import replace
from pathlib import Path
from typing import cast

from httk.workflow import TaskManager, Workspace, _manager_requests
from httk.workflow.models import STATE_KINDS, marker_basename
from httk.workflow.workspace import MarkerStream

_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def _payload(root: Path, *, tag: str) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / tag
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text(_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 1,
                "id": job_id,
                "tag": tag,
                "name": tag,
                "workflow": "tests.ownership",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "run",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
                "parent": None,
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def test_window_and_walk_filter_foreign_markers_in_every_state(tmp_path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="central-filter")
    workspace.submit(source, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None

        class Stream:
            def __init__(self, markers):
                self.markers = markers

            def advance(self, **_kwargs):
                return self.markers

        for index, kind in enumerate(STATE_KINDS):
            owned = replace(marker, kind=kind)
            foreign = replace(marker, kind=kind)
            manager._streams[f"central-{index}"] = cast(MarkerStream, Stream([foreign, owned]))
            monkeypatch.setattr(manager, "_owns", lambda candidate, owned=owned: candidate is owned)
            assert manager._window(f"central-{index}", kind) == [owned]

        owned = replace(marker, kind="waiting")
        foreign = replace(marker, kind="waiting")
        monkeypatch.setattr(manager.workspace, "walk_markers", lambda *_args, **_kwargs: iter([foreign, owned]))
        monkeypatch.setattr(manager, "_owns", lambda candidate: candidate is owned)
        assert list(manager._walk(STATE_KINDS)) == [owned]


def test_ready_hardlink_forgery_requires_payload_provenance(tmp_path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="legitimate")
    workspace.submit(source, "jobs")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        legitimate = workspace.find_marker_by_id(job_id)
        assert legitimate is not None and legitimate.kind == "ready"

        forged_id = str(uuid.uuid4())
        forged_key = f"forged--{forged_id}"
        forged_payload = workspace.payload_path(legitimate.placement, forged_key)
        shutil.copytree(workspace.payload_path(legitimate.placement, legitimate.job_key), forged_payload)
        document = json.loads((forged_payload / "job.json").read_text(encoding="utf-8"))
        document["id"] = forged_id
        document["tag"] = "forged"
        (forged_payload / "job.json").write_text(json.dumps(document), encoding="utf-8")
        forged_marker = legitimate.path.with_name(
            marker_basename(forged_key, legitimate.priority, legitimate.generation, legitimate.record_ref)
        )
        os.link(legitimate.path, forged_marker)

        from types import SimpleNamespace

        real_lstat = Path.lstat

        def fake_lstat(path: Path):
            original = real_lstat(path)
            if path == forged_payload:
                return SimpleNamespace(st_uid=manager.uid + 1, st_mode=original.st_mode)
            return original

        monkeypatch.setattr(Path, "lstat", fake_lstat)
        eligible = manager._eligible_ready()
        assert all(item.job_key != forged_key for item in eligible)
        assert any(item.job_key == legitimate.job_key for item in eligible)
        assert manager._claim_and_launch(legitimate)

        manager.tick()
        current = workspace.find_marker_by_id(job_id)
        assert current is not None and current.kind in {"running", "succeeded"}


def test_symlinked_payload_directory_is_refused(tmp_path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="symlinked")
    workspace.submit(source, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        payload = workspace.payload_path(marker.placement, marker.job_key)
        saved = payload.with_name(payload.name + ".saved")
        payload.rename(saved)
        payload.symlink_to(saved, target_is_directory=True)
        assert not manager._owns(marker)
        assert manager._eligible_ready() == []


def test_marker_symlink_is_not_owned_accepted(tmp_path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="marker-link")
    workspace.submit(source, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        target = marker.path.with_name(marker.path.name + ".target")
        marker.path.rename(target)
        marker.path.symlink_to(target)
        assert not manager._owns(marker)
        assert manager._eligible_ready() == []


def test_payload_swap_after_ownership_check_is_refused(tmp_path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="swap")
    workspace.submit(source, "jobs")
    alternate_source, _ = _payload(tmp_path / "alternate", tag="alternate")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "ready"
        payload = workspace.payload_path(marker.placement, marker.job_key)
        alternate = payload.with_name(payload.name + ".alternate")
        shutil.copytree(alternate_source, alternate)
        saved = payload.with_name(payload.name + ".saved")
        real_owns = manager._owns
        swapped = False

        def owns(candidate):
            nonlocal swapped
            result = real_owns(candidate)
            if not swapped and candidate.path == marker.path:
                swapped = True
                payload.rename(saved)
                alternate.rename(payload)
            return result

        monkeypatch.setattr(manager, "_owns", owns)
        assert not manager._claim_and_launch(marker)
        assert workspace.find_marker_by_id(job_id).kind == "ready"  # type: ignore[union-attr]


def test_job_digest_is_rechecked_before_launch(tmp_path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="digest")
    workspace.submit(source, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "ready"
        payload = workspace.payload_path(marker.placement, marker.job_key)
        real_launch = manager._launch_claimed

        def tamper_then_launch(claimed, job, state):
            document = json.loads((payload / "job.json").read_text(encoding="utf-8"))
            document["name"] = "tampered"
            (payload / "job.json").write_text(json.dumps(document), encoding="utf-8")
            real_launch(claimed, job, state)

        monkeypatch.setattr(manager, "_launch_claimed", tamper_then_launch)
        assert manager._claim_and_launch(marker)
        failed = workspace.find_marker_by_id(job_id)
        assert failed is not None and failed.kind == "failed"
        assert workspace.read_state(failed)["failure"]["code"] == "payload.tampered"
        assert not list(payload.glob(".httk-attempt.*"))


def test_request_replaced_during_claim_is_not_applied(tmp_path, monkeypatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="request")
    workspace.submit(source, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None and marker.kind == "ready"
        request = {
            "format": "httk-workflow-request",
            "format_version": 1,
            "request_id": str(uuid.uuid4()),
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "expected_generation": marker.generation,
            "expected_record_ref": marker.record_ref,
            "action": "cancel",
            "operator": "tester",
            "reason": "test",
        }
        request_path = workspace.publish_request(request)
        claimed_path = workspace.control / "requests" / "claimed" / manager.manager_id / request_path.name
        real_rename = _manager_requests.os.rename

        def rename(source_path, destination):
            real_rename(source_path, destination)
            if Path(destination) == claimed_path:
                swapped = tmp_path / "swapped.json"
                swapped.write_text(json.dumps({**request, "action": "pause"}), encoding="utf-8")
                os.replace(swapped, destination)

        monkeypatch.setattr(_manager_requests.os, "rename", rename)
        manager._handle_requests()

    current = workspace.find_marker_by_id(job_id)
    assert current is not None and current.kind == "ready"


def test_request_job_id_must_match_job_key(tmp_path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    source, job_id = _payload(tmp_path / "source", tag="request-id")
    workspace.submit(source, "jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        marker = workspace.find_marker_by_id(job_id)
        assert marker is not None
        workspace.publish_request(
            {
                "format": "httk-workflow-request",
                "format_version": 1,
                "request_id": str(uuid.uuid4()),
                "job_id": str(uuid.uuid4()),
                "job_key": marker.job_key,
                "placement": marker.placement.as_posix(),
                "expected_generation": marker.generation,
                "expected_record_ref": marker.record_ref,
                "action": "cancel",
                "operator": "tester",
                "reason": "test",
            }
        )
        manager._handle_requests()
    assert list((workspace.control / "quarantine").iterdir())
    current = workspace.find_marker_by_id(job_id)
    assert current is not None and current.kind == "ready"
