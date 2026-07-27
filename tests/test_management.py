import bz2
import json
import uuid
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from conftest import register_ws
from httk.core import CLIContext

from httk.workflow import Workspace
from httk.workflow.adapters import add_remote, import_v1_remote, run_adapter
from httk.workflow.configuration import identity_key_paths
from httk.workflow.manifests import create_manifest, verify_manifest
from httk.workflow.projects import initialize_project
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
                "format_version": 1,
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


def test_all_command_groups_have_help(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    for group in ("workspace", "runner", "job", "manager", "config", "project", "remote", "transfer"):
        assert command([group, "--help"], context) == 0
    assert command(["v1", "prepare", "--help"], context) == 0
    assert "usage:" in capsys.readouterr().out


def test_config_is_xdg_isolated_and_private_key_is_0600(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    context = CLIContext("httk", tmp_path)
    assert (
        command(
            ["config", "init", "--name", "A User", "--email", "a@example.test", "--non-interactive"],
            context,
        )
        == 0
    )
    private, public = identity_key_paths()
    assert private.is_file() and public.is_file()
    assert private.stat().st_mode & 0o777 == 0o600
    assert not (tmp_path / ".httk").exists()


def test_noninteractive_project_init_requires_explicit_name(tmp_path: Path) -> None:
    context = CLIContext("httk", tmp_path)
    assert command(["project", "init", str(tmp_path / "project"), "--non-interactive"], context) == 2


def test_manifest_determinism_special_names_exclusions_and_tampering(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="manifest-test", manifest_exclusions=("ignored*",))
    (project / "space and\nnewline").write_bytes(b"content")
    (project / "empty").mkdir()
    (project / "link").symlink_to("space and\nnewline")
    (project / "ignored-secret").write_text("private", encoding="utf-8")
    manifest = create_manifest(project)
    first = manifest.read_bytes()
    assert verify_manifest(project)
    assert create_manifest(project).read_bytes() == first
    (project / "ignored-secret").write_text("changed", encoding="utf-8")
    assert verify_manifest(project)
    (project / "space and\nnewline").write_bytes(b"tampered")
    assert not verify_manifest(project)
    body = bz2.decompress(first)
    assert b"space and\\nnewline" in body


def test_manifest_refuses_active_workspace(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="active")
    workspace = Workspace(project)
    payload, job_id = _payload(tmp_path)
    submitted = workspace.submit(payload, "jobs")
    # Construct the active state through the public transition protocol.
    from httk.workflow.journal import JournalWriter

    with JournalWriter(workspace.control) as writer:
        workspace.transition(writer, submitted, "running", {"reason": "test"})
    with pytest.raises(ValueError, match="quiescent"):
        create_manifest(project)
    assert workspace.find_marker_by_id(job_id) is not None


def test_adapter_json_contract_and_no_shell_interpolation(tmp_path: Path) -> None:
    bundle = tmp_path / "adapter-bundle"
    bundle.mkdir()
    executable = bundle / "adapter"
    executable.write_text(
        """#!/usr/bin/env python3
import json, sys
request = json.load(open(sys.argv[1]))
print(json.dumps({"format":"httk-computer-result","format_version":1,
                  "operation":request["operation"],"ok":True,"argv":request["argv"]}))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    (bundle / "remote.json").write_text(
        json.dumps(
            {
                "format": "httk-computer-adapter",
                "format_version": 1,
                "adapter_version": 1,
                "queues": {"default": {}},
            }
        ),
        encoding="utf-8",
    )
    sentinel = tmp_path / "must-not-exist"
    argument = f"value;touch {sentinel}"
    result = run_adapter(bundle, "invoke", {"argv": [argument]})
    assert result["argv"] == [argument]
    assert not sentinel.exists()


def test_safe_v1_remote_import_uses_maintained_adapter(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="legacy-import")
    legacy = tmp_path / "legacy-local"
    legacy.mkdir()
    for executable in ("command", "install", "push", "pull", "start-taskmgr"):
        (legacy / executable).write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (legacy / "config").write_text(
        'LOCAL_HTTK_DIR="~/Httk-runs"\nVASP_COMMAND="value; touch should-not-run"\n',
        encoding="utf-8",
    )
    imported = import_v1_remote(legacy, name="mapped", project=project)
    metadata = json.loads((imported / "remote.json").read_text(encoding="utf-8"))
    assert metadata["kind"] == "local"
    assert metadata["legacy_import"]["legacy_executables_copied"] is False
    assert metadata["queues"]["default"]["legacy_settings"]["VASP_COMMAND"] == "value; touch should-not-run"
    assert (imported / "adapter").is_file()
    assert not (imported / "command").exists()


def test_workspace_upgrade_and_transfer_round_trip_are_idempotent(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    assert source.upgrade(["detached-transfer-v1"]) == frozenset({"detached-transfer-v1"})
    destination = Workspace.initialize(tmp_path / "destination", extensions=["detached-transfer-v1"])
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    transfer_id = str(uuid.uuid4())
    bundle = source.detach(
        job_id,
        destination_workspace_id=destination.workspace_id,
        transfer_id=transfer_id,
    )
    assert source.recover_transfers()[0]["transfer_id"] == transfer_id
    acknowledgement = destination.import_bundle(bundle)
    retired = source.acknowledge_transfer(acknowledgement)
    assert destination.import_bundle(retired) == acknowledgement
    assert source.acknowledge_transfer(acknowledgement) == retired
    marker = destination.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "submitted"


def test_tasks_send_uses_adapter_status_push_import_and_ack(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["queues"]["default"]["workspace"] = str(destination_root)
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload, job_id = _payload(tmp_path)
    Workspace(source_root).submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination_root, "station", remote="cluster")
    assert command(["transfer", "home", "station", "--job", job_id], context) == 0
    imported = Workspace(destination_root).find_marker_by_id(job_id)
    assert imported is not None and imported.kind == "submitted"
    assert Workspace(source_root).find_marker_by_id(job_id) is None


def test_transfer_send_resumes_after_copy_before_import(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["queues"]["default"]["workspace"] = str(destination_root)
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload, job_id = _payload(tmp_path)
    Workspace(source_root).submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination_root, "station", remote="cluster")

    from httk.workflow import workflow_cli

    real_run_adapter = workflow_cli.run_adapter
    failed = False

    def interrupt_import(bundle, operation, request, *, timeout=None):
        nonlocal failed
        if operation == "invoke" and not failed:
            failed = True
            raise RuntimeError("simulated interruption after push")
        return real_run_adapter(bundle, operation, request, timeout=timeout)

    monkeypatch.setattr(workflow_cli, "run_adapter", interrupt_import)
    # A resumed transfer: an interrupted send, retyped, must pick up where it
    # stopped rather than start a second copy.
    arguments = ["transfer", "home", "station", "--job", job_id]
    assert command(arguments, context) == 2
    assert Workspace(source_root).find_marker_by_id(job_id) is None
    monkeypatch.setattr(workflow_cli, "run_adapter", real_run_adapter)
    assert command(arguments, context) == 0
    assert Workspace(destination_root).find_marker_by_id(job_id) is not None
