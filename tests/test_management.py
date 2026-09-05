import bz2
import json
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import identity_key_paths, read_identity_config
from httk.core.project.manifests import create_manifest

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.adapters import add_remote, import_v1_remote, run_adapter
from httk.workflow.configuration import read_config
from httk.workflow.journal import JournalWriter
from httk.workflow.manifests import verify_manifest, workspace_maintenance_guard
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


def test_all_command_groups_have_help(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    for group in ("workspace", "runner", "job", "manager", "config", "remote", "transfer"):
        assert command([group, "--help"], context) == 0
    assert command(["v1", "collect", "--help"], context) == 0
    assert "usage:" in capsys.readouterr().out


def test_config_import_v1_writes_identity_and_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    context = CLIContext("httk", tmp_path)
    legacy = tmp_path / "legacy-httk"
    (legacy / "keys").mkdir(parents=True)
    (legacy / "config").write_text(
        "[main]\nname = Legacy User\nemail = legacy@example.test\n",
        encoding="utf-8",
    )
    (legacy / "keys" / "key1.pub").write_bytes(b"legacy-public-key")
    (legacy / "keys" / "key1.seed").write_bytes(b"legacy-private-key")

    assert command(["config", "import-v1", str(legacy)], context) == 0

    # The identity part is written to identity.json by the core function.
    identity = read_identity_config()
    assert identity["identities"] == {"legacy": {"name": "Legacy User", "email": "legacy@example.test"}}
    assert identity["default_identity"] == "legacy"
    assert Path(str(identity["legacy_public_key"])).read_bytes() == b"legacy-public-key"
    # The workflow config records only where the import came from.
    assert read_config()["imported_from"] == str(legacy.resolve())
    # The new named key is generated; legacy private material remains untouched.
    assert identity_key_paths("legacy")[0].is_file()
    assert (legacy / "keys" / "key1.seed").read_bytes() == b"legacy-private-key"


def test_manifest_determinism_special_names_exclusions_and_tampering(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="manifest-test", manifest_exclusions=("ignored*",))
    Workspace.initialize(project)
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
    Workspace.initialize(project)
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


def test_maintenance_guard_refuses_cancelling_workspace(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _job_id = _payload(tmp_path)
    submitted = workspace.submit(payload, "jobs")
    with JournalWriter(workspace.control) as writer:
        running = workspace.transition(writer, submitted, "running", {"reason": "test"})
        workspace.transition(writer, running, "cancelling", {"reason": "test"})
    with pytest.raises(ValueError, match="quiescent workspace"), workspace_maintenance_guard(workspace):
        pass


def test_oversized_attempt_context_fails_as_protocol_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A context too large for one environment value leaves failure evidence."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path / "source")
    workspace.submit(payload, "jobs/oversized-context")
    monkeypatch.setattr(workspace, "read_settings", lambda: {"huge": "x" * 100_000})

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    assert state["failure"]["code"] == "protocol_error"
    root = workspace.payload_path(marker.placement, marker.job_key)
    assert len(list((root / "attempts").iterdir())) == 1


def test_adapter_json_contract_and_no_shell_interpolation(tmp_path: Path) -> None:
    bundle = tmp_path / "adapter-bundle"
    bundle.mkdir()
    executable = bundle / "adapter"
    executable.write_text(
        """#!/usr/bin/env python3
import json, sys
request = json.load(open(sys.argv[1]))
print(json.dumps({"format":"httk-computer-result","format_version":2,
                  "operation":request["operation"],"ok":True,"argv":request["argv"]}))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    (bundle / "remote.json").write_text(
        json.dumps(
            {
                "format": "httk-computer-adapter",
                "format_version": 2,
                "adapter_version": 2,
                "settings": {},
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
    assert metadata["settings"]["legacy_settings"]["VASP_COMMAND"] == "value; touch should-not-run"
    assert (imported / "adapter").is_file()
    assert not (imported / "command").exists()


def test_cli_imports_multiple_v1_remotes(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="legacy-import")
    sources = [tmp_path / "legacy-one", tmp_path / "legacy-two"]
    for source in sources:
        source.mkdir()
        for executable in ("command", "install", "push", "pull", "start-taskmgr"):
            (source / executable).write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (source / "config").write_text('LOCAL_HTTK_DIR="~/Httk-runs"\n', encoding="utf-8")

    assert command(["remote", "import-v1", *(str(source) for source in sources)], CLIContext("httk", project)) == 0
    output = capsys.readouterr().out
    assert all(source.name in output for source in sources)


def test_transfer_round_trip_is_idempotent(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
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
    Workspace.initialize(source_root)
    Workspace.initialize(destination_root)
    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(destination_root)
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload, job_id = _payload(tmp_path)
    Workspace(source_root).submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination_root, "station", remote="cluster")
    assert command(["transfer", "--job", job_id, "home", "cluster:station"], context) == 0
    imported = Workspace(destination_root).find_marker_by_id(job_id)
    assert imported is not None and imported.kind == "submitted"
    assert Workspace(source_root).find_marker_by_id(job_id) is None


def test_transfer_send_resumes_after_copy_before_import(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    initialize_project(source_root, name="source")
    initialize_project(destination_root, name="destination")
    Workspace.initialize(source_root)
    Workspace.initialize(destination_root)
    remote = add_remote("cluster", template="local", project=source_root)
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = str(destination_root)
    (remote / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    payload, job_id = _payload(tmp_path)
    Workspace(source_root).submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination_root, "station", remote="cluster")

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
    arguments = ["transfer", "--job", job_id, "home", "cluster:station"]
    assert command(arguments, context) == 2
    assert Workspace(source_root).find_marker_by_id(job_id) is None
    monkeypatch.setattr(transfer_cli, "run_adapter", real_run_adapter)
    monkeypatch.setattr(workflow_common, "run_adapter", real_run_adapter)
    assert command(arguments, context) == 0
    assert Workspace(destination_root).find_marker_by_id(job_id) is not None
