import bz2
import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import identity_key_paths, read_identity_config
from httk.core.project.manifests import create_manifest

from httk.workflow import TaskManager, Workspace
from httk.workflow.adapters import import_v1_remote, run_adapter
from httk.workflow.configuration import read_config
from httk.workflow.manifests import verify_manifest, workspace_maintenance_guard
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command
from v3_helpers import cli_owner, find, install, run, state_of, submit
from v3_helpers import workspace as v3_workspace


def test_all_command_groups_have_help(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    for group in ("workspace", "job", "manager", "config", "remote"):
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
    Workspace.initialize(project / "workspace")
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
    workspace = Workspace.initialize(project / "workspace")
    with cli_owner(workspace) as owner:
        with pytest.raises(ValueError, match="quiescent"):
            create_manifest(project)
        assert owner.owner_id in str(_guard_error(workspace))
    assert create_manifest(project).is_file()


def _guard_error(workspace: Workspace) -> ValueError:
    with pytest.raises(ValueError) as caught, workspace_maintenance_guard(workspace):
        pass
    return caught.value


def test_maintenance_guard_refuses_while_a_manager_runs(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        assert "quiescent workspace" in str(_guard_error(workspace)) and manager.manager_id in str(
            _guard_error(workspace)
        )
    with workspace_maintenance_guard(workspace):
        pass


def test_oversized_attempt_context_fails_as_protocol_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A context too large for one environment value leaves failure evidence."""

    workspace = v3_workspace(tmp_path / "workspace")
    ref = submit(workspace, install(workspace, tmp_path / "package"), {"start": "succeed"})
    monkeypatch.setattr(workspace, "read_settings", lambda: {"huge": "x" * 100_000})
    run(workspace)
    failed = find(workspace, ref.job_id)
    assert failed.state == "failed"
    doc = state_of(failed)
    assert doc.failure is not None and doc.failure["code"] == "protocol_error"


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
