import bz2
import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.project.manifests import create_manifest

from httk.workflow import Workspace
from httk.workflow._kernel import register_owner
from httk.workflow.adapters import add_remote, remote_settings, run_adapter
from httk.workflow.manifests import workspace_maintenance_guard
from httk.workflow.projects import PROJECT_DIRECTORY, initialize_project
from httk.workflow.workflow_cli import command


def _init_workspace(project: Path) -> Workspace:
    """Create ``project/workspace``, register it as ``default``, and record it as the project default."""

    from httk.workflow.projects import write_project_section
    from httk.workflow.registry import create_workspace

    create_workspace("default", project / "workspace")
    write_project_section(project, "workspace", {"default": "default"})
    return Workspace(project / "workspace")


def _project(tmp_path: Path, monkeypatch, name: str = "locking") -> Path:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    project = tmp_path / name
    initialize_project(project, name=name)
    _init_workspace(project)
    return project


def test_manifest_guard_refuses_while_an_owner_is_not_proven_dead(tmp_path: Path, monkeypatch) -> None:
    # The maintenance lock is gone: the guard is a read-only check that no owner is alive.
    project = _project(tmp_path, monkeypatch)
    workspace = Workspace(project / "workspace")
    with workspace_maintenance_guard(workspace):
        pass
    with register_owner(workspace, kind="cli", label="test", allocation=None, advertised={}) as owner:
        with pytest.raises(ValueError, match=owner.owner_id), workspace_maintenance_guard(workspace):
            pass
        with pytest.raises(ValueError, match="quiescent"):
            create_manifest(project)
    assert not (project / "workspace" / ".httk-workspace" / "maintenance.lock").exists()


def _configured(project: Path, *settings: str) -> tuple[Path, int]:
    remote = add_remote("cluster", template="local", project=project)
    code = command(
        ["remote", "configure", *[argument for value in settings for argument in ("--set", value)], "cluster"],
        CLIContext("httk", project),
    )
    return remote, code


def test_secret_setting_avoids_remote_json_and_manifests(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="secrets")
    remote, code = _configured(project, "password=hunter2", "token=abc")
    assert code == 0
    notice = capsys.readouterr().err
    assert "password, token" in notice and "credentials.json" in notice and "manifest" in notice

    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    assert "password" not in json.dumps(metadata) and "hunter2" not in json.dumps(metadata)
    assert metadata["settings"] == {}

    credentials = remote / "credentials.json"
    assert json.loads(credentials.read_text(encoding="utf-8")) == {"password": "hunter2", "token": "abc"}
    assert credentials.stat().st_mode & 0o777 == 0o600

    manifest = create_manifest(project)
    body = bz2.decompress(manifest.read_bytes()).decode("utf-8")
    relative = credentials.relative_to(project).as_posix()
    assert relative == f"{PROJECT_DIRECTORY}/remotes/cluster/credentials.json"
    assert relative not in body and "hunter2" not in body
    assert f"{PROJECT_DIRECTORY}/remotes/cluster/remote.json" in body


def test_scheduler_setting_is_refused_for_remote_configuration(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="scheduler-settings")
    remote = add_remote("cluster", template="local", project=project)

    code = command(["remote", "configure", "--set", "partition=x", "cluster"], CLIContext("httk", project))

    assert code == 1
    assert "unknown remote setting 'partition'" in capsys.readouterr().err
    assert json.loads((remote / "remote.json").read_text(encoding="utf-8"))["settings"] == {}
    assert not (remote / "credentials.json").exists()


def test_retired_bootstrap_setting_is_refused_as_unknown(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="bootstrap-retired")
    remote = add_remote("cluster", template="local", project=project)

    code = command(["remote", "configure", "--set", "bootstrap=pip", "cluster"], CLIContext("httk", project))

    assert code == 1
    assert "unknown remote setting 'bootstrap'" in capsys.readouterr().err
    assert json.loads((remote / "remote.json").read_text(encoding="utf-8"))["settings"] == {}
    assert not (remote / "credentials.json").exists()


def test_stale_project_anchor_secrets_stay_out_of_manifest(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="stale-anchor")
    stale = project / ".httk-project"
    (stale / "keys").mkdir(parents=True)
    (stale / "keys" / "project.seed").write_text("fake-seed\n", encoding="ascii")
    credentials = stale / "remotes" / "cluster" / "credentials.json"
    credentials.parent.mkdir(parents=True)
    credentials.write_text('{"default":{"password":"secret"}}\n', encoding="utf-8")

    body = bz2.decompress(create_manifest(project).read_bytes()).decode("utf-8")

    assert '"path":".httk-project/keys/project.seed"' not in body
    assert '"path":".httk-project/remotes/cluster/credentials.json"' not in body


def test_secret_setting_remains_visible_to_the_adapter(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="visible")
    remote, code = _configured(project, "password=hunter2", "host=login.example.test")
    assert code == 0
    assert remote_settings(remote) == {
        "host": "login.example.test",
        "password": "hunter2",
    }
    echo = tmp_path / "request.json"
    adapter = remote / "adapter"
    adapter.write_text(
        f"""#!/usr/bin/env python3
import json, shutil, sys
request = json.load(open(sys.argv[1]))
shutil.copyfile(sys.argv[1], {str(echo)!r})
print(json.dumps({{"format":"httk-computer-result","format_version":2,
                  "operation":request["operation"],"ok":True}}))
""",
        encoding="utf-8",
    )
    adapter.chmod(0o755)
    run_adapter(remote, "invoke", {"remote_settings": {}, "argv": ["true"]})
    assert json.loads(echo.read_text(encoding="utf-8"))["remote_settings"]["password"] == "hunter2"


def test_retired_workspace_root_setting_is_refused_as_unknown(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="whitelisted")
    destination = tmp_path / "elsewhere"
    _remote, code = _configured(project, f"workspace_root={destination}", "username=someone")
    assert code == 1
    assert "unknown remote setting 'workspace_root'" in capsys.readouterr().err


def test_remote_unset_removes_settings_and_credentials(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="unset")
    remote, code = _configured(project, "host=old", "password=secret", "token=retained")
    assert code == 0
    context = CLIContext("httk", project)
    configure = ["remote", "configure", "cluster"]
    assert command([*configure, "--unset", "host", "--unset", "password", "--unset", "absent"], context) == 0
    assert remote_settings(remote) == {"token": "retained"}
    assert json.loads((remote / "remote.json").read_text())["settings"] == {}
    assert json.loads((remote / "credentials.json").read_text()) == {"token": "retained"}
    assert (remote / "credentials.json").stat().st_mode & 0o777 == 0o600
    assert command([*configure, "--unset", "token", "--set", "token=replaced"], context) == 0
    assert remote_settings(remote) == {"token": "replaced"}
    before = (remote / "remote.json").read_bytes(), (remote / "credentials.json").read_bytes()
    assert command([*configure, "--unset", "token=wrong", "--set", "host=new"], context) == 1
    assert ((remote / "remote.json").read_bytes(), (remote / "credentials.json").read_bytes()) == before
    capsys.readouterr()
    assert command(["remote", "list", "--json"], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["name"] == "cluster"
    assert command(["remote", "list"], context) == 0
    assert capsys.readouterr().out.startswith("cluster\tproject\t")


def test_remote_unset_validates_candidate_before_persisting(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="unset-validation")
    remote, code = _configured(project, "host=required", "password=retained")
    assert code == 0
    adapter = remote / "adapter"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "request = json.load(open(sys.argv[1]))\n"
        "settings = {**request['remote_settings'], **request['settings']}\n"
        "print(json.dumps({'format':'httk-computer-result','format_version':2,"
        "'operation':request['operation'],'ok':'host' in settings,'error':'host is required'}))\n"
    )
    before = (remote / "remote.json").read_bytes(), (remote / "credentials.json").read_bytes()
    assert (
        command(
            ["remote", "configure", "cluster", "--unset", "host", "--unset", "password", "--set", "token=new"],
            CLIContext("httk", project),
        )
        == 1
    )
    assert "host is required" in capsys.readouterr().err
    assert ((remote / "remote.json").read_bytes(), (remote / "credentials.json").read_bytes()) == before


def test_remote_check_honors_temporary_settings(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="check-overrides")
    remote = add_remote("cluster", template="local", project=project)
    assert (
        command(["remote", "check", "cluster", "--set", "httk_command=/missing/httk"], CLIContext("httk", project)) == 1
    )
    assert "httk" in capsys.readouterr().err
    assert remote_settings(remote) == {}
