import bz2
import json
import os
import re
import socket
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace
from httk.workflow.adapters import add_remote, queue_settings, run_adapter
from httk.workflow.manifests import (
    MAINTENANCE_LOCK_FILE,
    MAINTENANCE_LOCK_MAX_AGE_SECONDS,
    create_manifest,
    read_maintenance_lock,
    workspace_maintenance_guard,
)
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command


def _project(tmp_path: Path, monkeypatch, name: str = "locking") -> Path:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    project = tmp_path / name
    initialize_project(project, name=name)
    return project


def _dead_pid() -> int:
    """Return a reaped process identifier that is certainly not running."""

    process = subprocess.Popen([sys.executable, "-c", ""])
    process.wait()
    return process.pid


def _write_lock(project: Path, value: object) -> Path:
    path = project / ".httk-workflow" / MAINTENANCE_LOCK_FILE
    path.write_text(value if isinstance(value, str) else json.dumps(value), encoding="utf-8")
    return path


def _lock_json(**overrides: object) -> dict[str, object]:
    created = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    record: dict[str, object] = {
        "pid": _dead_pid(),
        "hostname": socket.gethostname(),
        "created": created,
    }
    record.update(overrides)
    return record


def test_guard_records_json_and_removes_it(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch)
    workspace = Workspace(project)
    path = project / ".httk-workflow" / MAINTENANCE_LOCK_FILE
    with workspace_maintenance_guard(workspace):
        holder = read_maintenance_lock(workspace)
        assert holder is not None
        assert holder.pid == os.getpid() and holder.hostname == socket.gethostname()
        assert holder.created is not None and not holder.is_stale()
        assert path.stat().st_mode & 0o777 == 0o600
    assert not path.exists()
    assert read_maintenance_lock(workspace) is None


@pytest.mark.parametrize(
    "content",
    [
        _lock_json,
        lambda: "12345\n",
        lambda: "not json at all",
        lambda: _lock_json(pid=os.getpid(), created="2000-01-01T00:00:00.000000Z"),
    ],
    ids=["dead-pid", "legacy-pid", "unreadable", "expired"],
)
def test_stale_lock_is_reclaimed_by_the_guard(tmp_path: Path, monkeypatch, content) -> None:
    project = _project(tmp_path, monkeypatch)
    workspace = Workspace(project)
    path = _write_lock(project, content())
    with workspace_maintenance_guard(workspace):
        holder = read_maintenance_lock(workspace)
        assert holder is not None and holder.pid == os.getpid()
    assert not path.exists()


def test_live_lock_refuses_with_holder_information(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch)
    workspace = Workspace(project)
    path = _write_lock(project, _lock_json(pid=os.getpid()))
    expected = re.escape(f"pid {os.getpid()} on host {socket.gethostname()}")
    with pytest.raises(ValueError, match=expected), workspace_maintenance_guard(workspace):
        pass
    assert path.is_file()
    with pytest.raises(ValueError, match="maintenance"):
        create_manifest(project)
    assert path.is_file()


def test_lock_age_bound_is_one_day(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch)
    workspace = Workspace(project)
    assert MAINTENANCE_LOCK_MAX_AGE_SECONDS == 24 * 60 * 60
    _write_lock(project, _lock_json(pid=os.getpid(), hostname="another-host.example.test"))
    holder = read_maintenance_lock(workspace)
    assert holder is not None and not holder.local
    assert holder.age_seconds is not None
    assert holder.is_stale(max_age_seconds=0.0) and not holder.is_stale(max_age_seconds=holder.age_seconds + 60)


def test_workspace_unlock_clears_stale_and_needs_force_for_live(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch)
    context = CLIContext("httk", project)
    ws = register_ws(context, project)
    assert command(["workspace", "unlock", ws], context) == 0
    assert "no maintenance lock" in capsys.readouterr().out

    path = _write_lock(project, _lock_json())
    assert command(["workspace", "unlock", ws], context) == 0
    assert "removed stale maintenance lock" in capsys.readouterr().out
    assert not path.exists()

    path = _write_lock(project, _lock_json(pid=os.getpid()))
    assert command(["workspace", "unlock", ws], context) == 2
    captured = capsys.readouterr()
    assert f"pid {os.getpid()}" in captured.err and "--force" in captured.err
    assert path.is_file()

    assert command(["workspace", "unlock", ws, "--force"], context) == 0
    assert "removed live maintenance lock" in capsys.readouterr().out
    assert not path.exists()


def _configured(project: Path, *settings: str) -> tuple[Path, int]:
    remote = add_remote("cluster", template="local", project=project)
    code = command(
        ["remote", "configure", "cluster", *[argument for value in settings for argument in ("--set", value)]],
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
    assert metadata["queues"]["default"] == {}

    credentials = remote / "credentials.json"
    assert json.loads(credentials.read_text(encoding="utf-8")) == {"default": {"password": "hunter2", "token": "abc"}}
    assert credentials.stat().st_mode & 0o777 == 0o600

    manifest = create_manifest(project)
    body = bz2.decompress(manifest.read_bytes()).decode("utf-8")
    relative = credentials.relative_to(project).as_posix()
    assert relative == ".httk-project/remotes/cluster/credentials.json"
    assert relative not in body and "hunter2" not in body
    assert ".httk-project/remotes/cluster/remote.json" in body


def test_secret_setting_remains_visible_to_the_adapter(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path, monkeypatch, name="visible")
    remote, code = _configured(project, "password=hunter2", "host=login.example.test")
    assert code == 0
    assert queue_settings(remote, "default") == {
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
print(json.dumps({{"format":"httk-computer-result","format_version":1,
                  "operation":request["operation"],"ok":True}}))
""",
        encoding="utf-8",
    )
    adapter.chmod(0o755)
    run_adapter(remote, "invoke", {"queue": "default", "argv": ["true"]})
    assert json.loads(echo.read_text(encoding="utf-8"))["queue_settings"]["password"] == "hunter2"


def test_whitelisted_setting_still_lands_in_remote_json(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="whitelisted")
    destination = tmp_path / "elsewhere"
    remote, code = _configured(project, f"workspace={destination}", "username=someone")
    assert code == 0
    assert "credentials.json" not in capsys.readouterr().err
    metadata = json.loads((remote / "remote.json").read_text(encoding="utf-8"))
    assert metadata["queues"]["default"] == {"workspace": str(destination), "username": "someone"}
    assert not (remote / "credentials.json").exists()
