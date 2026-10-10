"""Remote error output and the ssh adapter's opt-in interactive authentication."""

import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import adapter_runtime, adapters
from httk.workflow.adapters import add_remote, run_adapter, split_settings
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command


def test_failed_remote_check_prints_no_empty_json_array(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    project = tmp_path / "project"
    initialize_project(project, name="stray")

    assert command(["remote", "check", "nope"], CLIContext("httk", project)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "remote nope:" in captured.err


def _transport(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, terminal: bool, **settings: object) -> list[str]:
    monkeypatch.setattr(adapter_runtime, "_terminal_present", lambda: terminal)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    return adapter_runtime._ssh_transport(settings)


def test_default_transport_is_batch_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=True)
    assert "BatchMode=yes" in argv
    assert not any(item.startswith("Control") for item in argv)


def test_interactive_yes_shares_a_private_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="yes")
    assert "BatchMode=yes" not in argv
    assert "NumberOfPasswordPrompts=2" in argv
    assert {"ControlMaster=auto", "ControlPersist=600", f"ControlPath={tmp_path}/httk/%C"} <= set(argv)
    assert ((tmp_path / "httk").stat().st_mode & 0o777) == 0o700


def test_interactive_per_call_prompts_without_sharing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="per-call")
    assert "BatchMode=yes" not in argv
    assert "NumberOfPasswordPrompts=2" in argv
    assert not any(item.startswith("Control") for item in argv)


def test_interactive_without_a_terminal_keeps_batch_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    default = _transport(monkeypatch, tmp_path, terminal=False)
    assert _transport(monkeypatch, tmp_path, terminal=False, interactive_auth="yes") == default
    assert "BatchMode=yes" in default
    assert "NumberOfPasswordPrompts=2" not in default


def test_unknown_interactive_mode_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="interactive_auth"):
        _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="maybe")


def test_interactive_auth_is_persisted_not_a_credential() -> None:
    persistable, credentials = split_settings({"interactive_auth": "yes"})
    assert persistable == {"interactive_auth": "yes"}
    assert credentials == {}


_DENIED = "someone@fake.example.test: Permission denied (keyboard-interactive)."


@contextmanager
def _no_terminal() -> Iterator[None]:
    """Put /dev/null on fd 0 so the adapter never sees a terminal, even under ``pytest -s``."""

    saved = os.dup(0)
    null = os.open(os.devnull, os.O_RDONLY)
    try:
        os.dup2(null, 0)
        yield
    finally:
        os.dup2(saved, 0)
        os.close(saved)
        os.close(null)


def _denied_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **settings: str) -> str:
    project = tmp_path / "project"
    initialize_project(project, name="denied")
    bundle = fake_remote(project, **settings)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", _DENIED)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    with _no_terminal(), pytest.raises(RuntimeError) as raised:
        run_adapter(bundle, "install", {"remote_settings": {}})
    return str(raised.value)


def test_denied_login_hints_at_interactive_auth(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    message = _denied_install(tmp_path, monkeypatch)
    assert "Permission denied" in message
    assert "interactive_auth=yes" in message


def test_denied_login_without_a_terminal_says_so(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert "no terminal is attached" in _denied_install(tmp_path, monkeypatch, interactive_auth="yes")


def test_hint_only_for_ssh_own_failure_status() -> None:
    def hint(returncode: int) -> str:
        return adapter_runtime._login_hint({}, subprocess.CompletedProcess([], returncode, "", _DENIED))

    assert hint(128) == ""  # a remote command forwarding its own ssh denial
    assert "interactive_auth=yes" in hint(255)


def test_non_interactive_adapter_gets_no_stdin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    project = tmp_path / "project"
    initialize_project(project, name="stdin")
    bundle = add_remote("cluster", template="ssh", project=project)
    seen: list[object] = []

    def capture(*args: object, **kwargs: object) -> object:
        seen.append(kwargs.get("stdin"))
        raise LookupError("captured")

    monkeypatch.setattr(adapters.subprocess, "run", capture)
    for interactive in (True, False):
        with pytest.raises(LookupError):
            run_adapter(bundle, "status", {"argv": ["true"]}, interactive=interactive)
    assert seen == [None, subprocess.DEVNULL]


def test_monitor_manager_start_never_prompts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.monitor import actions
    from httk.workflow.monitor.data import WorkspaceView
    from httk.workflow.registry import WorkspaceBinding
    from httk.workflow.workflow_cli import _common

    seen: list[object] = []

    def capture(*_args: object, **kwargs: object) -> dict[str, object]:
        seen.append(kwargs.get("interactive"))
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(_common, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle="unused"))
    monkeypatch.setattr(_common, "run_adapter", capture)
    view = WorkspaceView(WorkspaceBinding("cluster:station", "cluster", None), CLIContext("httk", tmp_path))
    assert actions.start_managers(view, 1) == "started 1 manager(s)"
    assert seen == [False]
