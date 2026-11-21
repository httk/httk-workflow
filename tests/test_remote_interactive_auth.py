"""Remote error output and the ssh adapter's opt-in interactive authentication and connection reuse."""

import json
import os
import subprocess
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import adapter_runtime, adapters
from httk.workflow.adapters import add_remote, resolve_remote, run_adapter, split_settings
from httk.workflow.configuration import config_home, configure_config, read_config, set_config_key
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer, command


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


def _transport(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    terminal: bool,
    controlpath: str = "none",
    share_connection: bool = False,
    **settings: object,
) -> list[str]:
    monkeypatch.setattr(adapter_runtime, "_terminal_present", lambda: terminal)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    calls: list[list[str]] = []

    def ssh_config(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, f"user u\ncontrolpath {controlpath}\nport 22\n", "")

    monkeypatch.setattr(adapter_runtime.subprocess, "run", ssh_config)
    argv = adapter_runtime._ssh_transport({"host": "h.example.test", **settings}, share_connection=share_connection)
    # Only a sharing call asks ssh what the user's configuration resolves.
    reuse = settings.get("connection_reuse")
    assert all(call[-3:] == ["-G", "--", "h.example.test"] for call in calls)
    assert bool(calls) == (share_connection if reuse is None else reuse == "yes")
    return argv


def test_default_transport_is_batch_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=True)
    assert "BatchMode=yes" in argv
    assert not any(item.startswith("Control") for item in argv)


def test_interactive_yes_prompts_without_sharing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="yes")
    assert "BatchMode=yes" not in argv
    assert "NumberOfPasswordPrompts=2" in argv
    assert not any(item.startswith("Control") for item in argv)


def test_connection_reuse_shares_a_private_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="yes", connection_reuse="yes")
    assert "BatchMode=yes" not in argv
    assert "NumberOfPasswordPrompts=2" in argv
    assert {"ControlMaster=auto", "ControlPersist=600", f"ControlPath={tmp_path}/httk/%C"} <= set(argv)
    assert ((tmp_path / "httk").stat().st_mode & 0o777) == 0o700
    # Reuse does not need interactive_auth: a key-based batch login shares too.
    batch = _transport(monkeypatch, tmp_path, terminal=True, connection_reuse="yes")
    assert "BatchMode=yes" in batch
    assert "ControlMaster=auto" in batch


def test_connection_reuse_honors_the_users_configured_control_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    argv = _transport(
        monkeypatch,
        tmp_path,
        terminal=True,
        controlpath="/home/u/.ssh/cm-%C",
        interactive_auth="yes",
        connection_reuse="yes",
    )
    assert "NumberOfPasswordPrompts=2" in argv
    assert not any(item.startswith("Control") for item in argv)


def test_interactive_without_a_terminal_keeps_batch_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    default = _transport(monkeypatch, tmp_path, terminal=False)
    assert "BatchMode=yes" in default
    assert "NumberOfPasswordPrompts=2" not in default
    # Without a terminal, connection reuse still joins the connection a foreground command opened.
    shared = _transport(monkeypatch, tmp_path, terminal=False, interactive_auth="yes", connection_reuse="yes")
    assert shared[: len(default)] == default
    assert {"ControlMaster=auto", "ControlPersist=600", f"ControlPath={tmp_path}/httk/%C"} <= set(shared)


def test_polling_caller_shares_a_batch_mode_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    argv = _transport(monkeypatch, tmp_path, terminal=False, share_connection=True)
    assert "BatchMode=yes" in argv
    assert {"ControlMaster=auto", "ControlPersist=600", f"ControlPath={tmp_path}/httk/%C"} <= set(argv)
    # A dead master must fail fast rather than stall every shared call.
    assert {"ServerAliveInterval=15", "ServerAliveCountMax=3"} <= set(argv)
    configured = _transport(
        monkeypatch, tmp_path, terminal=False, controlpath="/home/u/.ssh/cm-%C", share_connection=True
    )
    assert not any(item.startswith("Control") for item in configured)


def test_connection_reuse_no_never_shares(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for terminal in (True, False):
        argv = _transport(
            monkeypatch,
            tmp_path,
            terminal=terminal,
            share_connection=True,
            interactive_auth="yes",
            connection_reuse="no",
        )
        assert not any(item.startswith("Control") for item in argv)


def test_malformed_modes_are_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="interactive_auth must be yes or no"):
        _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="maybe")
    with pytest.raises(ValueError, match="connection_reuse must be yes or no"):
        _transport(monkeypatch, tmp_path, terminal=True, connection_reuse="maybe")
    # The retired per-call value names its replacement.
    with pytest.raises(ValueError, match="was split in two"):
        _transport(monkeypatch, tmp_path, terminal=True, interactive_auth="per-call")


def test_interactive_auth_and_connection_reuse_are_persisted_not_credentials() -> None:
    persistable, credentials = split_settings({"interactive_auth": "yes", "connection_reuse": "yes"})
    assert persistable == {"interactive_auth": "yes", "connection_reuse": "yes"}
    assert credentials == {}


def _requested_settings(
    bundle: Path, request: dict[str, object] | None = None, *, unset_settings: Sequence[str] = ()
) -> dict[str, object]:
    """Return the ``remote_settings`` one adapter request file would carry."""

    path = adapters._request_file(bundle, "status", request or {}, unset_settings)
    try:
        return json.loads(path.read_text(encoding="utf-8"))["remote_settings"]
    finally:
        path.unlink()


def test_user_configuration_supplies_the_connection_reuse_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The autouse fixture isolates the per-user configuration (HTTK_CONFIG_HOME).
    project = tmp_path / "project"
    initialize_project(project, name="reuse")
    bundle = add_remote("cluster", template="ssh", project=project)
    assert "connection_reuse" not in _requested_settings(bundle)

    set_config_key("connection_reuse", "YES")
    assert read_config()["connection_reuse"] == "yes"
    assert _requested_settings(bundle)["connection_reuse"] == "yes"
    # A pending --set value (which the adapter lays over remote_settings) is not shadowed by the default.
    assert "connection_reuse" not in _requested_settings(bundle, {"settings": {"connection_reuse": "no"}})
    adapters.write_json_atomic(
        bundle / "remote.json",
        {**adapters.read_metadata(bundle), "settings": {"connection_reuse": "no"}},
    )
    assert _requested_settings(bundle)["connection_reuse"] == "no"
    assert _requested_settings(bundle, unset_settings=["connection_reuse"])["connection_reuse"] == "yes"

    with pytest.raises(ValueError, match="must be one of yes, no"):
        set_config_key("connection_reuse", "sometimes")
    with pytest.raises(ValueError, match="applies to name lists"):
        configure_config((("add", "connection_reuse=yes"),))
    configure_config((("unset", "connection_reuse"),))
    assert "connection_reuse" not in read_config()
    assert _requested_settings(bundle, unset_settings=["connection_reuse"]) == {}


def test_configure_refuses_a_malformed_yes_no_setting() -> None:
    for key in ("interactive_auth", "connection_reuse"):
        request = {"settings": {key: "maybe", "check_connectivity": "no"}}
        with pytest.raises(ValueError, match=f"{key} must be yes or no"):
            adapter_runtime._configure("ssh", request)


def test_configure_notices_connection_reuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="notice")
    add_remote("cluster", template="ssh", project=project)
    monkeypatch.setattr(
        _transfer, "run_adapter", lambda *_args, **_kwargs: {"ok": True, "operation": "configure", "returncode": 0}
    )
    context = CLIContext("httk", project)

    def configure(*settings: str) -> str:
        assert command(["remote", "set", "cluster", "host=h.example.test", *settings], context) == 0
        return capsys.readouterr().err

    notice = configure()
    assert "httk remote set cluster connection_reuse=yes" in notice
    assert "httk config set connection_reuse=yes" in notice
    assert "notice" not in configure("connection_reuse=no")
    # An option between NAME and the required pairs still parses.
    assert command(["remote", "set", "cluster", "--adapter-timeout", "5", "host=h2.example.test"], context) == 0
    # Required pairs on both sides of an option are all collected.
    assert (
        command(["remote", "set", "cluster", "host=h3.example.test", "--adapter-timeout", "5", "port=23"], context) == 0
    )
    stored = adapters.read_metadata(resolve_remote("cluster", project=project).bundle)["settings"]
    assert stored["host"] == "h3.example.test" and stored["port"] == "23"
    capsys.readouterr()
    assert command(["remote", "unset", "cluster", "connection_reuse"], context) == 0
    assert "notice" in capsys.readouterr().err
    assert (
        "connection_reuse" not in adapters.read_metadata(resolve_remote("cluster", project=project).bundle)["settings"]
    )
    set_config_key("connection_reuse", "no")
    assert "notice" not in configure()


def test_remote_add_with_settings_runs_the_configure_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="add-configure")
    calls: list[tuple[str, dict[str, object]]] = []
    timeouts: list[object] = []
    returncode = 0

    def fake_run_adapter(_bundle: Path, operation: str, request: dict[str, object], **kwargs: object) -> dict:
        calls.append((operation, request))
        timeouts.append(kwargs.get("timeout"))
        return {"ok": returncode == 0, "operation": operation, "returncode": returncode}

    monkeypatch.setattr(_transfer, "run_adapter", fake_run_adapter)
    context = CLIContext("httk", project)
    assert command(["remote", "add", "cluster", "--kind", "ssh", "host=h.example.test"], context) == 0
    assert calls == [("configure", {"settings": {"host": "h.example.test"}})]
    bundle = resolve_remote("cluster", project=project).bundle
    metadata = adapters.read_metadata(bundle)
    assert metadata["kind"] == "ssh" and metadata["settings"]["host"] == "h.example.test"
    assert "httk remote set cluster connection_reuse=yes" in capsys.readouterr().err

    returncode = 3
    assert command(["remote", "add", "other", "--kind", "ssh", "host=bad.example.test"], context) == 1
    error = capsys.readouterr().err
    assert "remote other was created" in error and "httk remote set other KEY=VALUE" in error
    assert "host" not in adapters.read_metadata(resolve_remote("other", project=project).bundle).get("settings", {})

    returncode = 0
    assert command(["remote", "add", "third", "--kind", "ssh", "prelude=x"], context) == 2
    error = capsys.readouterr().err
    assert "cli_prelude" in error and "remote third was created" in error
    assert resolve_remote("third", project=project).bundle.is_dir()

    # A stranded word after an option is a pair or a parser error; other leaves keep argparse's refusal.
    assert command(["remote", "add", "fourth", "--kind", "ssh", "host"], context) == 2
    assert "expected KEY=VALUE, not 'host'" in capsys.readouterr().err
    assert command(["remote", "add", "fourth", "--kind", "ssh", "host=x", "--bogus"], context) == 2
    assert "unrecognized arguments: --bogus\n" in capsys.readouterr().err
    assert command(["remote", "list", "extra"], context) == 2
    assert "unrecognized arguments: extra" in capsys.readouterr().err
    assert len(calls) == 2  # neither refused add reached the adapter

    # --global configures the global bundle it created, even where a project remote shadows the name.
    argv = ["remote", "add", "cluster", "--global", "--kind", "ssh", "--adapter-timeout", "5"]
    assert command([*argv, "host=global.example.test", "password=pw"], context) == 0
    shared = config_home() / "remotes" / "cluster"
    assert adapters.read_metadata(shared)["settings"]["host"] == "global.example.test"
    assert adapters.read_credentials(shared) == {"password": "pw"}
    assert adapters.read_metadata(bundle)["settings"]["host"] == "h.example.test"
    assert not (bundle / "credentials.json").exists()
    assert timeouts[-1] == 5.0


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
    assert "connection_reuse=yes" in message


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
        seen.append((kwargs.get("stdin"), bool(kwargs.get("start_new_session"))))
        raise LookupError("captured")

    monkeypatch.setattr(adapters.subprocess, "run", capture)
    for interactive in (True, False):
        with pytest.raises(LookupError):
            run_adapter(bundle, "status", {"argv": ["true"]}, interactive=interactive)
    assert seen == [(None, False), (subprocess.DEVNULL, True)]


def _without_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every monitor session unsupported, so its reads take the per-call fallback."""

    from httk.workflow.monitor import data

    def unsupported(*_args: object, **_kwargs: object) -> tuple[int, str, str]:
        raise adapters.SessionUnsupported("no session here")

    monkeypatch.setattr(data, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle="unused"))
    monkeypatch.setattr(adapters.AdapterSession, "run", unsupported)


def test_monitor_manager_start_never_prompts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.monitor import actions
    from httk.workflow.monitor.data import WorkspaceView
    from httk.workflow.registry import WorkspaceBinding
    from httk.workflow.workflow_cli import _common

    seen: list[object] = []

    def capture(_bundle: object, _operation: str, request: dict[str, object], **kwargs: object) -> dict[str, object]:
        seen.append((kwargs.get("interactive"), request.get("share_connection")))
        return {"returncode": 0, "stdout": "", "stderr": ""}

    _without_session(monkeypatch)
    monkeypatch.setattr(_common, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle="unused"))
    monkeypatch.setattr(_common, "run_adapter", capture)
    view = WorkspaceView(WorkspaceBinding("cluster:station", "cluster", None), CLIContext("httk", tmp_path))
    assert actions.start_managers(view, 1) == "started 1 manager(s)"
    assert seen == [(False, True)]


def test_monitor_reads_ask_for_a_shared_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.workflow.monitor.data import WorkspaceView
    from httk.workflow.registry import WorkspaceBinding
    from httk.workflow.workflow_cli import _common

    requests: list[tuple[dict[str, object], object]] = []

    def capture(_bundle: object, _operation: str, request: dict[str, object], **kwargs: object) -> dict[str, object]:
        requests.append((request, kwargs.get("interactive")))
        return {"returncode": 0, "stdout": '{"jobs": [], "counts": {}}', "stderr": ""}

    _without_session(monkeypatch)
    monkeypatch.setattr(_common, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace(bundle="unused"))
    monkeypatch.setattr(_common, "run_adapter", capture)
    view = WorkspaceView(WorkspaceBinding("cluster:station", "cluster", None), CLIContext("httk", tmp_path))
    view.page()
    view.page()
    assert [(request["share_connection"], interactive) for request, interactive in requests] == [(True, False)] * 2

    shared: list[bool] = []

    def ssh_run(_settings: object, _argv: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        shared.append(bool(kwargs["share_connection"]))
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(adapter_runtime, "_ssh_run", ssh_run)
    adapter_runtime._invoke("ssh", "invoke", {"argv": ["true"], "share_connection": True})
    adapter_runtime._invoke("ssh", "invoke", {"argv": ["true"]})
    assert shared == [True, False]
    with pytest.raises(ValueError, match="share_connection"):
        adapter_runtime._invoke("ssh", "invoke", {"argv": ["true"], "share_connection": "yes"})
    capsys.readouterr()
