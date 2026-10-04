"""Prove the daemon entry bypasses mutable workflow discovery."""

import base64
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import _daemon_cli, _daemon_setup, workflow_cli
from httk.workflow._daemon_policy import Policy, Profile, policy_document

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def _snapshot(tmp_path: Path) -> Path:
    runtime = tmp_path / "runtime"
    policy = Policy(
        workspace=tmp_path / "site/workspace",
        workspace_id="12345678-1234-1234-1234-123456789abc",
        enrollment_id="1" * 32,
        exchange=tmp_path / "site/exchange",
        state=tmp_path / "state",
        snapshots=tmp_path / "snapshots",
        bwrap=runtime / "bwrap",
        python=runtime / "python",
        sbatch=runtime / "sbatch",
        squeue=runtime / "squeue",
        scancel=runtime / "scancel",
        cluster="cluster",
        readonly_paths=(runtime,),
        broker_paths=(),
        profiles=(Profile("small"),),
        authorized_keys=(AUTHORIZED_KEY,),
    )
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(policy_document(policy)), encoding="utf-8")
    return path


def test_daemon_dispatch_does_not_build_ordinary_parser(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("ordinary parser must not run")

    observed: list[object] = []

    def daemon(arguments: object, *, program: str) -> int:
        observed.extend([arguments, program])
        return 0

    monkeypatch.setattr(workflow_cli, "command", forbidden)
    monkeypatch.setattr(_daemon_cli, "command", daemon)
    context = CLIContext(program="httk", cwd=tmp_path)
    assert workflow_cli.workspace_command(["daemon", "/data", "--state", "/state", "--once"], context) == 0
    assert observed == [["/data", "--state", "/state", "--once"], "httk workspace daemon"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["/data/../other"],
        ["data/../other"],
        ["/data", "--state", "../link/../state"],
        ["/data", "--check", "--once"],
        ["/data", "--command", "rm"],
        ["/data", "--policy", "/policy"],
        ["/data", "--export-endpoint"],
    ],
)
def test_daemon_refuses_ambiguous_paths_and_execution_options(arguments: list[str]) -> None:
    assert _daemon_cli.command(arguments, program="httk workspace daemon") == 2


def test_relative_paths_anchor_to_the_physical_cwd_without_resolving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cwd = tmp_path / "site" / "data"
    cwd.mkdir(parents=True)
    calls: list[tuple[Path, Path, Path | None]] = []

    def initialize(workspace: Path, *, exchange: Path, state: Path | None, **_options: object) -> Path:
        calls.append((workspace, exchange, state))
        return _snapshot(tmp_path)

    monkeypatch.setattr(_daemon_setup, "initialize", initialize)
    monkeypatch.chdir(cwd)
    setup = ["--launcher", "small", "--authorize", AUTHORIZED_KEY, "--initialize"]
    assert (
        _daemon_cli.command([".", "--exchange", "../exchange", "--state", "../../state", *setup], program="httk") == 0
    )
    assert _daemon_cli.command(["link/x", "--exchange", "./exchange", *setup], program="httk") == 0
    assert calls == [
        (cwd, tmp_path / "site" / "exchange", tmp_path / "state"),
        (cwd / "link" / "x", cwd / "exchange", None),
    ]


def test_isolated_handoff_cleans_environment_cwd_and_inherited_descriptors(tmp_path: Path) -> None:
    installed = tmp_path / "installed"
    installed.mkdir()
    untrusted = tmp_path / "untrusted"
    untrusted.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    trap = tmp_path / "trap"
    (untrusted / "sitecustomize.py").write_text(f"open({str(trap)!r},'w').write('executed')\n")
    (installed / "_daemon_bootstrap.py").write_text(
        "import json,os,sys\n"
        "targets=[]\n"
        "for fd in os.listdir('/proc/self/fd'):\n"
        " try: targets.append(os.readlink('/proc/self/fd/'+fd))\n"
        " except FileNotFoundError: pass\n"
        "print(json.dumps({'cwd':os.getcwd(),'env':dict(os.environ),'isolated':sys.flags.isolated,"
        "'no_site':sys.flags.no_site,'targets':targets,'httk_loaded':'httk.workflow' in sys.modules,'argv':sys.argv}))\n"
    )
    script = f"""
import os
from httk.workflow import _daemon_cli
_daemon_cli.__file__ = {str(installed / '_daemon_cli.py')!r}
from httk.workflow import _daemon_setup
_daemon_setup.active_policy_path = lambda workspace, **_: __import__('pathlib').Path('/active/runtime.json')
fd = os.open({str(outside)!r}, os.O_RDONLY)
os.set_inheritable(fd, True)
os.environ.update(PYTHONPATH={str(untrusted)!r}, BASH_ENV='untrusted', SSH_AUTH_SOCK='untrusted')
os.chdir({str(untrusted)!r})
raise SystemExit(_daemon_cli.command([{str(workspace)!r},'--once'], program='httk workspace daemon'))
"""
    result = subprocess.run(
        [sys.executable, "-c", script], cwd="/", capture_output=True, text=True, check=True, timeout=20
    )
    observed = json.loads(result.stdout)
    assert observed["cwd"] == "/"
    assert observed["isolated"] == 1 and observed["no_site"] == 1
    assert not observed["httk_loaded"]
    assert str(outside) not in observed["targets"]
    assert not trap.exists()
    assert "PYTHONPATH" not in observed["env"] and "BASH_ENV" not in observed["env"]
    assert "SSH_AUTH_SOCK" not in observed["env"]
    assert observed["argv"][1:] == [
        "--mode",
        "broker",
        "--workspace",
        str(workspace),
        "--policy",
        "/active/runtime.json",
        "--once",
    ]


@pytest.mark.parametrize("mode", ["initialize", "reload"])
def test_local_setup_modes_print_approved_launchers_and_keys_without_exec(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, object]] = []

    def setup(workspace: Path, **options: object) -> Path:
        calls.append({"workspace": workspace, **options})
        return _snapshot(tmp_path)

    monkeypatch.setattr(_daemon_setup, mode, setup)
    monkeypatch.setattr(_daemon_cli.os, "execve", lambda *_args: pytest.fail("local setup must not exec"))
    arguments = ["/workspace", f"--{mode}", "--launcher", "small", "--launcher", "large", "--authorize", AUTHORIZED_KEY]
    if mode == "initialize":
        arguments += ["--exchange", "/exchange"]
    assert _daemon_cli.command(arguments, program="httk") == 0
    assert _daemon_cli.command([*arguments, "--force"], program="httk") == 0
    expected: dict[str, object] = {
        "workspace": Path("/workspace"),
        "launchers": ["small", "large"],
        "authorized_keys": [AUTHORIZED_KEY],
        "state": None,
        "snapshots": None,
    }
    if mode == "initialize":
        expected["exchange"] = Path("/exchange")
    assert calls == [{**expected, "force": False}, {**expected, "force": True}]
    assert capsys.readouterr().out == f"launcher small\nauthorized {AUTHORIZED_KEY}\n" * 2


def test_reload_without_lists_keeps_the_stored_launchers_and_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, object]] = []

    def reload(_workspace: Path, **options: object) -> Path:
        calls.append(options)
        return _snapshot(tmp_path)

    monkeypatch.setattr(_daemon_setup, "reload", reload)
    assert _daemon_cli.command(["/workspace", "--reload", "--state", "/state"], program="httk") == 0
    assert calls == [
        {"launchers": None, "authorized_keys": None, "state": Path("/state"), "snapshots": None, "force": False}
    ]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--once", "--force"], "--force applies only"),
        (["--check", "--force"], "--force applies only"),
        (["--force"], "--force applies only"),
        (["--initialize", "--launcher", "a", "--authorize", "k"], "--initialize requires --exchange"),
        (["--initialize", "--exchange", "/x", "--authorize", "k"], "--initialize requires"),
        (["--initialize", "--exchange", "/x", "--launcher", "a"], "--initialize requires"),
        (["--reload", "--exchange", "/x"], "--reload cannot change --exchange"),
        (["--once", "--exchange", "/x"], "apply only to --initialize and --reload"),
        (["--check", "--launcher", "a"], "apply only to --initialize and --reload"),
        (["--authorize", "k"], "apply only to --initialize and --reload"),
    ],
)
def test_cli_argument_matrix_is_refused_before_any_setup(
    arguments: list[str], message: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in ("initialize", "reload", "active_policy_path"):
        monkeypatch.setattr(_daemon_setup, name, lambda *_args, **_kwargs: pytest.fail("must refuse before setup"))
    monkeypatch.setattr(_daemon_cli.os, "execve", lambda *_args: pytest.fail("must refuse before any handoff"))
    assert _daemon_cli.command(["/workspace", *arguments], program="httk") == 2
    assert message in capsys.readouterr().err


def test_run_modes_hand_state_and_snapshots_to_the_active_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[object] = []

    def active(workspace: Path, **options: object) -> Path:
        observed.append((workspace, options))
        raise ValueError("inspection stop")

    monkeypatch.setattr(_daemon_setup, "active_policy_path", active)
    arguments = ["/workspace", "--once", "--state", "/state", "--snapshots", "/snapshots"]
    assert _daemon_cli.command(arguments, program="httk") == 2
    assert observed == [(Path("/workspace"), {"state": Path("/state"), "snapshots": Path("/snapshots")})]


def test_incompatible_ledger_is_a_clean_local_refusal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def incompatible(_workspace: Path, **_options: object) -> Path:
        raise sqlite3.DatabaseError("preserve this state and initialize a new enrollment")

    monkeypatch.setattr(_daemon_setup, "reload", incompatible)
    assert _daemon_cli.command(["/workspace", "--reload"], program="httk") == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "preserve this state" in captured.err
