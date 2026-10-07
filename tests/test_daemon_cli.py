"""Prove the daemon entry bypasses mutable workflow discovery."""

import argparse
import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import _daemon_cli, _daemon_setup, workflow_cli
from httk.workflow._daemon_policy import ApprovedLauncher, Policy, policy_document
from httk.workflow._daemon_state import LedgerError

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def _snapshot(tmp_path: Path) -> Path:
    runtime = tmp_path / "runtime"
    policy = Policy(
        workspace=tmp_path / "site/workspace",
        workspace_id="12345678-1234-1234-1234-123456789abc",
        enrollment_id="1" * 32,
        state=tmp_path / "state",
        snapshots=tmp_path / "snapshots",
        bwrap=runtime / "bwrap",
        python=runtime / "python",
        sbatch=runtime / "sbatch",
        squeue=runtime / "squeue",
        scancel=runtime / "scancel",
        cluster="cluster",
        launchers=(ApprovedLauncher("small", (("manager.confine", "bwrap"),), "a" * 64),),
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
    assert workflow_cli.workspace_command(["daemon", "run", "/data", "--state", "/state", "--once"], context) == 0
    assert observed == [["run", "/data", "--state", "/state", "--once"], "httk workspace daemon"]


def test_the_workflow_tree_exposes_the_same_daemon_subcommands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    context = CLIContext(program="httk", cwd=tmp_path)
    assert workflow_cli.command(["workspace", "daemon"], context) == 0
    output = capsys.readouterr().out
    assert all(mode in output for mode in ("init", "configure", "show", "check", "run"))
    observed: list[argparse.Namespace] = []

    def launch(arguments: argparse.Namespace) -> int:
        observed.append(arguments)
        return 0

    monkeypatch.setattr(_daemon_cli, "launch", launch)
    arguments = ["workspace", "daemon", "configure", "/data", "--add", "launchers=a", "--set", "force=true"]
    assert workflow_cli.command(arguments, context) == 0
    assert observed[0].daemon_mode == "configure"
    assert observed[0].changes == [("add", "launchers=a"), ("set", "force=true")]


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "/data/../other"],
        ["run", "data/../other"],
        ["run", "/data", "--state", "../link/../state"],
        ["check", "/data", "--once"],
        ["run", "/data", "--command", "rm"],
        ["run", "/data", "--policy", "/policy"],
        ["run", "/data", "--export-endpoint"],
        ["/data", "--once"],
        ["/data", "--reload"],
        ["/data", "--initialize"],
        ["run", "/data", "--set", "cluster=c"],
        ["show", "/data", "--add", "launchers=a"],
        ["init", "/data", "--remove", "launchers=a"],
        ["init", "/data", "--exchange", "/x"],
    ],
)
def test_daemon_refuses_ambiguous_paths_and_unknown_options(
    arguments: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("initialize", "configure", "describe", "activate"):
        monkeypatch.setattr(_daemon_setup, name, lambda *_args, **_kwargs: pytest.fail("must refuse before setup"))
    monkeypatch.setattr(_daemon_cli.os, "execve", lambda *_args: pytest.fail("must refuse before any handoff"))
    assert _daemon_cli.command(arguments, program="httk workspace daemon") == 2


def test_relative_paths_anchor_to_the_physical_cwd_without_resolving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cwd = tmp_path / "site" / "data"
    cwd.mkdir(parents=True)
    calls: list[tuple[Path, Path | None]] = []

    def initialize(workspace: Path, *, state: Path | None, **_options: object) -> Path:
        calls.append((workspace, state))
        return _snapshot(tmp_path)

    monkeypatch.setattr(_daemon_setup, "initialize", initialize)
    monkeypatch.setattr(_daemon_setup, "describe", lambda *_a, **_k: {"configuration": {}})
    monkeypatch.setattr(_daemon_cli, "_bootstrap_argv", lambda *_a, **_k: ["check"])
    monkeypatch.setattr(_daemon_cli.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0))
    monkeypatch.chdir(cwd)
    assert _daemon_cli.command(["init", ".", "--state", "../../state"], program="httk") == 0
    assert _daemon_cli.command(["init", "link/x"], program="httk") == 0
    assert calls == [(cwd, tmp_path / "state"), (cwd / "link" / "x", None)]


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
_daemon_setup.activate = lambda workspace, **_: (__import__('pathlib').Path('/active/runtime.json'), None)
fd = os.open({str(outside)!r}, os.O_RDONLY)
os.set_inheritable(fd, True)
os.environ.update(PYTHONPATH={str(untrusted)!r}, BASH_ENV='untrusted', SBATCH_ACCOUNT='other', NSC_RESOURCE_NAME='x')
os.chdir({str(untrusted)!r})
raise SystemExit(_daemon_cli.command(['run',{str(workspace)!r},'--once'], program='httk workspace daemon'))
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
    assert "SBATCH_ACCOUNT" not in observed["env"] and observed["env"]["NSC_RESOURCE_NAME"] == "x"
    assert observed["env"]["PATH"] == os.environ["PATH"]
    assert observed["argv"][1:] == [
        "--workspace",
        str(workspace),
        "--policy",
        "/active/runtime.json",
        "--once",
    ]


def _description() -> dict[str, object]:
    return {
        "workspace": "/workspace",
        "cluster": "c1",
        "configuration": {
            "launchers": ["small", "large"],
            "authorized_keys": [AUTHORIZED_KEY],
            "sacct": None,
            "force": True,
        },
    }


_RENDERED = (
    f"workspace: /workspace\ncluster: c1\nlaunchers=small,large\nauthorized_keys={AUTHORIZED_KEY}\nsacct=\nforce=true\n"
)


def test_init_and_configure_pass_changes_in_order_and_print_without_exec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[tuple[str, Path, dict[str, object]]] = []

    def initialize(workspace: Path, **options: object) -> Path:
        calls.append(("init", workspace, options))
        return _snapshot(tmp_path)

    def configure(workspace: Path, changes: object, **options: object) -> dict[str, object]:
        calls.append(("configure", workspace, {"changes": changes, **options}))
        return _description()

    monkeypatch.setattr(_daemon_setup, "initialize", initialize)
    monkeypatch.setattr(_daemon_setup, "configure", configure)
    monkeypatch.setattr(_daemon_setup, "describe", lambda *_a, **_k: _description())
    monkeypatch.setattr(_daemon_cli.os, "execve", lambda *_args: pytest.fail("local setup must not exec"))
    monkeypatch.setattr(_daemon_cli, "_bootstrap_argv", lambda *_a, **_k: ["check"])
    monkeypatch.setattr(_daemon_cli.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0))
    changes = ["--set", "bwrap=tools/bwrap", "--add", "launchers=small", "--set", "cluster=c1"]
    changes += ["--add", f"authorized_keys={AUTHORIZED_KEY}"]
    assert _daemon_cli.command(["init", "/workspace", *changes], program="httk") == 0
    arguments = ["configure", "/workspace", "--remove", "launchers=large", "--set", "force=true", "--state", "/state"]
    assert _daemon_cli.command(arguments, program="httk") == 0
    assert calls == [
        (
            "init",
            Path("/workspace"),
            {
                "changes": [
                    ("set", "bwrap=tools/bwrap"),
                    ("add", "launchers=small"),
                    ("set", "cluster=c1"),
                    ("add", f"authorized_keys={AUTHORIZED_KEY}"),
                ],
                "state": None,
                "snapshots": None,
                "report": print,
            },
        ),
        (
            "configure",
            Path("/workspace"),
            {
                "changes": [("remove", "launchers=large"), ("set", "force=true")],
                "state": Path("/state"),
                "snapshots": None,
            },
        ),
    ]
    assert capsys.readouterr().out == (
        f"{_RENDERED}sandbox check passed\n{_RENDERED}"
        "the configuration takes effect when the daemon next starts: httk workspace daemon run /workspace\n"
    )


def test_show_renders_or_prints_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[object] = []

    def describe(workspace: Path, **options: object) -> dict[str, object]:
        calls.append((workspace, options))
        return _description()

    monkeypatch.setattr(_daemon_setup, "describe", describe)
    assert _daemon_cli.command(["show", "/workspace", "--snapshots", "/snapshots"], program="httk") == 0
    assert capsys.readouterr().out == _RENDERED
    assert _daemon_cli.command(["show", "/workspace", "--json"], program="httk") == 0
    assert json.loads(capsys.readouterr().out) == _description()
    assert calls == [
        (Path("/workspace"), {"state": None, "snapshots": Path("/snapshots")}),
        (Path("/workspace"), {"state": None, "snapshots": None}),
    ]


def test_run_modes_hand_state_and_snapshots_to_activation(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[object] = []

    def activate(workspace: Path, **options: object) -> tuple[Path, None]:
        observed.append((workspace, options))
        raise ValueError("inspection stop")

    monkeypatch.setattr(_daemon_setup, "activate", activate)
    for mode in (["run", "--once"], ["check"]):
        arguments = [mode[0], "/workspace", *mode[1:], "--state", "/state", "--snapshots", "/snapshots"]
        assert _daemon_cli.command(arguments, program="httk") == 2
    assert observed == [(Path("/workspace"), {"state": Path("/state"), "snapshots": Path("/snapshots")})] * 2


def test_incompatible_ledger_is_a_clean_local_refusal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def incompatible(_workspace: Path, *_args: object, **_options: object) -> Path:
        raise LedgerError("preserve this state and initialize a new enrollment")

    monkeypatch.setattr(_daemon_setup, "configure", incompatible)
    assert _daemon_cli.command(["configure", "/workspace", "--set", "force=true"], program="httk") == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "preserve this state" in captured.err


def _init_with_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: int | OSError
) -> tuple[Path, list[dict[str, object]], list[object]]:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    snapshot = _snapshot(tmp_path)
    runs: list[dict[str, object]] = []
    argvs: list[object] = []

    def run(argv: list[str], **options: object) -> subprocess.CompletedProcess[str]:
        argvs.append(argv)
        runs.append(options)
        if isinstance(outcome, OSError):
            raise outcome
        return subprocess.CompletedProcess(argv, outcome)

    monkeypatch.setenv("NSC_RESOURCE_NAME", "tetralith")
    monkeypatch.setenv("SBATCH_ACCOUNT", "other")
    monkeypatch.setattr(_daemon_setup, "initialize", lambda *_a, **_k: snapshot)
    monkeypatch.setattr(_daemon_setup, "describe", lambda *_a, **_k: {"configuration": {}})
    monkeypatch.setattr(_daemon_cli.subprocess, "run", run)
    monkeypatch.setattr(_daemon_cli.os, "execve", lambda *_args: pytest.fail("setup must not exec"))
    arguments = ["init", str(workspace), "--add", "launchers=small"]
    status = _daemon_cli.command(arguments, program="httk")
    runs.append({"status": status})
    return snapshot, runs, argvs


def test_init_runs_one_clean_sandbox_check_and_reports_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshot, runs, argvs = _init_with_check(tmp_path, monkeypatch, 0)
    assert len(argvs) == 1 and runs[-1] == {"status": 0}
    argv = argvs[0]
    assert isinstance(argv, list) and argv[-1] == "--check" and "--mode" not in argv
    assert argv == _daemon_cli._bootstrap_argv(tmp_path / "ws", snapshot, "--check")
    environment = runs[0].pop("env")
    assert runs[0] == {"stdin": subprocess.DEVNULL, "cwd": "/", "close_fds": True, "check": False}
    assert isinstance(environment, dict) and environment == _daemon_cli._launch_environment()
    assert environment["NSC_RESOURCE_NAME"] == "tetralith" and "SBATCH_ACCOUNT" not in environment
    assert capsys.readouterr().out.endswith("sandbox check passed\n")


@pytest.mark.parametrize("outcome", [3, OSError("cannot start")])
def test_failed_check_keeps_the_enrollment_and_exits_2(
    outcome: int | OSError, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _snapshot_path, runs, _argvs = _init_with_check(tmp_path, monkeypatch, outcome)
    assert runs[-1] == {"status": 2}
    captured = capsys.readouterr()
    assert "sandbox check passed" not in captured.out
    assert "the enrollment was saved, but the sandbox check failed" in captured.err
    assert "httk workspace daemon configure" in captured.err and "httk workspace daemon check" in captured.err


@pytest.mark.parametrize(("mode", "flag"), [(["check"], "--check"), (["run", "--once"], "--once"), (["run"], None)])
@pytest.mark.parametrize("changed", [None, (), ("small",)])
def test_run_modes_exec_the_bootstrap_with_the_activated_snapshot(
    mode: list[str],
    flag: str | None,
    changed: tuple[str, ...] | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    active = tmp_path / "active.json"
    monkeypatch.setattr(_daemon_setup, "activate", lambda *_a, **_k: (active, changed))
    execs: list[tuple[str, list[str], dict[str, str]]] = []

    def execve(path: str, argv: list[str], env: dict[str, str]) -> None:
        execs.append((path, argv, env))
        raise OSError("stop")

    monkeypatch.setattr(_daemon_cli.os, "execve", execve)
    monkeypatch.setenv("NSC_RESOURCE_NAME", "tetralith")
    monkeypatch.setenv("SBATCH_ACCOUNT", "other")
    monkeypatch.delenv("PATH")
    monkeypatch.setattr(_daemon_cli.os, "chdir", lambda _p: None)
    monkeypatch.setattr(_daemon_cli, "_close_inherited", lambda: None)
    monkeypatch.setattr(_daemon_cli.os, "dup2", lambda *_a, **_k: 0)
    monkeypatch.setattr(_daemon_cli.os, "set_inheritable", lambda *_a: None)
    assert _daemon_cli.command([mode[0], str(tmp_path), *mode[1:]], program="httk") == 2
    expected = _daemon_cli._bootstrap_argv(tmp_path, active, flag)
    assert [(path, argv) for path, argv, _ in execs] == [(sys.executable, expected)]
    environment = execs[0][2]
    assert environment["NSC_RESOURCE_NAME"] == "tetralith" and "SBATCH_ACCOUNT" not in environment
    assert environment["PATH"] == "/usr/bin:/bin" and environment["LANG"] == os.environ.get("LANG", "C.UTF-8")
    output = capsys.readouterr().out
    if changed is None:
        assert output == ""
    else:
        names = ", ".join(changed) or "none"
        assert output == f"activated the changed daemon configuration; changed launchers: {names}\n"
