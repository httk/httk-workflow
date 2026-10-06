"""Pin confinement settings on managers with ``--setting`` and forward launcher pins."""

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Self

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace, launch_runtime
from httk.workflow._confine import confine_settings
from httk.workflow.launchers import add_launcher, configure_launcher, describe_launcher
from httk.workflow.projects import initialize_project
from httk.workflow.registry import register_workspace
from httk.workflow.workflow_cli import _manager, build_parser, command


def _parser(tmp_path: Path) -> argparse.ArgumentParser:
    return build_parser("httk workflow", CLIContext("httk", tmp_path))


@pytest.mark.parametrize("leaf", [["manager", "run"], ["run"]])
def test_allowed_keys_parse_and_the_last_occurrence_wins(tmp_path: Path, leaf: list[str]) -> None:
    arguments = _parser(tmp_path).parse_args(
        [
            *leaf,
            "--setting",
            "manager.confine=bwrap",
            "--setting",
            "confine.isolate_network=false",
            "--setting",
            "manager.launch_template=srun --mpi=pmix {command}",
            "--setting",
            "manager.bind_cpus=true",
            "--setting",
            "manager.launch_mpi=pmi2",
            "--setting",
            "confine.environment.OMPI_MCA_x=a=b",
            "--setting",
            "manager.confine=none",
        ]
    )
    assert _manager._pinned_settings(arguments) == {
        "manager.confine": "none",
        "confine.isolate_network": "false",
        "manager.launch_template": "srun --mpi=pmix {command}",
        "manager.bind_cpus": "true",
        "manager.launch_mpi": "pmi2",
        "confine.environment.OMPI_MCA_x": "a=b",
    }
    assert _manager._pinned_settings(_parser(tmp_path).parse_args(leaf)) == {}


@pytest.mark.parametrize(
    ("item", "message"),
    [
        (
            "manager.workers=2",
            "manager.confine, manager.launch_template, manager.launch_mpi, manager.bind_cpus or confine.*",
        ),
        (
            "manager.launch=slurm",
            "manager.confine, manager.launch_template, manager.launch_mpi, manager.bind_cpus or confine.*",
        ),
        (
            "slurm.partition=debug",
            "manager.confine, manager.launch_template, manager.launch_mpi, manager.bind_cpus or confine.*",
        ),
        ("manager.confine", "KEY=VALUE"),
        ("manager.confine=chroot", "manager.confine must be none or bwrap"),
        ("confine.bogus=1", "unknown confinement setting 'confine.bogus'"),
        ("confine.isolate_network=maybe", "must be true or false"),
        ("confine.readonly_paths=usr", "absolute paths"),
        ("confine.environment.HTTK_X=1", "must name a variable"),
        ("manager.launch_template=a\0b", "NUL"),
        ("manager.launch_mpi=PMI2", "manager.launch_mpi must be 1-32 lowercase"),
    ],
)
def test_other_keys_and_bad_values_are_usage_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], item: str, message: str
) -> None:
    for leaf in (["manager", "run"], ["run"]):
        with pytest.raises(SystemExit) as raised:
            _parser(tmp_path).parse_args([*leaf, "--setting", item])
        assert raised.value.code == 2
        error = capsys.readouterr().err
        assert "--setting" in error and message in error


def test_setting_is_hidden_from_the_help(tmp_path: Path) -> None:
    parser = _parser(tmp_path)
    subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    manager_group = subparsers.choices["manager"]
    manager_subparsers = next(
        action for action in manager_group._actions if isinstance(action, argparse._SubParsersAction)
    )
    for leaf in (manager_subparsers.choices["run"], subparsers.choices["run"]):
        assert "--setting" not in leaf.format_help()


def test_argv_tail_forwards_the_effective_pins_to_children_and_remotes(tmp_path: Path) -> None:
    arguments = _parser(tmp_path).parse_args(
        [
            "manager",
            "run",
            "--setting",
            "manager.confine=bwrap",
            "--setting",
            "confine.readonly_paths=/usr:/opt/my tools",
            "--setting",
            "manager.confine=none",
        ]
    )
    tail = _manager.manager_argv_tail(arguments)
    assert tail == [
        "--setting",
        "manager.confine=none",
        "--setting",
        "confine.readonly_paths=/usr:/opt/my tools",
    ]
    remote = _manager._remote_manager_argv(arguments, "cluster-ws")
    assert remote[-4:] == tail
    # A child parses the forwarded tail back to the same pins.
    child = _parser(tmp_path).parse_args(["manager", "run", *tail])
    assert _manager._pinned_settings(child) == _manager._pinned_settings(arguments)


def test_launcher_pins_follow_the_forwarded_cli_pins() -> None:
    argv = ["httk", "workflow", "manager", "run", "--setting", "manager.confine=none"]
    workspace = {"confine.devices": "/dev/nvidia0", "manager.bind_cpus": "true", "manager.confine": "none"}
    launcher = {
        "manager.confine": "bwrap",
        "confine.isolate_network": 0,
        "manager.workers": "2",
        "slurm.partition": "debug",
        "confine.environment.EMPTY": None,
    }
    result = launch_runtime._manager_argv(argv, {**workspace, **launcher}, launcher)
    assert result == [
        *argv,
        "--workers",
        "2",
        "--allocation",
        "slurm",
        "--setting",
        "confine.isolate_network=0",
        "--setting",
        "manager.confine=bwrap",
    ]
    # Without a bundle (the old two-argument form) nothing is pinned.
    assert "--setting" not in launch_runtime._manager_argv(["httk"], {**workspace, **launcher})[1:]
    with pytest.raises(ValueError, match="manager.launch_template must be a string or number"):
        launch_runtime._manager_argv(["httk"], {}, {"manager.launch_template": ["srun"]})


@pytest.mark.parametrize("value", [0, 1, "0", "1", "true", "FALSE"])
def test_launcher_boolean_pins_round_trip_through_the_manager_cli(tmp_path: Path, value: object) -> None:
    bundle = {"confine.isolate_network": value, "manager.confine": "bwrap"}
    argv = launch_runtime._manager_argv(["httk", "workflow", "manager", "run"], bundle, bundle)
    pinned = _manager._pinned_settings(_parser(tmp_path).parse_args(argv[2:]))
    assert pinned["confine.isolate_network"] == str(value)
    expected = str(value).lower() in ("1", "true")
    assert confine_settings(pinned).isolate_network is expected
    assert confine_settings(bundle).isolate_network is expected


def test_batch_script_carries_launcher_pins_after_cli_ones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bundle = tmp_path / "launcher"
    bundle.mkdir()
    launcher_settings = {
        "manager.confine": "bwrap",
        "confine.readonly_paths": "/usr:/opt/my tools",
        "manager.launch_template": "srun --cpu-bind=none {command}",
        "slurm.partition": "debug",
    }
    (bundle / "launcher.json").write_text(
        json.dumps({"kind": "slurm", "required_binaries": [], "settings": launcher_settings}), encoding="utf-8"
    )
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    request = tmp_path / "request.json"
    cli_argv = [
        sys.executable,
        "-m",
        "httk.core.cli",
        "workflow",
        "manager",
        "run",
        "--setting",
        "manager.confine=none",
    ]
    request.write_text(
        json.dumps(
            {
                "operation": "start",
                "launcher_dir": str(bundle),
                "workspace": str(workspace),
                "argv": cli_argv,
                "count": 1,
                "settings": {"confine.devices": "/dev/nvidia0", "manager.bind_cpus": "true"},
                "launcher_settings": launcher_settings,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        launch_runtime.subprocess,
        "run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, stdout="Submitted batch job 9\n", stderr=""),
    )
    assert launch_runtime.main([str(request)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is True
    script = Path(result["script"]).read_text(encoding="utf-8")
    assert "#SBATCH --partition=debug" in script
    [exec_line] = [line for line in script.splitlines() if line.startswith("exec ")]
    assert "--setting 'confine.readonly_paths=/usr:/opt/my tools'" in exec_line
    assert "--setting 'manager.launch_template=srun --cpu-bind=none {command}'" in exec_line
    assert exec_line.index("manager.confine=none") < exec_line.index("manager.confine=bwrap")
    assert "confine.devices" not in exec_line and "manager.bind_cpus" not in exec_line
    words = shlex.split(exec_line)[1:]
    assert words[: len(cli_argv)] == cli_argv
    parsed = _parser(tmp_path).parse_args(words[4:])
    assert _manager._pinned_settings(parsed) == {
        "manager.confine": "bwrap",
        "confine.readonly_paths": "/usr:/opt/my tools",
        "manager.launch_template": "srun --cpu-bind=none {command}",
    }


@pytest.fixture
def sbatch_on_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    binaries = tmp_path / "bin"
    binaries.mkdir()
    sbatch = binaries / "sbatch"
    sbatch.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    sbatch.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binaries}:/usr/bin:/bin")


@pytest.mark.usefixtures("sbatch_on_path")
def test_slurm_launchers_validate_confinement_settings() -> None:
    with pytest.raises(ValueError, match="manager.confine must be none or bwrap"):
        add_launcher("bad", template="slurm", settings={"manager.confine": "chroot"}, global_=True)
    add_launcher("cluster", template="slurm", settings={"manager.confine": "bwrap"}, global_=True)
    for settings, message in (
        ({"confine.bogus": "1"}, "unknown confinement setting 'confine.bogus'"),
        ({"confine.readonly_paths": "/usr:relative"}, "absolute paths"),
        ({"confine.environment.HTTK_X": "1"}, "must name a variable"),
    ):
        with pytest.raises(ValueError, match=message):
            configure_launcher("cluster", settings)
    configure_launcher("cluster", {"confine.isolate_network": "false", "confine.devices": "/dev/nvidia0"})
    assert describe_launcher("cluster")["settings"] == {
        "manager.confine": "bwrap",
        "confine.isolate_network": "false",
        "confine.devices": "/dev/nvidia0",
    }


def test_in_process_manager_uses_and_validates_effective_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    initialize_project(tmp_path, name="pins")
    workspace = Workspace.initialize(tmp_path / "workspace")
    register_workspace("pins", workspace.root)
    constructed: list[object] = []
    slots: list[bool] = []

    class FakeManager:
        manager_id = "manager"
        pools = frozenset({"default"})
        capabilities: frozenset[str] = frozenset()
        allowed_executors = frozenset({"path"})
        drained: str | None = None

        def __init__(self, _workspace: object, **_kwargs: object) -> None:
            self.resources: dict[str, int] = {}
            constructed.append(self)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def run_until_idle(self, **_kwargs: object) -> object:
            return type("Census", (), {"summary_line": lambda _self: "idle", "time_advice": lambda _self: None})()

    def probe(_spec: str, _environ: object, *, cpu_slots: bool) -> None:
        slots.append(cpu_slots)

    monkeypatch.setattr(_manager, "TaskManager", FakeManager)
    monkeypatch.setattr(_manager, "probe_allocation", probe)
    context = CLIContext("httk", tmp_path)
    run = ["manager", "run", "--inline", "--workspace", "pins", "--allocation", "none"]
    assert command([*run, "--setting", "manager.bind_cpus=true"], context) == 0
    assert slots == [True] and len(constructed) == 1

    workspace.set_setting("manager.bind_cpus", "true")
    assert command([*run, "--setting", "manager.bind_cpus=false"], context) == 0
    assert slots == [True, False]

    workspace.set_setting("confine.bogus", "1")
    capsys.readouterr()
    assert command(run, context) == 2
    assert "unknown confinement setting 'confine.bogus'" in capsys.readouterr().err
    assert len(constructed) == 2
