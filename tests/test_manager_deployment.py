"""Manager capacity parsing and deployment defaults."""

from pathlib import Path
from typing import Self

import pytest

from httk.workflow.workflow_cli import _manager


def test_worker_resources_reject_invalid_and_duplicate_pairs() -> None:
    assert _manager._worker_resources([["procs", "4"], ["mem", "100"]]) == {"procs": 4, "mem": 100}
    with pytest.raises(ValueError, match="duplicate"):
        _manager._worker_resources([["procs", "4"], ["procs", "8"]])
    with pytest.raises(ValueError, match="non-negative"):
        _manager._worker_resources([["procs", "-1"]])
    with pytest.raises(ValueError, match="non-negative"):
        _manager._worker_resources([["procs", "four"]])
    with pytest.raises(ValueError, match="job requirement, not a manager capacity"):
        _manager._worker_resources([["maxtime", "10"]])


def test_slurm_resources_require_an_active_job() -> None:
    assert _manager._scheduler_resources({"SLURM_NTASKS": "8"}) == {}


def test_slurm_resources_parse_full_allocation() -> None:
    assert _manager._scheduler_resources(
        {
            "SLURM_JOB_ID": "123",
            "SLURM_NTASKS": "8",
            "SLURM_CPUS_PER_TASK": "2",
            "SLURM_MEM_PER_CPU": "2000",
            "SLURM_GPUS": "2",
            "SLURM_JOB_NUM_NODES": "1",
        }
    ) == {"procs": 8, "gpus": 2, "nodes": 1, "mem": 32000}


def test_slurm_resources_memory_fallback_and_units(caplog: pytest.LogCaptureFixture) -> None:
    assert (
        _manager._scheduler_resources({"SLURM_JOB_ID": "123", "SLURM_JOB_NUM_NODES": "2", "SLURM_MEM_PER_NODE": "4G"})[
            "mem"
        ]
        == 8192
    )
    assert (
        _manager._scheduler_resources({"SLURM_JOB_ID": "123", "SLURM_NTASKS": "2", "SLURM_MEM_PER_CPU": "4G"})["mem"]
        == 8192
    )

    caplog.clear()
    resources = _manager._scheduler_resources(
        {"SLURM_JOB_ID": "123", "SLURM_NTASKS": "garbage", "SLURM_MEM_PER_CPU": "2000"}
    )
    assert "procs" not in resources and "mem" not in resources
    assert "SLURM_NTASKS" in caplog.text


def test_run_passes_cli_resources_over_slurm(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.core.cli import CLIContext

    from httk.workflow import Workspace
    from httk.workflow.projects import initialize_project
    from httk.workflow.registry import register_workspace
    from httk.workflow.workflow_cli import command

    initialize_project(tmp_path, name="resources")
    workspace = Workspace.initialize(tmp_path / "workspace")
    register_workspace("resources", workspace.root)
    seen: dict[str, object] = {}

    class FakeManager:
        manager_id = "manager"
        pools = frozenset({"default"})
        capabilities: frozenset[str] = frozenset()
        manager_directory = workspace.root / ".httk-workspace" / "owners" / "manager"
        drained: str | None = None

        def __init__(self, _workspace, **kwargs: object) -> None:
            seen.update(kwargs)
            self.resources = kwargs["resources"]
            self.manager_directory.mkdir(parents=True, exist_ok=True)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def run_until_idle(self, **_kwargs: object):
            return type("Census", (), {"summary_line": lambda _self: "idle", "time_advice": lambda _self: None})()

    monkeypatch.setattr(_manager, "TaskManager", FakeManager)
    monkeypatch.setenv("SLURM_JOB_ID", "123")
    monkeypatch.setenv("SLURM_NTASKS", "8")
    monkeypatch.setenv("SLURM_MEM_PER_CPU", "2000")
    assert (
        command(
            [
                "run",
                "--workspace",
                "resources",
                "--worker-resource",
                "procs",
                "4",
                "--worker-resource",
                "mem",
                "100",
            ],
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    assert seen["resources"] == {"procs": 4, "mem": 100}


def test_remote_run_forwards_worker_resources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    from httk.core.cli import CLIContext

    from httk.workflow import workflow_cli
    from httk.workflow.registry import WorkspaceBinding

    context = CLIContext("httk", tmp_path)
    parser = workflow_cli.build_parser("httk workflow", context)
    arguments = parser.parse_args(
        [
            "manager",
            "run",
            "--workspace",
            "cluster:station",
            "--worker-resource",
            "procs",
            "4",
            "--worker-resource",
            "mem",
            "100",
        ]
    )
    seen: dict[str, object] = {}
    monkeypatch.setattr(
        _manager,
        "remote_workspace_output",
        lambda _binding, _context, argv, **kwargs: seen.update(argv=argv, **kwargs) or (0, "", ""),
    )
    assert (
        _manager._submit_remote_manager(WorkspaceBinding("cluster:station", "cluster", None), arguments, context) == 0
    )
    assert seen["argv"] == [
        "httk",
        "workflow",
        "manager",
        "run",
        "--workspace",
        "station",
        "--detach",
        "--worker-resource",
        "procs",
        "4",
        "--worker-resource",
        "mem",
        "100",
    ]
    capsys.readouterr()


def test_local_count_starts_multiple_manager_processes(tmp_path: Path) -> None:
    from httk.core.cli import CLIContext

    from httk.workflow.registry import create_workspace
    from httk.workflow.workflow_cli import command

    create_workspace("many", tmp_path / "workspace")
    assert (
        command(
            [
                "manager",
                "run",
                "--workspace",
                "many",
                "--count",
                "2",
                "--idle-timeout",
                "1",
                "--poll-interval",
                "0.01",
            ],
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    # Each manager closed cleanly, so neither left its owner directory behind.
    owners = tmp_path / "workspace" / ".httk-workspace" / "owners"
    assert not owners.exists() or list(owners.iterdir()) == []
    logs = list((tmp_path / "workspace" / "logs" / "managers").glob("*.log"))
    assert len(logs) == 2
    assert all(log.read_text(encoding="utf-8").splitlines() for log in logs)


def test_local_count_splits_capacity_in_each_child_argv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.core.cli import CLIContext

    from httk.workflow import workflow_cli

    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    arguments = parser.parse_args(
        [
            "manager",
            "run",
            "--workspace",
            str(tmp_path / "workspace"),
            "--count",
            "2",
            "--worker-resource",
            "mem",
            "100",
        ]
    )
    captured: list[list[str]] = []

    class FakePopen:
        def __init__(self, argv, **_kwargs):
            captured.append(list(argv))
            self.returncode = 0
            self.terminated = False

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            self.terminated = True

        def wait(self) -> int:
            return self.returncode

    monkeypatch.setenv("SLURM_JOB_ID", "1")
    monkeypatch.setenv("SLURM_NTASKS", "5")
    monkeypatch.setattr(_manager.subprocess, "Popen", FakePopen)
    assert _manager._run_local_manager_children(arguments, tmp_path / "workspace", CLIContext("httk", tmp_path)) == 0
    assert len(captured) == 2
    assert [next(argv[index + 1] for index, value in enumerate(argv) if value == "procs") for argv in captured] == [
        "3",
        "2",
    ]
    assert all(
        next(argv[index + 1] for index, value in enumerate(argv) if value == "mem") == "100" for argv in captured
    )


def test_process_launcher_is_available_for_detached_managers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import Workspace

    workspace = Workspace.initialize(tmp_path / "workspace")
    captured: list[list[str]] = []

    class FakePopen:
        def __init__(self, argv, **_kwargs):
            captured.append(list(argv))
            self.pid = len(captured)

    monkeypatch.setattr(_manager.subprocess, "Popen", FakePopen)
    result = _manager.launch_processes(
        workspace_root=workspace.root,
        argv=["python", "manager"],
        count=2,
        settings={},
        capacity={"procs": 3},
    )
    assert result["pids"] == [1, 2]
    assert len(captured) == 2


def test_manager_option_defaults_match_the_parsers_and_cover_the_argv_tail(tmp_path: Path) -> None:
    import argparse
    import inspect
    import re

    from httk.core.cli import CLIContext

    from httk.workflow import workflow_cli

    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    defaults = _manager.manager_option_defaults()
    for argv in (["manager", "run"], ["run"]):
        parsed = vars(parser.parse_args(argv))
        assert {name: parsed[name] for name in defaults} == defaults
    tail = _manager.manager_argv_tail(argparse.Namespace(**defaults))
    assert tail == [] == _manager.manager_argv_tail(parser.parse_args(["manager", "run"]))
    source = inspect.getsource(_manager.manager_argv_tail)
    read = set(re.findall(r'(?:getattr\(arguments, |changed\()"(\w+)"', source))
    # The durability switches default to argparse.SUPPRESS; absent reads as false.
    assert read - {"durable", "no_durable"} <= set(defaults)
