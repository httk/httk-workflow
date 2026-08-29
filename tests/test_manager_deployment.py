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


def test_slurm_resources_require_an_active_job() -> None:
    assert _manager._slurm_resources({"SLURM_NTASKS": "8"}) == {}


def test_slurm_resources_parse_full_allocation() -> None:
    assert _manager._slurm_resources(
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
        _manager._slurm_resources({"SLURM_JOB_ID": "123", "SLURM_JOB_NUM_NODES": "2", "SLURM_MEM_PER_NODE": "4G"})[
            "mem"
        ]
        == 8192
    )
    assert (
        _manager._slurm_resources({"SLURM_JOB_ID": "123", "SLURM_NTASKS": "2", "SLURM_MEM_PER_CPU": "4G"})["mem"]
        == 8192
    )

    caplog.clear()
    resources = _manager._slurm_resources(
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
        allowed_executors = frozenset({"path"})
        manager_directory = workspace.root / ".httk-workspace" / "managers" / "manager"

        def __init__(self, _workspace, **kwargs: object) -> None:
            seen.update(kwargs)
            self.resources = kwargs["resources"]
            self.manager_directory.mkdir(parents=True, exist_ok=True)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def run_until_idle(self, **_kwargs: object):
            return type("Census", (), {"summary_line": lambda _self: "idle"})()

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
    monkeypatch.setattr(_manager, "resolve_remote", lambda *_args, **_kwargs: "target")
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
    managers = list((tmp_path / "workspace" / ".httk-workspace" / "managers").iterdir())
    assert managers == []
    log = tmp_path / "workspace" / ".httk-workspace" / "managers.log"
    assert len(log.read_text(encoding="utf-8").splitlines()) > 0


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
