"""Launcher bundle, runtime, and CLI behaviour."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import Remote
from httk.workflow import Workspace, launch_runtime, launchers
from httk.workflow.launchers import (
    add_launcher,
    launch_processes,
    list_launchers,
    resolve_launcher,
    split_capacity,
    start_managers,
    validate_launcher_bundle,
)
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command


def test_split_capacity_uses_quotient_remainder_and_preserves_explicit() -> None:
    assert split_capacity({"procs": 10, "mem": 5, "license": 7}, 3, {"license"}) == [
        {"procs": 4, "mem": 2, "license": 7},
        {"procs": 3, "mem": 2, "license": 7},
        {"procs": 3, "mem": 1, "license": 7},
    ]


def test_launcher_validation_checks_executable_format_and_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "launcher"
    bundle.mkdir()
    (bundle / "launcher").write_text("#!/bin/sh\n", encoding="utf-8")
    metadata = {
        "format": "httk-manager-launcher",
        "format_version": 2,
        "launcher_version": 2,
        "kind": "slurm",
        "settings": {},
        "required_binaries": [],
        "timeout_seconds": 60,
    }
    metadata_path = bundle / "launcher.json"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="not runnable"):
        validate_launcher_bundle(bundle)
    (bundle / "launcher").chmod(0o755)
    assert validate_launcher_bundle(bundle)["kind"] == "slurm"
    metadata["format"] = "wrong"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="format version 2"):
        validate_launcher_bundle(bundle)
    metadata["format"] = "httk-manager-launcher"
    metadata["required_binaries"] = ["definitely-not-a-real-launcher-binary"]
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setattr(launchers.shutil, "which", lambda name: None)
    with pytest.raises(ValueError, match="required launcher binary"):
        validate_launcher_bundle(bundle)


def test_launcher_cli_round_trip_project_and_global(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-cli")
    context = CLIContext("httk", project)
    assert command(["launcher", "add", "--template", "slurm", "cluster"], context) == 0
    local_bundle = project / "httk_project" / "launchers" / "cluster"
    assert (local_bundle / "launcher").is_file()
    assert command(["launcher", "add", "--template", "slurm", "--global", "shared"], context) == 0
    assert (Path(os.environ["HTTK_CONFIG_HOME"]) / "launchers" / "shared" / "launcher.json").is_file()
    assert [row["name"] for row in list_launchers(project)] == ["cluster", "shared"]
    capsys.readouterr()
    assert command(["launcher", "show", "--json", "cluster"], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["kind"] == "slurm"
    assert command(["launcher", "check", "cluster"], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["ok"] is True
    assert command(["launcher", "remove", "--force", "cluster"], context) == 0
    assert not local_bundle.exists()


def test_launcher_add_process_is_reserved(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="reserved-launcher")
    with pytest.raises(ValueError, match="reserved"):
        add_launcher("process", template="slurm", project=project)


def test_slurm_launcher_start_spools_scripts(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-start")
    workspace = Workspace.initialize(tmp_path / "workspace")
    bundle = add_launcher("cluster", template="slurm", project=project)
    target = resolve_launcher("cluster", project=project)
    settings = {
        "slurm.account": "p2026-1",
        "slurm.partition": "main",
        "slurm.time_limit": "04:00:00",
        "slurm.nodes": "2",
        "slurm.cpus_per_task": "16",
        "slurm.reservation": "special",
        "environment.prelude": "module load python",
    }
    result = start_managers(
        target,
        workspace_root=workspace.root,
        argv=["httk", "workflow", "manager", "run", "--workspace", str(workspace.root)],
        count=2,
        settings=settings,
        timeout=None,
    )
    assert result["job_ids"] == ["4201", "4202"]
    script = Path(str(result["script"])).read_text(encoding="utf-8")
    for directive in (
        "--account=p2026-1",
        "--partition=main",
        "--time=04:00:00",
        "--nodes=2",
        "--cpus-per-task=16",
        "--reservation=special",
    ):
        assert f"#SBATCH {directive}" in script
    assert f"#SBATCH --chdir={workspace.root}" in script
    assert "module load python" in script
    assert f"exec httk workflow manager run --workspace {workspace.root}" in script
    assert bundle.exists()


def test_unknown_launcher_kind_is_a_clean_cli_refusal(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unknown-launcher")
    bundle = add_launcher("cluster", template="slurm", project=project)
    metadata_path = bundle / "launcher.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["kind"] = "pbs"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    assert command(["launcher", "check", "cluster"], CLIContext("httk", project)) == 1
    captured = capsys.readouterr()
    assert "is not implemented" in captured.err
    assert "Traceback" not in captured.err


def test_launch_processes_detaches_and_filters_slurm(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    class Child:
        def __init__(self, pid: int) -> None:
            self.pid = pid

    def fake_popen(argv: list[str], **kwargs: object) -> Child:
        calls.append({"argv": argv, **kwargs})
        return Child(700 + len(calls))

    monkeypatch.setenv("SLURM_JOB_ID", "ignored")
    monkeypatch.setenv("KEEP_ME", "yes")
    monkeypatch.setattr(launchers.subprocess, "Popen", fake_popen)
    result = launch_processes(
        workspace_root=tmp_path,
        argv=[
            sys.executable,
            "-m",
            "httk.core.cli",
            "workflow",
            "manager",
            "run",
            "--worker-resource",
            "license",
            "99",
        ],
        count=2,
        settings={"environment.prelude": "module load python"},
        capacity={"procs": 5, "mem": 3, "license": 8},
    )
    assert result == {"ok": True, "kind": "process", "count": 2, "pids": [701, 702]}
    assert all(call["start_new_session"] is True for call in calls)
    assert all(call["stdin"] is subprocess.DEVNULL for call in calls)
    assert all(call["stdout"] is subprocess.DEVNULL for call in calls)
    assert all(call["stderr"] is subprocess.DEVNULL for call in calls)
    launched_argvs: list[list[str]] = []
    for call in calls:
        environment = call["env"]
        launched = call["argv"]
        assert isinstance(environment, dict)
        assert isinstance(launched, list)
        assert "SLURM_JOB_ID" not in environment
        assert environment["KEEP_ME"] == "yes"
        launched_argvs.append(launched)
    assert "--worker-resource license 99" in " ".join(launched_argvs[0])
    assert "--worker-resource procs 3" in " ".join(launched_argvs[0])
    assert "--worker-resource procs 2" in " ".join(launched_argvs[1])
    assert all(sys.executable not in argv for argv in launched_argvs)
    assert all("httk workflow manager run" in " ".join(argv) for argv in launched_argvs)


def test_manager_command_must_be_a_nonempty_string(tmp_path: Path) -> None:
    argv = [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"]
    with pytest.raises(ValueError, match="manager.command must be a nonempty string"):
        launch_processes(
            workspace_root=tmp_path,
            argv=argv,
            count=1,
            settings={"manager.command": 7},
            capacity={"procs": 1},
        )
    with pytest.raises(ValueError, match="manager.command must be a nonempty string"):
        launch_runtime._batch_script(argv, settings={"manager.command": 7}, workspace="/ws", directory="/ws/.batch")


def test_slurm_launcher_reports_partial_submission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    bundle = tmp_path / "launcher"
    bundle.mkdir()
    (bundle / "launcher.json").write_text(json.dumps({"kind": "slurm", "required_binaries": []}), encoding="utf-8")
    request = tmp_path / "request.json"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    request.write_text(
        json.dumps(
            {
                "operation": "start",
                "launcher_dir": str(bundle),
                "workspace": str(workspace),
                "argv": [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"],
                "count": 2,
                "settings": {},
            }
        ),
        encoding="utf-8",
    )
    calls = 0

    def fake_run(argv, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(argv, 0, stdout="Submitted batch job 4201\n", stderr="")
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="quota exceeded\n")

    monkeypatch.setattr(launch_runtime.subprocess, "run", fake_run)
    assert launch_runtime.main([str(request)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is False
    assert result["submitted"] == 1
    assert result["job_ids"] == ["4201"]
    assert "quota exceeded" in result["error"]
