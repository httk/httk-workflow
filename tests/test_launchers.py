"""Launcher bundle, runtime, and CLI behaviour."""

import argparse
import json
import logging
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import IO, cast

import pytest
from httk.core.cli import CLIContext

from conftest import Remote
from httk.workflow import Workspace, launch_runtime, launchers
from httk.workflow._daemon_policy import sbatch_option_lines
from httk.workflow.launchers import (
    add_launcher,
    configure_launcher,
    launch_processes,
    list_launchers,
    resolve_launcher,
    split_capacity,
    start_managers,
    validate_launcher_bundle,
)
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _manager, command


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
    assert command(["launcher", "configure", "--set", "slurm.partition=devel", "cluster"], context) == 0
    metadata = json.loads((local_bundle / "launcher.json").read_text(encoding="utf-8"))
    assert metadata["settings"] == {"slurm.partition": "devel"}
    capsys.readouterr()
    assert command(["launcher", "show", "--json", "cluster"], context) == 0
    shown = json.loads(capsys.readouterr().out)[0]
    assert shown["kind"] == "slurm"
    assert shown["settings"] == {"slurm.partition": "devel"}
    assert command(["launcher", "check", "cluster"], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["ok"] is True
    assert command(["launcher", "remove", "--force", "cluster"], context) == 0
    assert not local_bundle.exists()


def test_launcher_add_persists_settings_and_slurm_prefers_them(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-settings")
    workspace = Workspace.initialize(tmp_path / "workspace")
    bundle = add_launcher(
        "gpu",
        template="slurm",
        settings={
            "slurm.partition": "gpu",
            "manager.workers": "8",
            "manager.command": "/opt/gpu-httk",
            "environment.prelude": "module load gpu",
        },
        project=project,
    )
    configure_launcher("gpu", {"slurm.cpus_per_task": "8"}, project=project)
    workspace_settings = {
        "slurm.partition": "batch",
        "slurm.cpus_per_task": "2",
        "manager.workers": "4",
        "manager.command": "/opt/batch-httk",
        "environment.prelude": "module load batch",
    }
    result = start_managers(
        resolve_launcher("gpu", project=project),
        workspace_root=workspace.root,
        argv=[sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"],
        count=1,
        settings=workspace_settings,
        timeout=None,
    )
    [script_path] = cast(list[str], result["scripts"])
    script = Path(script_path).read_text(encoding="utf-8")
    assert "#SBATCH --partition=gpu" in script
    assert "#SBATCH --cpus-per-task=8" in script
    assert "--workers 8" in script
    assert "module load gpu" in script
    assert "module load batch" not in script
    assert "exec /opt/gpu-httk workflow manager run" in script
    assert bundle.exists()


@pytest.mark.parametrize(
    "settings",
    [
        {"slurm.partition": {"gpu": True}},
        {"slurm.partition": True},
        {"slurm.partition": "gpu\x00partition"},
        {"slurm-partition": "gpu"},
    ],
)
def test_launcher_settings_use_workspace_validation_rules(
    tmp_path: Path, remote: Remote, settings: dict[str, object]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-setting-validation")
    with pytest.raises(ValueError):
        add_launcher("invalid", template="slurm", settings=settings, project=project)
    add_launcher("cluster", template="slurm", project=project)
    with pytest.raises(ValueError):
        configure_launcher("cluster", settings, project=project)


@pytest.mark.parametrize("value", ["ALL", "NONE,FOO", "", "NONE --wrap x", "none", "nil", " NONE", "NONE\n"])
def test_slurm_export_rejects_bad_values_at_configure_and_add(tmp_path: Path, remote: Remote, value: str) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="slurm-export-validation")
    with pytest.raises(ValueError, match="slurm.export must be exactly"):
        add_launcher("bad", template="slurm", settings={"slurm.export": value}, project=project)
    add_launcher("cluster", template="slurm", project=project)
    with pytest.raises(ValueError, match="slurm.export must be exactly"):
        configure_launcher("cluster", {"slurm.export": value}, project=project)


def test_block_mpi_spawn_is_validated_at_configure_and_add(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="block-mpi-spawn-validation")
    setting = "manager.confine.block_mpi_spawn"
    with pytest.raises(ValueError, match=f"{setting} must be on, off or auto"):
        add_launcher("bad", template="slurm", settings={setting: "maybe"}, project=project)
    bundle = add_launcher("cluster", template="slurm", settings={setting: "off"}, project=project)
    with pytest.raises(ValueError, match=f"{setting} must be on, off or auto"):
        configure_launcher("cluster", {setting: "ON"}, project=project)
    configure_launcher("cluster", {setting: "auto"}, project=project)
    stored = json.loads((bundle / "launcher.json").read_text(encoding="utf-8"))
    assert stored["settings"][setting] == "auto"


def test_launch_mpi_is_validated_at_configure_and_add(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launch-mpi-validation")
    with pytest.raises(ValueError, match="manager.launch_mpi"):
        add_launcher("bad", template="slurm", settings={"manager.launch_mpi": "PMI 2"}, project=project)
    bundle = add_launcher("cluster", template="slurm", settings={"manager.launch_mpi": "pmi2"}, project=project)
    with pytest.raises(ValueError, match="manager.launch_mpi"):
        configure_launcher("cluster", {"manager.launch_mpi": "PMI 2"}, project=project)
    configure_launcher("cluster", {"manager.launch_mpi": "pmix"}, project=project)
    stored = json.loads((bundle / "launcher.json").read_text(encoding="utf-8"))
    assert stored["settings"]["manager.launch_mpi"] == "pmix"


@pytest.mark.parametrize("value", ["NONE", "NIL"])
def test_slurm_export_accepts_none_and_nil(tmp_path: Path, remote: Remote, value: str) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="slurm-export-accept")
    bundle = add_launcher("cluster", template="slurm", settings={"slurm.export": value}, project=project)
    stored = json.loads((bundle / "launcher.json").read_text(encoding="utf-8"))
    assert stored["settings"]["slurm.export"] == value
    configure_launcher("cluster", {"slurm.export": "NIL" if value == "NONE" else "NONE"}, project=project)


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
        "slurm.ntasks": "32",
        "slurm.ntasks_per_node": "16",
        "slurm.mem": "64G",
        "slurm.gres": "gpu:a100:2",
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
    launch_ids = cast(list[str], result["launch_ids"])
    scripts = cast(list[str], result["scripts"])
    assert len(set(launch_ids)) == 2 and all(re.fullmatch(r"[0-9a-f]{32}", launch_id) for launch_id in launch_ids)
    # One script per submission, each named after and exporting its own launch id.
    directory = workspace.root / "logs" / "launchers"
    assert scripts == [str(directory / f"{launch_id}.sbatch") for launch_id in launch_ids]
    for launch_id, path in zip(launch_ids, scripts, strict=True):
        text = Path(path).read_text(encoding="utf-8")
        assert f"export HTTK_LAUNCH_ID={launch_id}\n" in text
        assert f"#SBATCH --output={directory / launch_id}.out\n" in text
        assert Path(path).stat().st_mode & 0o777 == 0o700
    script = Path(scripts[0]).read_text(encoding="utf-8")
    for directive in (
        "--account=p2026-1",
        "--partition=main",
        "--time=04:00:00",
        "--nodes=2",
        "--cpus-per-task=16",
        "--ntasks=32",
        "--ntasks-per-node=16",
        "--mem=64G",
        "--gres=gpu:a100:2",
        "--reservation=special",
    ):
        assert f"#SBATCH {directive}" in script
    assert f"#SBATCH --chdir={workspace.root}" in script
    assert "module load python" in script
    assert f"exec httk workflow manager run --workspace {workspace.root} --allocation slurm" in script
    assert bundle.exists()


def _sbatch_lines(result: dict[str, object]) -> list[str]:
    script = Path(cast(list[str], result["scripts"])[0]).read_text(encoding="utf-8")
    return [line for line in script.splitlines() if line.startswith("#SBATCH ")]


def test_sbatch_options_are_emitted_last_one_line_per_word(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="sbatch-options")
    workspace = Workspace.initialize(tmp_path / "workspace")
    options = "--qos=high  --exclusive --comment=\"it's a #1\" --constraint=rome"
    add_launcher("cluster", template="slurm", settings={"slurm.sbatch_options": options}, project=project)
    result = start_managers(
        resolve_launcher("cluster", project=project),
        workspace_root=workspace.root,
        argv=["httk", "workflow", "manager", "run"],
        count=1,
        settings={"slurm.partition": "main"},
        timeout=None,
    )
    lines = _sbatch_lines(result)
    assert lines[:2] == ["#SBATCH --job-name=httk-manager", "#SBATCH --partition=main"]
    assert lines[2].startswith("#SBATCH --chdir=") and lines[3].startswith("#SBATCH --output=")
    assert lines[4:] == [
        "#SBATCH --qos=high",
        "#SBATCH --exclusive",
        "#SBATCH --comment='it'\"'\"'s a #1'",
        "#SBATCH --constraint=rome",
    ]
    assert shlex.split(lines[6].removeprefix("#SBATCH ")) == ["--comment=it's a #1"]


def test_launcher_sbatch_options_replace_the_workspace_value(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="sbatch-options-merge")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("plain", template="slurm", project=project)
    add_launcher("own", template="slurm", settings={"slurm.sbatch_options": "--qos=high"}, project=project)

    def start(name: str) -> list[str]:
        result = start_managers(
            resolve_launcher(name, project=project),
            workspace_root=workspace.root,
            argv=["httk", "workflow", "manager", "run"],
            count=1,
            settings={"slurm.sbatch_options": "--qos=low --exclusive"},
            timeout=None,
        )
        return _sbatch_lines(result)[3:]

    assert start("plain") == ["#SBATCH --qos=low", "#SBATCH --exclusive"]
    assert start("own") == ["#SBATCH --qos=high"]


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("--job-n=x", r"--job-n \(--job-name\): reserved by httk"),
        ("--par=gpu", r"--par \(--partition\): use slurm\.partition"),
        ("--export=ALL", r"must not set --export: reserved by httk"),
        ("--n=2", r"--n \(--nodes\): use slurm\.nodes"),
        ("--mem=4G", r"use slurm\.mem"),
        ("--time=10", r"must not set --time: use slurm\.time_limit"),
        ("--vers", r"--vers \(--version\): reserved by httk"),
        ("--help", r"must not set --help: reserved by httk"),
        ("-p gpu", "use --name=value"),
        ("high", "use --name=value"),
        ("---x", "use --name=value"),
        ("--qos=high\n--exclusive", "control characters"),
        ("--comment=a\\b", "backslashes"),
        ("--qos=", "empty value"),
        ("--qos=high --qos=low", "repeats option --qos$"),
        ("--comment='open", "not valid shell words"),
        (4, "must be a string"),
        (1.5, "must be a string"),
    ],
)
def test_sbatch_options_refusals(value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        sbatch_option_lines(value)


def test_sbatch_options_accept_options_beside_the_denied_names() -> None:
    accepted = (
        "--mem-per-cpu=2G --time-min=10 --nodelist=n1 --ntasks-per-socket=2 --gres-flags=enforce-binding "
        "--wait-all-nodes=1 --exclusive --qos=high --gpus=4 --gpus-per-node=2"
    )
    assert sbatch_option_lines(accepted) == tuple(accepted.split())
    assert sbatch_option_lines("  ") == sbatch_option_lines(None) == ()


def test_workspace_sbatch_options_are_validated_before_any_submission(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="sbatch-options-workspace")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    with pytest.raises(RuntimeError, match="must not set --time: use slurm.time_limit"):
        start_managers(
            resolve_launcher("cluster", project=project),
            workspace_root=workspace.root,
            argv=["httk", "workflow", "manager", "run"],
            count=2,
            settings={"slurm.sbatch_options": "--qos=high --time=1"},
            timeout=None,
        )
    assert not list(remote.spool.glob("*.sbatch"))
    assert not list((workspace.root / "logs" / "launchers").glob("*.sbatch"))


def test_sbatch_options_are_validated_at_configure_and_add(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="sbatch-options-validation")
    with pytest.raises(ValueError, match="slurm.sbatch_options"):
        add_launcher("bad", template="slurm", settings={"slurm.sbatch_options": "--wrap=x"}, project=project)
    add_launcher("cluster", template="slurm", project=project)
    with pytest.raises(ValueError, match="slurm.sbatch_options"):
        configure_launcher("cluster", {"slurm.sbatch_options": "--time=10"}, project=project)
    configure_launcher("cluster", {"slurm.sbatch_options": "--qos=high"}, project=project)


@pytest.mark.parametrize(
    "settings",
    [
        {"slurm.time_limit": "4:00:00", "slurm.time_limit_max": "3:59:59"},
        {"slurm.time_limit_max": "soon"},
        {"slurm.time_limit_max": "0"},
        {"slurm.time_limit": "0", "slurm.time_limit_max": "1:00:00"},
    ],
)
def test_time_limit_max_is_validated_at_configure_and_add(
    tmp_path: Path, remote: Remote, settings: dict[str, object]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="time-limit-max-validation")
    with pytest.raises(ValueError, match="slurm.time_limit"):
        add_launcher("bad", template="slurm", settings=settings, project=project)
    add_launcher("cluster", template="slurm", project=project)
    with pytest.raises(ValueError, match="slurm.time_limit"):
        configure_launcher("cluster", settings, project=project)
    configure_launcher("cluster", {"slurm.time_limit": "4:00:00", "slurm.time_limit_max": "4:00:00"}, project=project)


def test_slurm_launcher_start_applies_a_bounded_time_limit(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-time-limit")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher(
        "cluster",
        template="slurm",
        settings={"slurm.time_limit": 240, "slurm.time_limit_max": "1-0"},
        project=project,
    )
    target = resolve_launcher("cluster", project=project)
    argv = ["httk", "workflow", "manager", "run", "--workspace", str(workspace.root)]

    def start(time_limit: str | None) -> dict[str, object]:
        return start_managers(
            target,
            workspace_root=workspace.root,
            argv=argv,
            count=1,
            settings={},
            timeout=None,
            time_limit=time_limit,
        )

    with pytest.raises(RuntimeError):
        start("1-00:00:01")
    assert not list(remote.spool.glob("*.sbatch"))
    result = start("1-0")
    assert result["time_limit"] == "1-00:00:00"
    script = Path(cast(list[str], result["scripts"])[0]).read_text(encoding="utf-8")
    assert "#SBATCH --time=1-00:00:00\n" in script
    assert "--time-limit" not in script
    assert start(None)["time_limit"] == "04:00:00"


@pytest.mark.parametrize(
    ("kind", "settings", "sent"),
    [
        ("slurm", {"slurm.time_limit": "4:00:00"}, "01:30:00"),
        ("slurm", {"slurm.time_limit_max": "1-0"}, "01:30:00"),
        ("slurm", {}, None),
        ("slurm", {"slurm.time_limit": "UNLIMITED"}, None),
        ("slurm", {"slurm.time_limit": "0"}, None),
        ("pbs", {"slurm.time_limit": "4:00:00"}, None),
    ],
)
def test_cli_time_limit_becomes_the_job_time_only_for_a_bounded_slurm_launcher(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    settings: dict[str, object],
    sent: str | None,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-time-limit-cli")
    workspace = Workspace.initialize(tmp_path / "workspace")
    bundle = add_launcher("cluster", template="slurm", settings=settings, project=project)
    metadata = json.loads((bundle / "launcher.json").read_text(encoding="utf-8"))
    metadata["kind"] = kind
    (bundle / "launcher.json").write_text(json.dumps(metadata), encoding="utf-8")
    calls: list[dict[str, object]] = []

    def record(*_args: object, **kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(_manager, "start_managers", record)
    monkeypatch.setattr(_manager, "launch_processes", record)
    options = {**_manager.manager_option_defaults(), "time_limit": "90"}
    context = CLIContext("httk", project)
    _manager.launch_workspace_managers(
        workspace.root, argparse.Namespace(**{**options, "launcher": "cluster"}), context
    )
    _manager.launch_workspace_managers(workspace.root, argparse.Namespace(**{**options, "detach": True}), context)
    launcher, process = calls
    assert launcher["time_limit"] == sent
    assert ("--time-limit" in cast(list[str], launcher["argv"])) == (sent is None)
    assert "time_limit" not in process
    assert cast(list[str], process["argv"])[-2:] == ["--time-limit", "90"]


def test_a_start_result_without_the_requested_time_limit_warns(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="launcher-time-limit-unconfirmed")
    add_launcher("cluster", template="slurm", settings={"slurm.time_limit": "4:00:00"}, project=project)
    target = resolve_launcher("cluster", project=project)
    # An older httk behind the launcher ignores the member and reports no time_limit.
    monkeypatch.setattr(launchers, "run_launcher", lambda *_args, **_kwargs: {"ok": True})
    with caplog.at_level(logging.WARNING, logger="httk.workflow.launchers"):
        start_managers(
            target, workspace_root=tmp_path, argv=["httk"], count=1, settings={}, timeout=None, time_limit="01:00:00"
        )
        assert [record.levelno for record in caplog.records if record.name == "httk.workflow.launchers"] == [
            logging.WARNING
        ]
        caplog.clear()
        start_managers(target, workspace_root=tmp_path, argv=["httk"], count=1, settings={}, timeout=None)
        assert not caplog.records


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
    launch_ids = cast(list[str], result.pop("launch_ids"))
    assert result == {"ok": True, "kind": "process", "count": 2, "pids": [701, 702]}
    assert len(set(launch_ids)) == 2 and all(re.fullmatch(r"[0-9a-f]{32}", launch_id) for launch_id in launch_ids)
    assert all(call["start_new_session"] is True for call in calls)
    assert all(call["stdin"] is subprocess.DEVNULL for call in calls)
    for launch_id, call in zip(launch_ids, calls, strict=True):
        # Standard output and error both go to the manager's launcher log, which the parent closed again.
        output = cast(IO[bytes], call["stdout"])
        assert call["stderr"] is output and output.closed and output.mode == "ab"
        assert Path(output.name) == tmp_path.resolve() / "logs" / "launchers" / f"{launch_id}.out"
        assert cast(dict[str, str], call["env"])["HTTK_LAUNCH_ID"] == launch_id
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
    # Two managers split the host, so neither may bind to its devices.
    assert all("--allocation none" in " ".join(argv) for argv in launched_argvs)


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
        launch_runtime._batch_script(
            argv, settings={"manager.command": 7}, workspace="/ws", directory="/ws/.batch", launch_id="0" * 32
        )


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


def test_the_daemon_template_is_gone(tmp_path: Path, remote: Remote) -> None:
    with pytest.raises(ValueError, match="unknown maintained launcher template: daemon"):
        add_launcher("small", template="daemon", global_=True)
    assert not (Path(os.environ["HTTK_CONFIG_HOME"]) / "launchers" / "small").exists()


def _stored(name: str) -> dict[str, object]:
    return json.loads((Path(os.environ["HTTK_CONFIG_HOME"]) / "launchers" / name / "launcher.json").read_text())[
        "settings"
    ]


def test_launcher_configure_add_path(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _launcher

    monkeypatch.setattr(_launcher, "default_readonly_paths", lambda: (Path("/usr"), Path("/opt/py")))
    project = tmp_path / "project"
    initialize_project(project, name="add-path")
    context = CLIContext("httk", project)
    assert command(["launcher", "add", "--template", "slurm", "--global", "small"], context) == 0
    capsys.readouterr()
    args = ["launcher", "configure", "small", "--add-path"]
    assert command([*args, "confine.readonly_paths=/software"], context) == 0
    assert _stored("small")["confine.readonly_paths"] == "/usr:/opt/py:/software"
    assert capsys.readouterr().out.splitlines()[-1] == "confine.readonly_paths=/usr:/opt/py:/software"
    assert command(["launcher", "configure", "small", "--set", "confine.readonly_paths=/a:/b"], context) == 0
    assert command([*args, "confine.readonly_paths=/b:/c"], context) == 0
    assert _stored("small")["confine.readonly_paths"] == "/a:/b:/c"
    assert command([*args, "confine.devices=/dev/x", "--set", "slurm.partition=p"], context) == 0
    assert _stored("small")["confine.devices"] == "/dev/x"
    assert _stored("small")["slurm.partition"] == "p"
    assert command([*args, "confine.pmix_roots=/var/spool/slurmd"], context) == 0
    assert _stored("small")["confine.pmix_roots"] == "/var/spool/slurmd"
    assert command([*args, "confine.readonly_paths=relative"], context) != 0
    assert re.search("absolute", capsys.readouterr().err)
    for refused in ("slurm.partition=x", "confine.bwrap=/usr/bin/bwrap", "confine.readonly_paths"):
        assert command([*args, refused], context) != 0
        assert re.search("--add only applies.*confine.readonly_paths", capsys.readouterr().err)
    assert _stored("small")["confine.readonly_paths"] == "/a:/b:/c"


def test_launcher_configuration_removals(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow._confine import confine_settings, default_readonly_paths
    from httk.workflow.workflow_cli import _launcher

    project = tmp_path / "project"
    initialize_project(project, name="remove-settings")
    context = CLIContext("httk", project)
    bundle = add_launcher("small", template="slurm", global_=True)
    configure = ["launcher", "configure", "small"]
    monkeypatch.setattr(_launcher, "default_readonly_paths", lambda: (Path("/usr"),))
    assert command([*configure, "--remove", "confine.readonly_paths=/usr"], context) == 0
    assert _stored("small")["confine.readonly_paths"] == ""
    assert confine_settings(_stored("small")).readonly_paths == ()
    assert command([*configure, "--unset", "confine.readonly_paths"], context) == 0
    assert "confine.readonly_paths" not in _stored("small")
    assert confine_settings(_stored("small")).readonly_paths == default_readonly_paths()
    assert command([*configure, "--add", "confine.devices=/dev/a:/dev/a:/dev/b"], context) == 0
    assert _stored("small")["confine.devices"] == "/dev/a:/dev/b"
    assert command([*configure, "--remove-path", "confine.devices=/dev/a:/dev/b"], context) == 0
    assert confine_settings(_stored("small")).devices == ()
    assert command([*configure, "--add-path", "confine.devices=/dev/c"], context) == 0
    assert _stored("small")["confine.devices"] == "/dev/c"
    before = (bundle / "launcher.json").read_bytes()
    for invalid in ("confine.devices=relative", "confine.devices=/dev/a:", "confine.devices=", "slurm.partition=x"):
        assert command([*configure, "--set", "slurm.partition=new", "--remove", invalid], context) == 1
        assert (bundle / "launcher.json").read_bytes() == before
    assert command([*configure, "--unset", "bad=key"], context) == 1
    assert (bundle / "launcher.json").read_bytes() == before
    assert command([*configure, "--unset", "confine.devices", "--set", "confine.devices=/dev/d"], context) == 0
    assert _stored("small")["confine.devices"] == "/dev/d"
    capsys.readouterr()
    assert command(["launcher", "list", "--json"], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["name"] == "small"
    assert command(["launcher", "list"], context) == 0
    assert capsys.readouterr().out.startswith("small\tglobal\t")
