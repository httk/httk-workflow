"""Two-layer shell prelude: storage, the executor wrap, and the launcher batch tail.

A prelude is shell text an operator configures to initialize the environment
before a job runs — ``module load VASP/6.2.1`` and the like. Layer 2 is a
workspace-side map keyed by workflow id, applied by the executor per launch;
Layer 1 is the ``environment.prelude`` application setting, applied by the
launcher. These tests cover the workspace round-trip and validation, the pure
executor wrap, and the slurm batch-script tail.
"""

import sys
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import Workspace
from httk.workflow.executors import AttemptLaunch, PathRunnerExecutor
from httk.workflow.launch_runtime import _batch_script
from httk.workflow.models import Marker
from httk.workflow.protocol import JobSpec, prepare_job_payload


def test_workflow_prelude_round_trip_and_validation(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")

    workspace.set_workflow_prelude("relax-vasp", "module load VASP/6.2.1")
    assert workspace.read_workflow_preludes()["relax-vasp"] == "module load VASP/6.2.1"

    # A fresh instance on the same root reads the persisted map.
    assert Workspace(workspace.root).read_workflow_preludes()["relax-vasp"] == "module load VASP/6.2.1"

    # A workspace that has never stored a prelude reads an empty map.
    assert Workspace.initialize(tmp_path / "empty").read_workflow_preludes() == {}

    assert workspace.unset_workflow_prelude("relax-vasp") == {}
    assert Workspace(workspace.root).read_workflow_preludes() == {}
    with pytest.raises(ValueError):
        workspace.unset_workflow_prelude("relax-vasp")

    with pytest.raises(ValueError):
        workspace.set_workflow_prelude("has space", "module load VASP")
    with pytest.raises(ValueError):
        workspace.set_workflow_prelude("relax-vasp", 7)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        workspace.set_workflow_prelude("relax-vasp", "bad\0value")


def _launch(tmp_path: Path, prelude: str) -> AttemptLaunch:
    payload = tmp_path / "payload"
    payload.mkdir(exist_ok=True)
    runner = payload / "runner.sh"
    if not runner.exists():
        runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Prelude",
            workflow="tests.prelude",
            runner_path="runner.sh",
            initial_step="only",
            data_mode="none",
        ),
    )
    control = tmp_path / "control"
    control.mkdir(exist_ok=True)
    marker = Marker(
        kind="running",
        placement=PurePosixPath("project/prelude"),
        job_key=job.job_key,
        priority=0,
        generation=0,
        record_ref="r",
        path=control / "marker",
    )
    return AttemptLaunch(
        job=job,
        marker=marker,
        payload=payload,
        workdir=tmp_path / "workdir",
        control=control,
        context={},
        workflow_prelude=prelude,
    )


@pytest.mark.parametrize("prelude", ["", "   \n "])
def test_executor_without_prelude_returns_plain_argv(tmp_path: Path, prelude: str) -> None:
    launch = _launch(tmp_path, prelude)
    base = [str(launch.runner_command), *launch.job.runner_arguments]
    assert list(PathRunnerExecutor().command(launch)) == base
    assert not (launch.control / "prelude.sh").exists()


def test_executor_with_prelude_wraps_in_login_shell(tmp_path: Path) -> None:
    launch = _launch(tmp_path, "module load VASP/6.2.1")
    base = [str(launch.runner_command), *launch.job.runner_arguments]
    script = launch.control / "prelude.sh"
    assert list(PathRunnerExecutor().command(launch)) == ["bash", "-l", str(script), *base]
    text = script.read_text(encoding="utf-8")
    assert text.startswith("set -e\n")
    assert "module load VASP/6.2.1" in text
    assert text.endswith('exec "$@"\n')


def test_batch_script_runs_prelude_before_exec_under_set_e() -> None:
    argv = [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run", "--by-path"]
    script = _batch_script(
        argv,
        settings={"environment.prelude": "module load VASP/6.2.1"},
        workspace="/ws",
        directory="/ws/.batch",
    )
    assert script.startswith("#!/bin/bash -l\n")
    assert "set -e\n" in script and "set -eu" not in script
    prelude_at = script.index("module load VASP/6.2.1")
    exec_at = script.index("exec ")
    assert prelude_at < exec_at
    assert "exec httk workflow manager run --by-path" in script
    assert sys.executable not in script


def test_batch_script_uses_configured_manager_command_after_prelude() -> None:
    argv = [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"]
    script = _batch_script(
        argv,
        settings={"environment.prelude": "module load python", "manager.command": "/opt/httk with space"},
        workspace="/ws",
        directory="/ws/.batch",
    )
    assert "exec '/opt/httk with space' workflow manager run" in script


def test_batch_script_without_prelude_omits_it() -> None:
    argv = [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"]
    script = _batch_script(argv, settings={}, workspace="/ws", directory="/ws/.batch")
    assert "module load" not in script
    assert "set -e\n" in script and "set -eu" not in script
    assert f"exec {sys.executable} -m httk.core.cli workflow manager run" in script


def test_batch_script_quotes_slurm_values_and_paths_with_spaces() -> None:
    script = _batch_script(
        [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"],
        settings={"slurm.partition": "main cluster"},
        workspace="/ws with space",
        directory="/ws with space/.httk-workspace/batch",
    )
    assert "#SBATCH --partition='main cluster'" in script
    assert "#SBATCH --chdir='/ws with space'" in script
    assert "#SBATCH --output='/ws with space/.httk-workspace/batch/manager-%j.out'" in script
    assert "#SBATCH --error='/ws with space/.httk-workspace/batch/manager-%j.err'" in script
