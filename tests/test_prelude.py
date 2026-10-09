"""Two-layer shell prelude: storage, the executor wrap, and the launcher batch tail.

A prelude is shell text an operator configures to initialize the environment
before a job runs — ``module load VASP/6.2.1`` and the like. Layer 2 is a
workspace-side map keyed by workflow id, applied by the executor per launch;
Layer 1 is the ``environment.prelude`` application setting, applied by the
launcher. These tests cover the workspace round-trip and validation, the
manager's wrap of the runner command, and the slurm batch-script tail.
"""

import os
import shlex
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow._job import JobDefinition
from httk.workflow.launch_runtime import _batch_script
from httk.workflow.runtime_builders import JobSpec


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


def _wrap(tmp_path: Path, prelude: str) -> tuple[list[str], Path]:
    """Return what the manager launches for a job of ``tests.prelude`` with *prelude* stored, and its control dir."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    if prelude:
        workspace.set_workflow_prelude("tests.prelude", prelude)
    job = JobDefinition.from_mapping(
        JobSpec(name="Prelude", workflow_id="local:tests.prelude", workflow_name="tests.prelude").as_mapping()
    )
    control = tmp_path / "control"
    control.mkdir(exist_ok=True)
    manager = SimpleNamespace(workspace=workspace)
    return TaskManager._with_prelude(manager, control, job, ["runner", "--flag"]), control  # type: ignore[arg-type]


@pytest.mark.parametrize("prelude", ["", "   \n "])
def test_a_job_without_a_prelude_runs_the_plain_argv(tmp_path: Path, prelude: str) -> None:
    command, control = _wrap(tmp_path, prelude.strip() and prelude)
    assert command == ["runner", "--flag"]
    assert not (control / "prelude.sh").exists()


def test_a_workflow_prelude_wraps_the_runner_in_a_login_shell(tmp_path: Path) -> None:
    command, control = _wrap(tmp_path, "module load VASP/6.2.1")
    script = control / "prelude.sh"
    assert command == ["bash", "-l", str(script), "runner", "--flag"]
    text = script.read_text(encoding="utf-8")
    assert text.startswith(f'set -e\nexport PATH={shlex.quote(os.path.dirname(sys.executable))}:"$PATH"\nmodule load')
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
        directory="/ws with space/logs/batch",
    )
    assert "#SBATCH --partition='main cluster'" in script
    assert "#SBATCH --chdir='/ws with space'" in script
    assert "#SBATCH --output='/ws with space/logs/batch/manager-%j.out'" in script
    assert "#SBATCH --error='/ws with space/logs/batch/manager-%j.err'" in script
