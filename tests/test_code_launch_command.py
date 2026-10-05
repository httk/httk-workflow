"""Tests for :func:`httk.workflow.codes.launch_command`."""

import pytest

from httk.workflow.codes import launch_command

LAUNCHERS = ["srun", "mpirun", "mpiexec", "mpiexec.hydra", "orterun", "mpprun", "aprun", "jsrun", "ibrun"]


@pytest.fixture(autouse=True)
def _no_prefix(monkeypatch):
    monkeypatch.delenv("HTTK_WORKFLOW_LAUNCH", raising=False)


@pytest.mark.parametrize("value", [None, "", "  "])
def test_no_prefix_passthrough(monkeypatch, value):
    if value is not None:
        monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", value)
    assert launch_command(["vasp_std", "-x"]) == ["vasp_std", "-x"]


def test_prefix_prepended_with_quoting(monkeypatch):
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "env 'A=b c' srun --ntasks=4")
    assert launch_command(["vasp_std"]) == ["env", "A=b c", "srun", "--ntasks=4", "vasp_std"]


def test_launch_false_passthrough(monkeypatch):
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "srun -n 4")
    assert launch_command(["mpirun", "x"], launch=False) == ["mpirun", "x"]


@pytest.mark.parametrize("word", [*LAUNCHERS, "/usr/bin/srun"])
def test_launcher_refused_only_with_prefix(monkeypatch, word):
    assert launch_command([word, "x"]) == [word, "x"]
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "srun -n 4")
    with pytest.raises(ValueError, match="manager.launch_template"):
        launch_command([word, "x"])


def test_empty_argv():
    with pytest.raises(ValueError):
        launch_command([])
