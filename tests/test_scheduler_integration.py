"""Scheduler hooks reach manager binding without scheduler-specific manager code."""

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace, _scheduler
from httk.workflow._allocation import Allocation, Node, Run, probe_allocation
from httk.workflow._manager_binding import Placement, assign
from httk.workflow.workflow_cli import _manager


def test_scheduler_hooks_supply_capacity_and_normalized_step_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    allocation = Allocation("example", 2_000_000_000, (Node("compute-alias", 2, local=True),), cpus_per_proc=4)

    class ExampleScheduler:
        name = "example"

        def detects(self, environ: Mapping[str, str]) -> bool:
            return "EXAMPLE_JOB" in environ

        def counts(self, environ: Mapping[str, str]) -> dict[str, int]:
            return allocation.capacity()

        def end_time(self, environ: Mapping[str, str]) -> float | None:
            return allocation.end_time

        def probe_allocation(self, environ: Mapping[str, str], *, run: Run, cpu_slots: bool = False) -> Allocation:
            return allocation

        def step_argv(
            self,
            placement: Placement,
            *,
            nodefile: str,
            gpus_present: bool = False,
            cpus_per_proc: int = 1,
            mem: int | None = None,
            mpi: str | None = None,
        ) -> list[str]:
            return ["example-run", "--cpus", str(cpus_per_proc), "--nodefile", nodefile]

        def allocation_ended(self, identity: Mapping[str, str], *, run: Run, timeout: float) -> bool | None:
            return None

    scheduler = ExampleScheduler()
    monkeypatch.setattr(_scheduler, "_maintained", lambda: (scheduler,))
    environment = {"EXAMPLE_JOB": "123"}
    # The probe records the scheduler it asked, so launch records can ask it again.
    assert probe_allocation("auto", environment) == replace(allocation, probe="example")
    assert probe_allocation("example", environment) == replace(allocation, probe="example")
    assert _manager._scheduler_resources(environment) == allocation.capacity()
    assert _scheduler.detect_scheduler(environment) is scheduler
    assert scheduler.end_time(environment) == allocation.end_time

    # A stale foreign environment value must not override the probe snapshot.
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "99")
    workspace = Workspace.initialize(tmp_path / "workspace")
    control, launch = tmp_path / "control", tmp_path / "launch"
    control.mkdir()
    launch.mkdir()
    with TaskManager(workspace, allocation=allocation, end_time=allocation.end_time) as manager:
        assert manager._inventory is not None
        placement = assign(manager._inventory, {"procs": 1})
        assert placement is not None
        binding, _, _ = manager._attempt_binding(placement, control, launch, {}, None)
        assert binding["launch"] == ["example-run", "--cpus", "4", "--nodefile", str(launch / "nodefile")]
        assert (launch / "nodefile").read_text() == "compute-alias\n"
        assert manager._local_share(placement) is not None
