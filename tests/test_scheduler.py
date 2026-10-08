"""Scheduler lookup and the maintained Slurm launch hooks."""

from httk.workflow._allocation import Allocation, Node, allocation_from_envelope
from httk.workflow._manager_binding import NodeShare, Placement
from httk.workflow._scheduler import detect_scheduler, scheduler_for
from httk.workflow._slurm import SLURM, slurm_allocation


def _placement(*shares: NodeShare) -> Placement:
    return Placement(tuple(shares), None)


def test_scheduler_lookup_has_one_maintained_slurm_entry() -> None:
    assert scheduler_for("slurm") is SLURM
    assert scheduler_for("host") is None
    assert detect_scheduler({"SLURM_JOB_ID": "42"}) is SLURM
    assert detect_scheduler({}) is None


def test_slurm_step_argv_uses_placement_and_normalized_cpu_metadata() -> None:
    placement = _placement(NodeShare("n01", 2, 2048, 0, None, None, False))
    assert SLURM.step_argv(placement, nodefile="/tmp/nodes", gpus_present=True, cpus_per_proc=2) == [
        "env",
        "SLURM_HOSTFILE=/tmp/nodes",
        "srun",
        "--ntasks=2",
        "--distribution=arbitrary",
        "--exact",
        "--cpus-per-task=2",
        "--mem=2048M",
        "--gres=none",
    ]


def test_slurm_step_argv_puts_the_mpi_plugin_after_srun() -> None:
    placement = _placement(NodeShare("n01", 2, None, 0, None, None, False))
    argv = SLURM.step_argv(placement, nodefile="/tmp/nodes", mpi="pmi2")
    assert argv is not None
    assert argv[:5] == ["env", "SLURM_HOSTFILE=/tmp/nodes", "srun", "--mpi=pmi2", "--ntasks=2"]


def test_slurm_step_argv_skips_zero_task_placement() -> None:
    placement = _placement(NodeShare("n01", 0, None, 0, None, None, False))
    assert SLURM.step_argv(placement, nodefile="/tmp/nodes") is None


def test_slurm_step_argv_skips_gpu_only_placement() -> None:
    placement = _placement(NodeShare("n01", 0, None, 1, None, None, False))
    assert SLURM.step_argv(placement, nodefile="/tmp/nodes") is None


def test_slurm_step_argv_rejects_gpu_share_mixed_with_zero_cpu_share() -> None:
    placement = _placement(
        NodeShare("n01", 1, None, 0, None, None, False),
        NodeShare("n02", 0, None, 1, None, None, False),
    )
    try:
        SLURM.step_argv(placement, nodefile="/tmp/nodes")
    except ValueError as exc:
        assert "GPU share" in str(exc)
    else:
        raise AssertionError("mixed GPU-only share must not launch")


def test_slurm_allocation_keeps_normalized_cpus_per_proc() -> None:
    environ = {
        "SLURM_JOB_ID": "42",
        "SLURM_JOB_NUM_NODES": "1",
        "SLURM_NTASKS": "2",
        "SLURM_CPUS_PER_TASK": "4",
        "SLURMD_NODENAME": "n01",
    }
    allocation = slurm_allocation(environ)
    assert allocation is not None
    assert allocation == Allocation("slurm", None, (Node("n01", 2, local=True),), {}, 4, {"job_id": "42"})
    environ["SLURM_CPUS_PER_TASK"] = "8"
    assert allocation.cpus_per_proc == 4


def test_slurm_aggregate_allocation_keeps_normalized_cpus_per_proc() -> None:
    allocation = slurm_allocation(
        {
            "SLURM_JOB_ID": "42",
            "SLURM_JOB_NUM_NODES": "2",
            "SLURM_NTASKS": "4",
            "SLURM_CPUS_PER_TASK": "3",
        }
    )
    assert allocation is not None and allocation.nodes == () and allocation.cpus_per_proc == 3


def test_envelope_accepts_cpu_metadata_and_local_identity() -> None:
    envelope = {
        "format": "httk-workflow-allocation",
        "format_version": 1,
        "kind": "pbs",
        "nodes": [
            {
                "host": "n01",
                "procs": 1,
                "cpus": ["0-3"],
                "local": True,
            },
            {"host": "n02", "procs": 1, "cpus": ["0-3"]},
        ],
        "cpus_per_proc": 2,
    }
    allocation = allocation_from_envelope(envelope)
    assert allocation.cpus_per_proc == 2
    assert allocation.nodes[0].local
