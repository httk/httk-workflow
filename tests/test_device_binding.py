"""GPU visibility export and opt-in CPU pinning for locally executed attempts."""

import logging
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from httk.workflow import TaskManager
from httk.workflow._allocation import (
    Allocation,
    Node,
    allocation_from_envelope,
    bind_cpus_setting,
    format_cpulist,
    host_allocation,
    local_cpu_slots,
    parse_cpulist,
    probe_allocation,
)
from httk.workflow._manager_binding import Inventory, NodeShare, Placement
from httk.workflow._slurm import slurm_allocation
from httk.workflow.errors import FormatError
from test_binding import HOST, _Campaign

_DEVICES = """
seen = {
    "gpu": os.environ.get("CUDA_VISIBLE_DEVICES"),
    "ze": os.environ.get("ZE_AFFINITY_MASK"),
    "cpus": sorted(os.sched_getaffinity(0)),
}
Path(os.environ["HTTK_WORKFLOW_WORKDIR"], "seen.json").write_text(json.dumps(seen))
go = Path(os.environ["HTTK_WORKFLOW_WORKDIR"], "go")
while not go.exists():
    time.sleep(0.02)
publish("succeed")
"""

_AFFINITY = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
_PINNABLE = pytest.mark.skipif(
    not hasattr(os, "sched_setaffinity") or len(_AFFINITY) < 2, reason="needs sched_setaffinity and two CPUs"
)


class _Devices(_Campaign):
    body = _DEVICES


def _manager(campaign: _Campaign, *nodes: Node, workers: int = 2) -> TaskManager:
    allocation = Allocation("host", None, nodes, {})
    return TaskManager(
        campaign.workspace,
        resources=allocation.capacity(),
        maximum_workers=workers,
        allocation=allocation,
        heartbeat_interval=0.01,
    )


def test_bind_cpus_setting_truthiness() -> None:
    for value in (True, 1, "true", "TRUE", "1", "Yes"):
        assert bind_cpus_setting({"manager.bind_cpus": value})
    for other in (False, "false", "0", "no", "on", 0, 2, 1.0, None, ""):
        assert not bind_cpus_setting({"manager.bind_cpus": other})
    assert not bind_cpus_setting({})


def test_cpulists_render_compactly_and_parse_back() -> None:
    assert format_cpulist([8, 0, 1, 2, 3]) == "0-3,8"
    assert format_cpulist([5]) == "5"
    assert parse_cpulist("0-3,8") == {0, 1, 2, 3, 8}
    for bad in ("7-3", "0,7-3", "x", ""):
        with pytest.raises(ValueError):
            parse_cpulist(bad)


def test_probes_discover_local_slots_only_when_asked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {0, 1, 2, 3, 4, 5, 6, 7}, raising=False)
    assert local_cpu_slots(4) == ("0-1", "2-3", "4-5", "6-7")
    assert local_cpu_slots(3) == ("0-1", "2-3", "4-5")
    assert local_cpu_slots(9) is None and local_cpu_slots(0) is None
    assert host_allocation({}).nodes[0].cpu_slots is None
    assert host_allocation({}, cpu_slots=True).nodes[0].cpu_slots == tuple(str(cpu) for cpu in range(8))
    assert probe_allocation("host", {}, cpu_slots=True).nodes[0].cpu_slots is not None  # type: ignore[union-attr]

    def scontrol(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, "n1 n2\n", "")

    environ = {
        "SLURM_JOB_ID": "1",
        "SLURM_NTASKS": "8",
        "SLURM_JOB_NUM_NODES": "2",
        "SLURM_JOB_NODELIST": "n[1-2]",
        "SLURM_TASKS_PER_NODE": "4(x2)",
        "SLURMD_NODENAME": "n2",
    }
    plain = slurm_allocation(environ, run=scontrol)
    assert plain is not None and all(node.cpu_slots is None for node in plain.nodes)
    bound = slurm_allocation(environ, run=scontrol, cpu_slots=True)
    assert bound is not None
    assert [node.cpu_slots for node in bound.nodes] == [None, ("0-1", "2-3", "4-5", "6-7")]


def test_only_a_single_share_on_this_host_is_local() -> None:
    nodes = (Node("n1", 1), Node("n1.cluster.example", 1), Node("n2", 1), Node("batch", 1, local=True))
    allocation = Allocation("slurm", None, nodes, {})
    inventory = Inventory.from_allocation(allocation, allocation.capacity())
    manager = SimpleNamespace(hostname="n1.cluster.example", _inventory=inventory)

    def share(host: str) -> NodeShare:
        return NodeShare(host, 1, None, 0, None, None, False)

    local = TaskManager._local_share
    assert local(manager, Placement((share("n1"),), None)) == (share("n1"), nodes[0])  # type: ignore[arg-type]
    assert local(manager, Placement((share("n1.cluster.example"),), None)) is not None  # type: ignore[arg-type]
    assert local(manager, Placement((share("batch"),), None)) is not None  # type: ignore[arg-type]
    assert local(manager, Placement((share("n2"),), None)) is None  # type: ignore[arg-type]
    assert local(manager, Placement((share("n1"), share("n2")), None)) is None  # type: ignore[arg-type]


def test_envelope_cpus_must_ascend() -> None:
    node = {"host": "a", "procs": 1, "cpus": ["7-3"]}
    envelope = {"format": "httk-workflow-allocation", "format_version": 1, "kind": "site", "nodes": [node]}
    with pytest.raises(FormatError, match="ascending"):
        allocation_from_envelope(envelope)
    assert allocation_from_envelope({**envelope, "nodes": [{**node, "cpus": ["3-7"]}]}).nodes[0].cpu_slots == ("3-7",)


@pytest.mark.timing
def test_local_gpu_shares_see_exactly_their_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "inherited")
    campaign = _Devices(tmp_path)
    campaign.submit("first", gpus=1)
    campaign.submit("second", gpus=1)
    allocation = host_allocation({"CUDA_VISIBLE_DEVICES": "GPU-a,GPU-b"})
    with TaskManager(
        campaign.workspace,
        resources=allocation.capacity(),
        maximum_workers=2,
        allocation=allocation,
        heartbeat_interval=0.01,
    ) as manager:
        ids = {campaign.seen(manager, tag)["gpu"] for tag in ("first", "second")}
        assert ids == {"GPU-a", "GPU-b"}
        campaign.finish(manager, "first", "second")
        # No GPUs asked means none seen, not the ones given to others.
        campaign.submit("cpu", gpus=0)
        assert campaign.seen(manager, "cpu")["gpu"] == ""
        campaign.finish(manager, "cpu")


@pytest.mark.timing
def test_an_empty_level_zero_mask_is_never_exported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZE_AFFINITY_MASK", "inherited")
    campaign = _Devices(tmp_path)
    campaign.submit("cpu", gpus=0)
    allocation = host_allocation({"ZE_AFFINITY_MASK": "0,1"})
    with TaskManager(
        campaign.workspace, resources=allocation.capacity(), allocation=allocation, heartbeat_interval=0.01
    ) as manager:
        assert campaign.seen(manager, "cpu")["ze"] == "inherited"
        campaign.finish(manager, "cpu")


@pytest.mark.timing
def test_the_slurm_batch_host_is_local_by_its_node_name(tmp_path: Path) -> None:
    campaign = _Devices(tmp_path)
    campaign.submit("gpu", gpus=1)
    environ = {
        "SLURM_JOB_ID": "1",
        "SLURM_NTASKS": "2",
        "SLURM_GPUS": "1",
        "SLURM_JOB_NUM_NODES": "1",
        "SLURMD_NODENAME": "slurm-name-of-this-host",
        "CUDA_VISIBLE_DEVICES": "GPU-s",
    }
    allocation = slurm_allocation(environ)
    assert allocation is not None and allocation.nodes[0].local
    with TaskManager(
        campaign.workspace, resources=allocation.capacity(), allocation=allocation, heartbeat_interval=0.01
    ) as manager:
        assert campaign.seen(manager, "gpu")["gpu"] == "GPU-s"
        campaign.finish(manager, "gpu")


@pytest.mark.timing
def test_a_share_on_another_host_gets_no_gpu_export(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "inherited")
    campaign = _Devices(tmp_path)
    campaign.submit("remote", gpus=1)
    node = Node("other-node", 2, None, 1, gpu_ids=("GPU-z",), gpu_variable="CUDA_VISIBLE_DEVICES")
    with _manager(campaign, node) as manager:
        assert campaign.seen(manager, "remote")["gpu"] == "inherited"
        campaign.finish(manager, "remote")


@_PINNABLE
@pytest.mark.timing
@pytest.mark.parametrize("bind", [True, False])
def test_bind_cpus_pins_a_local_share_to_its_slots(tmp_path: Path, bind: bool) -> None:
    campaign = _Devices(tmp_path)
    if bind:
        campaign.workspace.set_setting("manager.bind_cpus", "true")
    campaign.submit("pinned", procs=1)
    slots = (str(_AFFINITY[0]), str(_AFFINITY[1]))
    with _manager(campaign, Node(HOST, 2, cpu_slots=slots)) as manager:
        expected = [_AFFINITY[0]] if bind else _AFFINITY
        assert campaign.seen(manager, "pinned")["cpus"] == expected
        campaign.finish(manager, "pinned")


@_PINNABLE
@pytest.mark.timing
def test_a_failed_pin_warns_and_runs_unpinned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def refuse(pid: int, cpus: object) -> None:
        raise OSError(22, "Invalid argument")

    monkeypatch.setattr(os, "sched_setaffinity", refuse)
    campaign = _Devices(tmp_path)
    campaign.workspace.set_setting("manager.bind_cpus", "yes")
    campaign.submit("unpinned", procs=1)
    slots = (str(_AFFINITY[0]), str(_AFFINITY[1]))
    with (
        caplog.at_level(logging.WARNING, logger="httk.workflow"),
        _manager(campaign, Node(HOST, 2, cpu_slots=slots)) as manager,
    ):
        assert campaign.seen(manager, "unpinned")["cpus"] == _AFFINITY
        campaign.finish(manager, "unpinned")
    assert any("cannot pin attempt" in record.getMessage() for record in caplog.records)
