"""Node inventory placement and the binding information an attempt is given."""

import copy
import json
import random
import socket
import sys
import time
from dataclasses import replace
from itertools import product
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace, launchers
from httk.workflow._allocation import Allocation, Node
from httk.workflow._manager_binding import (
    Inventory,
    NodeShare,
    Placement,
    _Free,
    assign,
    can_assign,
    describe,
    fits,
    nodefile_lines,
    release,
    render_launch,
)
from httk.workflow._manager_scheduling import unplaceable_resource
from httk.workflow.runtime import AttemptContext
from test_manager_scheduling import _payload, _publish_cancel
from test_maxtime import _HEADER

HOST = socket.gethostname()


def _inventory(*nodes: Node, **capacity: int) -> Inventory:
    allocation = Allocation("test", None, nodes, {})
    inventory = Inventory.from_allocation(allocation, {**allocation.capacity(), **capacity})
    assert inventory is not None
    return inventory


def _hosts(placement: Placement | None) -> list[tuple[str, int, int, int | None]]:
    assert placement is not None
    return [(share.host, share.procs, share.gpus, share.mem) for share in placement.nodes]


_TWO = (Node("a", 4, 4000), Node("b", 8, 8000))


@pytest.mark.parametrize(
    ("requirement", "expected"),
    [
        # Best fit: the node with the fewest free procs that holds it.
        ({"procs": 2, "mem": 100}, [("a", 2, 0, 100)]),
        ({"procs": 6}, [("b", 6, 0, None)]),
        # Spill filled from the largest node, listed in inventory order, mem split by procs.
        ({"procs": 10, "mem": 1000}, [("a", 2, 0, 200), ("b", 8, 0, 800)]),
        ({"procs": 13}, None),
        # Whole nodes: the smallest idle ones that hold the requirement.
        ({"nodes": 1}, [("a", 4, 0, 4000)]),
        ({"nodes": 1, "procs": 6}, [("b", 8, 0, 8000)]),
        ({"nodes": 2}, [("a", 4, 0, 4000), ("b", 8, 0, 8000)]),
        ({"nodes": 3}, None),
        # Nothing placed: no procs on the node with the most free ones.
        ({}, [("b", 0, 0, None)]),
    ],
)
def test_assign_places_by_the_rules(requirement: dict[str, int], expected: object) -> None:
    inventory = _inventory(*_TWO)
    placement = assign(inventory, requirement)
    assert (None if placement is None else _hosts(placement)) == expected


def test_whole_node_fallback_finds_a_noncontiguous_feasible_set() -> None:
    inventory = _inventory(Node("a", 2, 100, 4), Node("b", 4, 100), Node("c", 8, 100))
    placement = assign(inventory, {"nodes": 2, "procs": 10, "gpus": 4})
    assert _hosts(placement) == [("a", 2, 4, 100), ("c", 8, 0, 100)]


def test_spill_skips_nodes_whose_memory_cannot_hold_a_share() -> None:
    inventory = _inventory(Node("a", 8, 0), Node("b", 4, 100), Node("c", 4, 100))
    placement = assign(inventory, {"procs": 8, "mem": 100})
    assert _hosts(placement) == [("b", 4, 0, 50), ("c", 4, 0, 50)]


def test_gpu_shares_need_a_reserved_processor_slot() -> None:
    inventory = _inventory(Node("a", 4, 100), Node("b", 0, 100, 1))
    assert assign(inventory, {"procs": 4, "gpus": 1}) is None

    inventory = _inventory(Node("a", 4, 100), Node("b", 1, 100, 1))
    placement = assign(inventory, {"procs": 4, "gpus": 1})
    assert _hosts(placement) == [("a", 3, 0, None), ("b", 1, 1, None)]
    assert placement is not None
    assert len(nodefile_lines(placement)) == 4


def test_gpu_nodes_without_memory_capacity_are_skipped() -> None:
    inventory = _inventory(Node("a", 2, 0, 2), Node("b", 2, 10, 1), Node("c", 2, 10, 0))
    placement = assign(inventory, {"procs": 3, "gpus": 1, "mem": 10})
    assert _hosts(placement) == [("b", 1, 1, 4), ("c", 2, 0, 6)]


def _oracle_fits(nodes: tuple[Node, ...], procs: int, gpus: int, mem: int) -> bool:
    """Check the small integer placement space independently of the planner."""

    if not procs and not gpus:
        return any((node.mem or 0) >= mem for node in nodes)
    processor_options = product(*(range(node.procs + 1) for node in nodes))
    for processor_share in processor_options:
        if sum(processor_share) != procs:
            continue
        gpu_options = product(*(range(node.gpus + 1) for node in nodes))
        for gpu_share in gpu_options:
            if sum(gpu_share) != gpus or (
                procs and any(gpu and not proc for proc, gpu in zip(processor_share, gpu_share))
            ):
                continue
            weights = processor_share if procs else gpu_share
            if all(
                -(-mem * weight // (procs or gpus)) <= (node.mem or 0) for node, weight in zip(nodes, weights) if weight
            ):
                return True
    return False


def test_small_integer_placements_match_an_independent_feasibility_oracle() -> None:
    nodes = (
        Node("a", 2, 2, 1),
        Node("b", 2, 2, 1),
        Node("c", 1, 3, 0),
        Node("gpu-only", 0, 4, 2),
        Node("cpu-only", 3, 0, 0),
    )
    for procs in range(5):
        for gpus in range(3):
            for mem in range(7):
                inventory = _inventory(*nodes)
                requirement = {"procs": procs, "gpus": gpus, "mem": mem}
                expected = _oracle_fits(nodes, procs, gpus, mem)
                assert fits(inventory, requirement) == expected
                empty = copy.deepcopy(inventory)
                placement = assign(inventory, requirement)
                assert (placement is not None) == expected
                if placement is not None:
                    release(inventory, placement)
                assert inventory == empty


def _brute_spill(frees: list[_Free], procs: int, gpus: int, mem: int | None) -> bool:
    """Try every per-node procs/gpus vector against the rounded-up memory share rule."""

    need = procs or gpus
    if not need:
        return bool(frees) and (not mem or any((free.mem or 0) >= mem for free in frees))
    for taken_procs in product(*(range(free.procs + 1) for free in frees)):
        if sum(taken_procs) != procs:
            continue
        for taken_gpus in product(*(range(free.gpus + 1) for free in frees)):
            if sum(taken_gpus) != gpus or (procs and any(g and not p for p, g in zip(taken_procs, taken_gpus))):
                continue
            weights = taken_procs if procs else taken_gpus
            if not mem or all(-(-mem * w // need) <= (free.mem or 0) for free, w in zip(frees, weights) if w):
                return True
    return False


def test_spill_feasibility_matches_brute_force() -> None:
    rng = random.Random(20261003)
    for _ in range(2000):
        nodes = tuple(
            Node(f"n{index}", rng.randint(0, 4), rng.randint(0, 40), rng.randint(0, 2))
            for index in range(rng.randint(1, 5))
        )
        inventory = _inventory(*nodes)
        if rng.random() < 0.2:
            inventory.labels -= {"mem"}
        for _ in range(rng.randint(0, 2)):
            assign(inventory, {"procs": rng.randint(0, 2), "gpus": rng.randint(0, 1), "mem": rng.randint(0, 15)})
        procs, gpus, mem = rng.randint(0, 8), rng.randint(0, 4), rng.randint(0, 120)
        requirement = {"procs": procs, "gpus": gpus, "mem": mem}
        placed = mem if "mem" in inventory.labels else None
        frees = [free for free in inventory.nodes if not free.whole]
        expected = _brute_spill(frees, procs, gpus, placed)
        assert can_assign(inventory, requirement) == expected, (nodes, inventory, requirement)
        before = {free.node.host: (free.procs, free.gpus, free.mem) for free in inventory.nodes}
        placement = assign(inventory, requirement)
        if placement is None:
            continue
        shares = placement.nodes
        assert sum(share.procs for share in shares) == procs and sum(share.gpus for share in shares) == gpus
        for share in shares:
            free_procs, free_gpus, free_mem = before[share.host]
            assert share.procs <= free_procs and share.gpus <= free_gpus
            assert not (procs and share.gpus and not share.procs)
            if placed is not None:
                assert share.mem is not None and share.mem <= (free_mem or 0)
        if placed is not None:
            assert sum(share.mem or 0 for share in shares) == placed


def test_large_homogeneous_whole_node_request_uses_the_fast_path() -> None:
    nodes = tuple(Node(f"n{index}", 8, 100, 1) for index in range(1000))
    inventory = _inventory(*nodes)
    placement = assign(inventory, {"nodes": 1, "procs": 8, "gpus": 1})
    assert placement is not None and len(placement.nodes) == 1


def test_large_homogeneous_impossible_whole_request_is_rejected() -> None:
    nodes = tuple(Node(f"n{index}", 2, 100) for index in range(2000))
    inventory = _inventory(*nodes)
    assert assign(inventory, {"nodes": 1000, "procs": 2001}) is None


def test_large_homogeneous_memory_shortfall_is_rejected() -> None:
    nodes = tuple(Node(f"n{index}", 2, 1) for index in range(2000))
    inventory = _inventory(*nodes)
    assert assign(inventory, {"procs": 2001, "mem": 2001}) is None


def test_impossible_gpu_colocation_is_rejected() -> None:
    nodes = tuple(Node(f"n{index}", 1, 100, 1) for index in range(30))
    inventory = _inventory(*nodes)

    assert assign(inventory, {"procs": 10, "gpus": 11, "mem": 1}) is None


def test_memory_rounding_shortfall_with_heterogeneous_nodes_is_rejected() -> None:
    nodes = tuple(Node(f"n{index}", 2, 0) for index in range(100)) + (Node("rich", 2, 50),)
    inventory = _inventory(*nodes)

    assert assign(inventory, {"procs": 101, "mem": 50}) is None


def test_large_feasible_spill_is_found() -> None:
    # Filling the larger, memory-poor nodes first cannot hold the mem shares.
    nodes = tuple(Node(f"big{index}", 4, 1) for index in range(100))
    nodes += tuple(Node(f"n{index}", 2, 2) for index in range(200))
    inventory = _inventory(*nodes)
    placement = assign(inventory, {"procs": 201, "mem": 200})

    assert placement is not None
    assert sum(share.procs for share in placement.nodes) == 201
    assert sum(share.mem or 0 for share in placement.nodes) == 200
    assert all((share.mem or 0) <= (1 if share.host.startswith("big") else 2) for share in placement.nodes)


def test_whole_nodes_refuse_partially_used_nodes_and_release_restores_exactly() -> None:
    inventory = _inventory(*_TWO)
    empty = copy.deepcopy(inventory)
    small = assign(inventory, {"procs": 1})
    assert _hosts(small) == [("a", 1, 0, None)]
    whole = assign(inventory, {"nodes": 2})
    assert whole is None
    one = assign(inventory, {"nodes": 1})
    assert _hosts(one) == [("b", 8, 0, 8000)] and one is not None and one.nodes[0].whole
    # A whole node takes nothing else, even an empty requirement.
    assert _hosts(assign(copy.deepcopy(inventory), {})) == [("a", 0, 0, None)]
    assert not can_assign(inventory, {"procs": 4})
    assert inventory.free() == {"procs": 3, "gpus": 0, "nodes": 0, "mem": 4000}
    assert small is not None
    release(inventory, small)
    release(inventory, one)
    assert inventory == empty


def test_slots_and_gpu_ids_are_never_shared_and_gpus_co_locate() -> None:
    nodes = (
        Node("a", 2, None, 2, ("0", "1"), ("g0", "g1"), "CUDA_VISIBLE_DEVICES"),
        Node("b", 2, None, 2, ("0", "1"), ("g0", "g1"), "CUDA_VISIBLE_DEVICES"),
    )
    inventory = _inventory(*nodes)
    empty = copy.deepcopy(inventory)
    first = assign(inventory, {"procs": 1, "gpus": 1})
    second = assign(inventory, {"procs": 1, "gpus": 1})
    assert first is not None and second is not None
    assert (first.nodes[0].cpu_slots, first.nodes[0].gpu_ids) == (("0",), ("g0",))
    assert (second.nodes[0].host, second.nodes[0].cpu_slots, second.nodes[0].gpu_ids) == ("a", ("1",), ("g1",))
    assert first.gpu_variable == "CUDA_VISIBLE_DEVICES"
    # Two GPUs fit only on node b, together with the procs.
    third = assign(inventory, {"procs": 1, "gpus": 2})
    assert _hosts(third) == [("b", 1, 2, None)]
    assert third is not None
    assert describe(third) == [{"host": "b", "procs": 1, "gpus": 2, "gpu_ids": ["g0", "g1"], "cpus": ["0"]}]
    for placement in (second, first, third):
        assert placement is not None
        release(inventory, placement)
    assert inventory == empty


def test_capacity_overrides_truncate_from_the_end_or_disable_the_inventory() -> None:
    allocation = Allocation("test", None, (Node("a", 4, 400, cpu_slots=("0", "1", "2", "3")), Node("b", 4, 400)), {})
    capacity = allocation.capacity()
    inventory = Inventory.from_allocation(allocation, {**capacity, "procs": 5, "mem": 600})
    assert inventory is not None
    assert [(free.procs, free.mem) for free in inventory.nodes] == [(4, 400), (1, 200)]
    inventory = Inventory.from_allocation(allocation, {**capacity, "procs": 3})
    assert inventory is not None and inventory.nodes[0].cpu_slots == ["0", "1", "2"]
    inventory = Inventory.from_allocation(allocation, {**capacity, "nodes": 1})
    assert inventory is not None and [free.node.host for free in inventory.nodes] == ["a"]
    for override in ({"procs": 9}, {"nodes": 3}, {"gpus": 1}):
        assert Inventory.from_allocation(allocation, {**capacity, **override}) is None
    assert Inventory.from_allocation(None, capacity) is None
    assert Inventory.from_allocation(Allocation("slurm", None, (), {"procs": 4}), {"procs": 4}) is None


def test_census_reports_the_first_placed_label_that_never_fits() -> None:
    inventory = _inventory(*_TWO)
    manager = SimpleNamespace(resources={"procs": 12, "nodes": 2, "mem": 12000}, _inventory=inventory)
    assert unplaceable_resource(manager, {"nodes": 3}) == "nodes"
    # One node of four procs cannot hold nine, though the allocation can.
    assert unplaceable_resource(manager, {"nodes": 1, "procs": 9}) == "procs"
    assert unplaceable_resource(manager, {"procs": 9}) is None
    assert unplaceable_resource(manager, {"gpus": 1}) == "gpus"
    assert fits(inventory, {"nodes": 2}) and not fits(inventory, {"procs": 13})


def test_nodefile_and_launch_prefix() -> None:
    shares = (
        NodeShare("a", 2, 100, 0, None, ("0-1", "2-3"), False),
        NodeShare("b", 0, None, 1, ("7",), None, False),
    )
    placement = Placement(shares, None)
    assert nodefile_lines(placement) == ["a", "a"]
    with pytest.raises(ValueError, match="GPU share"):
        render_launch(placement, kind="slurm", template=None, nodefile="/n")
    placement = Placement((shares[0], replace(shares[1], procs=1)), None)
    assert nodefile_lines(placement) == ["a", "a", "b"]
    # The prefix scopes SLURM_HOSTFILE to its own srun.
    srun = ("env", "SLURM_HOSTFILE=/n", "srun")
    layout = ("--distribution=arbitrary", "--exact")
    # b's memory is unknown, so a multi-node step gets no memory option.
    assert render_launch(placement, kind="slurm", template=None, nodefile="/n") == [
        *srun,
        *("--ntasks=3", *layout, "--cpus-per-task=1", "--gpus=1"),
    ]
    single = Placement((NodeShare("a", 4, 900, 0, None, None, True),), None)
    assert render_launch(single, kind="slurm", template=None, nodefile="/n", gpus_present=True) == [
        *srun,
        *("--ntasks=4", *layout, "--cpus-per-task=1", "--mem=900M", "--gres=none"),
    ]
    # Several nodes: memory per CPU, rounded up, over every task's CPUs.
    spread = Placement(
        (NodeShare("a", 3, 700, 0, None, None, False), NodeShare("b", 1, 300, 0, None, None, False)), None
    )
    assert render_launch(spread, kind="slurm", template=None, nodefile="/n", cpus_per_proc=2) == [
        *srun,
        *("--ntasks=4", *layout),
        *("--cpus-per-task=2", "--mem-per-cpu=125M"),
    ]
    default = render_launch(spread, kind="slurm", template=None, nodefile="/n")
    assert default is not None and default[-1] == "--mem-per-cpu=250M"
    # Unplaced memory: the shares carry none, so the counted requirement is used.
    inventory = _inventory(Node("a", 4), Node("b", 4))
    one, two = assign(inventory, {"procs": 2, "mem": 600}), assign(inventory, {"procs": 6, "mem": 600})
    assert one is not None and two is not None and len(two.nodes) == 2
    launches = [
        render_launch(placement, kind="slurm", template=None, nodefile="/n", mem=600) for placement in (one, two)
    ]
    assert [launch and launch[-1] for launch in launches] == ["--mem=600M", "--mem-per-cpu=100M"]
    assert not any("mem" in item for item in render_launch(one, kind="slurm", template=None, nodefile="/n") or [])
    # srun reads --mem=0 as the whole node, so zero memory is no option.
    assert not any(
        "mem" in item for item in render_launch(one, kind="slurm", template=None, nodefile="/n", mem=0) or []
    )
    assert render_launch(single, kind="host", template=None, nodefile="/n") is None
    placement = Placement(shares, None)
    template = "mpirun -np {procs} --hostfile {nodefile} -x OMP={cpus_per_proc} --host '{hosts}' {mem}"
    assert render_launch(placement, kind="host", template=template, nodefile="/n") == [
        *("mpirun", "-np", "2", "--hostfile", "/n", "-x", "OMP=2", "--host", "a,b", "100"),
    ]
    with pytest.raises(ValueError, match=r"\{bogus\}"):
        render_launch(placement, kind="slurm", template="run {bogus}", nodefile="/n")
    # Only {identifier} is a placeholder; other braces stay as written.
    assert render_launch(placement, kind="host", template="xargs -I{} {} {procs}", nodefile="/n") == [
        *("xargs", "-I{}", "{}", "2"),
    ]


def test_attempt_context_binding_is_optional_and_validated(tmp_path: Path) -> None:
    base = {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        **{name: "x" for name in ("workspace_id", "job_id", "job_key", "placement", "step", "activation_id")},
        "attempt_id": "x",
        "payload": str(tmp_path),
    }
    assert AttemptContext.from_mapping(base).binding is None
    binding = {"nodes": [{"host": "a", "procs": 1, "gpus": 0}], "nodefile": "/n"}
    assert AttemptContext.from_mapping({**base, "binding": binding}).binding == binding
    invalids: list[object] = [{"nodefile": "/n"}, {"nodes": []}, [1]]
    for invalid in invalids:
        with pytest.raises(ValueError, match="binding"):
            AttemptContext.from_mapping({**base, "binding": invalid})


def test_one_process_manager_takes_its_capacity_from_the_host_probe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[list[str]] = []

    class Child:
        pid = 1

    def fake_popen(argv: list[str], **_kwargs: object) -> Child:
        calls.append(argv)
        return Child()

    monkeypatch.setattr(launchers.subprocess, "Popen", fake_popen)
    argv = [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"]
    launchers.launch_processes(workspace_root=tmp_path, argv=argv, count=1, settings={}, capacity={"procs": 4})
    assert "--worker-resource" not in calls[0] and calls[0][-2:] == ["--allocation", "host"]
    calls.clear()
    launchers.launch_processes(workspace_root=tmp_path, argv=argv, count=2, settings={}, capacity={"procs": 4})
    assert all("--worker-resource" in launched for launched in calls)


# The runner records what it was given, then waits for its go file.
_RECORD = """
seen = {
    "binding": context.get("binding"),
    "env": {name: os.environ.get(name) for name in (
        "HTTK_WORKFLOW_NODELIST", "HTTK_WORKFLOW_NODEFILE", "HTTK_WORKFLOW_LAUNCH")},
}
seen["env"]["SLURM_HOSTFILE"] = os.environ.get("SLURM_HOSTFILE")
if seen["binding"] is not None:
    seen["nodefile"] = Path(seen["binding"]["nodefile"]).read_text()
    seen["file"] = json.loads(Path(seen["binding"]["file"]).read_text())
Path(os.environ["HTTK_WORKFLOW_WORKDIR"], "seen.json").write_text(json.dumps(seen))
go = Path(os.environ["HTTK_WORKFLOW_WORKDIR"], "go")
while not go.exists():
    time.sleep(0.02)
publish("succeed")
"""


class _Campaign:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.workspace = Workspace.initialize(tmp_path / "workspace")
        self.jobs: dict[str, str] = {}

    def submit(self, tag: str, **resources: int) -> None:
        payload, job_id = _payload(self.tmp_path / "source", _HEADER + _RECORD, tag=tag, resources=resources)
        self.workspace.submit(payload, f"project/{tag}")
        self.jobs[tag] = job_id

    def workdir(self, tag: str) -> Path:
        marker = self.workspace.find_marker_by_id(self.jobs[tag])
        assert marker is not None
        return self.workspace.payload_path(marker.placement, marker.job_key) / "run"

    def kind(self, tag: str) -> str:
        marker = self.workspace.find_marker_by_id(self.jobs[tag])
        assert marker is not None
        return marker.kind

    def seen(self, manager: TaskManager, tag: str) -> dict[str, Any]:
        path = self.workdir(tag) / "seen.json"
        deadline = time.monotonic() + 20.0
        while not path.exists():
            assert time.monotonic() < deadline, f"{tag} never ran"
            manager.tick()
            time.sleep(0.02)
        return json.loads(path.read_text())

    def finish(self, manager: TaskManager, *tags: str) -> None:
        for tag in tags:
            (self.workdir(tag) / "go").touch()
        deadline = time.monotonic() + 20.0
        # Finished means reaped too, so their placements are back in the inventory.
        ids = {self.jobs[tag] for tag in tags}
        while any(self.kind(tag) != "succeeded" for tag in tags) or any(
            local.marker.job_id in ids for local in manager._running.values()
        ):
            assert time.monotonic() < deadline, "jobs never finished"
            manager.tick()
            time.sleep(0.02)


def _manager(
    campaign: _Campaign, kind: str = "host", nodes: tuple[Node, ...] | None = None, **resources: int
) -> TaskManager:
    allocation = Allocation(kind, None, nodes or (Node(HOST, 4), Node("other-node", 4)), {})
    return TaskManager(
        campaign.workspace,
        resources={**allocation.capacity(), **resources},
        maximum_workers=4,
        allocation=allocation,
        heartbeat_interval=0.01,
    )


@pytest.mark.timing
def test_attempts_are_placed_and_told_their_binding(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("small", procs=2)
    with _manager(campaign) as manager:
        seen = campaign.seen(manager, "small")
        nodefile = seen["binding"]["nodefile"]
        assert seen["binding"] == {
            "nodes": [{"host": HOST, "procs": 2, "gpus": 0}],
            "nodefile": nodefile,
            "file": str(Path(nodefile).with_name("binding.json")),
        }
        assert seen["env"] == {
            "HTTK_WORKFLOW_NODELIST": HOST,
            "HTTK_WORKFLOW_NODEFILE": nodefile,
            "HTTK_WORKFLOW_LAUNCH": None,
            "SLURM_HOSTFILE": None,
        }
        assert seen["nodefile"] == f"{HOST}\n{HOST}\n"
        campaign.finish(manager, "small")
        assert manager._inventory is not None and manager._inventory.free()["nodes"] == 2

        campaign.submit("left", procs=4)
        campaign.submit("right", procs=4)
        hosts = {campaign.seen(manager, tag)["env"]["HTTK_WORKFLOW_NODELIST"] for tag in ("left", "right")}
        assert hosts == {HOST, "other-node"}
        campaign.finish(manager, "left", "right")

        campaign.submit("wide", procs=8)
        assert campaign.seen(manager, "wide")["env"]["HTTK_WORKFLOW_NODELIST"] in (
            f"{HOST},other-node",
            f"other-node,{HOST}",
        )
        campaign.finish(manager, "wide")


@pytest.mark.timing
def test_whole_node_job_waits_for_an_idle_node(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("first", procs=3)
    campaign.submit("second", procs=3)
    with _manager(campaign) as manager:
        hosts = {campaign.seen(manager, tag)["env"]["HTTK_WORKFLOW_NODELIST"] for tag in ("first", "second")}
        assert hosts == {HOST, "other-node"}
        campaign.submit("whole", nodes=1)
        for _ in range(10):
            manager.tick()
        assert campaign.kind("whole") == "ready"
        campaign.finish(manager, "first")
        seen = campaign.seen(manager, "whole")
        assert seen["binding"]["nodes"][0]["procs"] == 4
        campaign.finish(manager, "second", "whole")


@pytest.mark.timing
def test_slurm_allocation_gets_an_srun_prefix(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("mpi", procs=8)
    with _manager(campaign, kind="slurm") as manager:
        seen = campaign.seen(manager, "mpi")
        nodefile = seen["binding"]["nodefile"]
        assert str(seen["env"]["HTTK_WORKFLOW_LAUNCH"]).startswith(
            f"env SLURM_HOSTFILE={nodefile} srun --ntasks=8 --distribution=arbitrary "
        )
        # Only the prefix's own srun reads the hostfile; the runner's environment does not carry it.
        assert seen["env"]["SLURM_HOSTFILE"] is None
        assert seen["binding"]["launch"][:3] == ["env", f"SLURM_HOSTFILE={nodefile}", "srun"]
        campaign.finish(manager, "mpi")


@pytest.mark.timing
def test_without_an_allocation_nothing_is_bound(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTK_WORKFLOW_NODELIST", "inherited")
    campaign = _Campaign(tmp_path)
    campaign.submit("plain", procs=2)
    with TaskManager(campaign.workspace, resources={"procs": 8}, heartbeat_interval=0.01) as manager:
        assert manager._inventory is None
        seen = campaign.seen(manager, "plain")
        assert seen["binding"] is None and set(seen["env"].values()) == {None}
        campaign.finish(manager, "plain")


def test_census_reports_whole_node_jobs_beyond_the_inventory(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("huge", nodes=3)
    with _manager(campaign) as manager:
        manager._register_submissions()
        assert manager._work_census().ready_blocked["resources"] == {"nodes": 1}


@pytest.mark.timing
def test_cancelled_and_unlaunched_attempts_return_their_placement(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("cancelled", procs=4)
    with _manager(campaign) as manager:
        assert manager._inventory is not None
        campaign.seen(manager, "cancelled")
        marker = campaign.workspace.find_marker_by_id(campaign.jobs["cancelled"])
        assert marker is not None and manager._inventory.free()["nodes"] == 1
        _publish_cancel(campaign.workspace, marker)
        deadline = time.monotonic() + 20.0
        while campaign.kind("cancelled") != "cancelled" or manager._running:
            assert time.monotonic() < deadline, "the attempt was never cancelled"
            manager.tick()
            time.sleep(0.02)
        assert manager._inventory.free()["nodes"] == 2

        campaign.workspace.set_setting("manager.launch_template", "run {bogus}")
        campaign.submit("unlaunched", procs=4)
        deadline = time.monotonic() + 20.0
        while campaign.kind("unlaunched") != "failed":
            assert time.monotonic() < deadline, "the launch never failed"
            manager.tick()
        assert manager._inventory.free()["nodes"] == 2 and not manager._unlaunched


def test_launch_releases_the_claim_when_the_inventory_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("raced", procs=4)
    with _manager(campaign) as manager:
        manager._register_submissions()
        marker = campaign.workspace.find_marker_by_id(campaign.jobs["raced"])
        assert marker is not None
        monkeypatch.setattr("httk.workflow.manager.assign", lambda *_args: None)
        assert manager._claim_and_launch(marker)
        raced = campaign.workspace.find_marker_by_id(campaign.jobs["raced"])
        assert raced is not None
        state = campaign.workspace.read_state(raced)
        assert campaign.kind("raced") == "ready" and state["reason"] == "resources_changed"
        assert state["attempt_ordinal"] == 0


def test_an_empty_requirement_holds_its_node_against_whole_node_requests() -> None:
    inventory = _inventory(Node("a", 0), Node("b", 0))
    empty = copy.deepcopy(inventory)
    nothing = assign(inventory, {})
    assert _hosts(nothing) == [("a", 0, 0, None)]
    first = assign(inventory, {"nodes": 1})
    assert _hosts(first) == [("b", 0, 0, None)]
    assert assign(inventory, {"nodes": 1}) is None
    assert nothing is not None and first is not None
    release(inventory, nothing)
    second = assign(inventory, {"nodes": 1})
    assert _hosts(second) == [("a", 0, 0, None)]
    # Both nodes are now whole: nothing else, and no other whole request, fits.
    assert assign(inventory, {"nodes": 1}) is None and assign(inventory, {}) is None
    assert second is not None
    release(inventory, first)
    release(inventory, second)
    assert inventory == empty


def test_can_assign_is_a_dry_run() -> None:
    inventory = _inventory(*_TWO)
    assign(inventory, {"procs": 3, "mem": 500})
    before = copy.deepcopy(inventory)
    for requirement in ({"procs": 1}, {"nodes": 1}, {"procs": 9, "mem": 900}, {"procs": 13}):
        expected = assign(copy.deepcopy(inventory), requirement) is not None
        assert can_assign(inventory, requirement) is expected
        assert inventory == before


def test_unknown_node_memory_is_left_to_the_counters() -> None:
    allocation = Allocation("test", None, (Node("a", 4, 1000), Node("b", 4)), {})
    inventory = Inventory.from_allocation(allocation, {**allocation.capacity(), "mem": 1000})
    assert inventory is not None and inventory.labels == {"nodes", "procs", "gpus"}
    assert all(free.mem is None for free in inventory.nodes) and "mem" not in inventory.free()
    placement = assign(inventory, {"procs": 1, "mem": 5000})
    assert placement is not None and placement.nodes[0].mem is None


def test_manager_capacity_follows_the_allocation_and_its_inventory(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    allocation = Allocation("test", None, (Node("a", 8), Node("b", 8)), {"license": 2})
    with TaskManager(workspace, allocation=allocation) as manager:
        assert manager.resources == {"procs": 16, "nodes": 2, "license": 2}
    with TaskManager(workspace, resources={**allocation.capacity(), "nodes": 1}, allocation=allocation) as manager:
        assert manager.resources == {"procs": 8, "nodes": 1, "license": 2}


@pytest.mark.timing
def test_binding_json_holds_slots_and_ids_the_context_leaves_out(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("gpu", procs=1, gpus=1)
    nodes = (Node(HOST, 2, 2000, 1, ("0-1", "2-3"), ("g7",), "CUDA_VISIBLE_DEVICES"),)
    with _manager(campaign, nodes=nodes) as manager:
        seen = campaign.seen(manager, "gpu")
        # The fair share of 2000 MB over four workers.
        assert seen["binding"]["nodes"] == [{"host": HOST, "procs": 1, "gpus": 1, "mem": 500}]
        assert seen["file"] == {
            "nodes": [{"host": HOST, "procs": 1, "gpus": 1, "mem": 500, "gpu_ids": ["g7"], "cpus": ["0-1"]}],
            "nodefile": seen["binding"]["nodefile"],
        }
        campaign.finish(manager, "gpu")


def test_a_binding_too_large_for_the_context_fails_the_attempt_by_name(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("huge", nodes=3000)
    nodes = tuple(Node(f"node{index:05d}", 1) for index in range(3000))
    with _manager(campaign, nodes=nodes) as manager:
        deadline = time.monotonic() + 60.0
        while campaign.kind("huge") != "failed":
            assert time.monotonic() < deadline, "the launch never failed"
            manager.tick()
        marker = campaign.workspace.find_marker_by_id(campaign.jobs["huge"])
        assert marker is not None
        failure = campaign.workspace.read_state(marker)["failure"]
        assert isinstance(failure, dict) and "binding of 3000 nodes is too large" in failure["message"]
        assert manager._inventory is not None and manager._inventory.free()["nodes"] == 3000


@pytest.mark.timing
def test_counted_memory_on_unknown_memory_nodes(tmp_path: Path) -> None:
    campaign = _Campaign(tmp_path)
    campaign.submit("first", procs=1, mem=600)
    campaign.submit("second", procs=1, mem=600)
    campaign.submit("too-big", procs=1, mem=1200)
    with _manager(campaign, mem=1000) as manager:
        assert manager._inventory is not None and "mem" not in manager._inventory.labels
        seen = campaign.seen(manager, "first")
        assert "mem" not in seen["binding"]["nodes"][0]
        for _ in range(10):
            manager.tick()
        assert campaign.kind("second") == "ready"
        assert manager._work_census().ready_blocked["resources"] == {"mem": 1}
        campaign.finish(manager, "first")
        campaign.seen(manager, "second")
        campaign.finish(manager, "second")
