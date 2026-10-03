"""Place attempts on the nodes of a manager's allocation.

An :class:`Inventory` tracks what is free on every node of the allocation; it is
in-memory only, so a replacement manager re-derives it from its own probe.
:func:`assign` reserves one attempt's ``procs``, ``gpus``, ``mem`` and ``nodes``
and returns the :class:`Placement` the attempt is told about; :func:`release`
returns it.
"""

import re
import shlex
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Self

from ._allocation import Allocation, Node

#: The requirement labels an inventory places, checked in this order when
#: reporting which one can never fit.
INVENTORY_LABELS = ("nodes", "procs", "gpus", "mem")
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass(frozen=True)
class NodeShare:
    """What one attempt was given on one node.

    :param host: The node's host name.
    :param procs: The processor slots given.
    :param mem: The memory given in MB, or ``None`` when the attempt has no ``mem``.
    :param gpus: The GPUs given.
    :param gpu_ids: The device ids of those GPUs, when the node knows them.
    :param cpu_slots: The cpulists of those processor slots, when the node knows them.
    :param whole: Whether the attempt has the whole node to itself.
    """

    host: str
    procs: int
    mem: int | None
    gpus: int
    gpu_ids: tuple[str, ...] | None
    cpu_slots: tuple[str, ...] | None
    whole: bool


@dataclass(frozen=True)
class Placement:
    """The node shares of one attempt.

    :param nodes: One share per node the attempt was placed on.
    :param gpu_variable: The environment variable the GPU ids belong in, when known.
    """

    nodes: tuple[NodeShare, ...]
    gpu_variable: str | None


@dataclass
class _Free:
    """The unreserved part of one node and how many placements hold part of it."""

    node: Node
    procs: int
    mem: int | None
    gpus: int
    gpu_ids: list[str] | None
    cpu_slots: list[str] | None
    holders: int = 0
    whole: bool = False

    @classmethod
    def of(cls, node: Node) -> Self:
        return cls(
            node,
            node.procs,
            node.mem,
            node.gpus,
            None if node.gpu_ids is None else list(node.gpu_ids),
            None if node.cpu_slots is None else list(node.cpu_slots),
        )

    @property
    def idle(self) -> bool:
        return self.holders == 0 and not self.whole

    def share(self, procs: int, gpus: int, mem: int | None, *, whole: bool = False) -> NodeShare:
        """Return the share taking the first free slots and ids, without reserving it."""

        slots = None if self.cpu_slots is None else tuple(self.cpu_slots[:procs])
        ids = None if self.gpu_ids is None else tuple(self.gpu_ids[:gpus])
        return NodeShare(self.node.host, procs, mem, gpus, ids, slots, whole)

    def reserve(self, share: NodeShare) -> None:
        if self.cpu_slots is not None:
            del self.cpu_slots[: share.procs]
        if self.gpu_ids is not None:
            del self.gpu_ids[: share.gpus]
        self.procs -= share.procs
        self.gpus -= share.gpus
        if self.mem is not None and share.mem is not None:
            self.mem -= share.mem
        self.holders += 1
        self.whole = self.whole or share.whole


@dataclass
class Inventory:
    """The free part of every node of one manager's allocation.

    :param nodes: The free part of each node, in allocation order.
    :param labels: The requirement labels this inventory places: ``nodes``,
        ``procs``, ``gpus`` and, only when every node knows its memory, ``mem``.
    """

    nodes: list[_Free]
    labels: frozenset[str]

    @classmethod
    def from_allocation(cls, allocation: Allocation | None, capacity: Mapping[str, int]) -> Self | None:
        """Build the empty inventory of an allocation, cut down to the manager's capacity.

        A capacity of a placed label that differs from the allocation's own sum
        overrides it: a smaller one drops the trailing nodes (``nodes``) or takes
        the excess from the last nodes backwards; a larger one cannot be placed,
        so there is no inventory. ``mem`` is placed only when every node knows
        it; otherwise it is left to the manager's counters, override or not.

        :param allocation: The manager's allocation.
        :param capacity: The manager's validated capacity.
        :return: The inventory, or ``None`` without an allocation, without nodes,
            or when the capacity exceeds the nodes.
        """

        if allocation is None or not allocation.nodes:
            return None
        nodes = list(allocation.nodes)
        labels = frozenset(INVENTORY_LABELS)
        if any(node.mem is None for node in nodes):
            labels -= {"mem"}
            nodes = [replace(node, mem=None) for node in nodes]
        overrides = {
            label: capacity[label]
            for label in INVENTORY_LABELS
            if label in labels and label in capacity and capacity[label] != _total(nodes, label)
        }
        if "nodes" in overrides:
            if overrides["nodes"] > len(nodes):
                return None
            nodes = nodes[: overrides["nodes"]]
        for label in ("procs", "gpus", "mem"):
            if label not in overrides:
                continue
            excess = _total(nodes, label) - overrides[label]
            if excess < 0:
                return None
            for index in reversed(range(len(nodes))):
                node = nodes[index]
                value = getattr(node, label) or 0
                keep = value - min(value, excess)
                excess -= value - keep
                if label == "procs":
                    slots = None if node.cpu_slots is None else node.cpu_slots[:keep]
                    nodes[index] = replace(node, procs=keep, cpu_slots=slots)
                elif label == "gpus":
                    ids = None if node.gpu_ids is None else node.gpu_ids[:keep]
                    nodes[index] = replace(node, gpus=keep, gpu_ids=ids)
                else:
                    nodes[index] = replace(node, mem=keep)
        return cls([_Free.of(node) for node in nodes], labels)

    def empty(self) -> Self:
        """Return a copy of this inventory with nothing reserved."""

        return type(self)([_Free.of(free.node) for free in self.nodes], self.labels)

    def free(self) -> dict[str, int]:
        """Return the free amount of every placed label; ``nodes`` counts idle nodes."""

        totals = {
            "procs": sum(free.procs for free in self.nodes),
            "gpus": sum(free.gpus for free in self.nodes),
            "nodes": sum(free.idle for free in self.nodes),
            "mem": sum(free.mem or 0 for free in self.nodes),
        }
        return {label: value for label, value in totals.items() if label in self.labels}


def _total(nodes: Sequence[Node], label: str) -> int:
    return len(nodes) if label == "nodes" else sum(getattr(node, label) or 0 for node in nodes)


def _plan(inventory: Inventory, requirement: Mapping[str, int]) -> list[tuple[_Free, NodeShare]] | None:
    """Choose one attempt's node shares without reserving them (see :func:`assign`)."""

    procs, gpus, count = (requirement.get(name, 0) for name in ("procs", "gpus", "nodes"))
    mem = requirement.get("mem") if "mem" in inventory.labels else None
    candidates = [free for free in inventory.nodes if not free.whole]
    if count >= 1:
        idle = sorted(
            (free for free in candidates if free.idle),
            key=lambda free: (free.node.procs, free.node.gpus, free.node.mem or 0),
        )
        for start in range(len(idle) - count + 1):
            chosen = idle[start : start + count]
            if (
                sum(free.procs for free in chosen) >= procs
                and sum(free.gpus for free in chosen) >= gpus
                and (mem is None or sum(free.mem or 0 for free in chosen) >= mem)
            ):
                kept = {id(free) for free in chosen}
                chosen = [free for free in inventory.nodes if id(free) in kept]
                return [(free, free.share(free.procs, free.gpus, free.mem, whole=True)) for free in chosen]
        return None
    if procs == gpus == 0 and not mem:
        if not candidates:
            return None
        best = max(candidates, key=lambda free: free.procs)
        return [(best, best.share(0, 0, mem))]
    fitting = [
        free
        for free in candidates
        if free.procs >= procs and free.gpus >= gpus and (mem is None or (free.mem or 0) >= mem)
    ]
    if fitting:
        best = min(fitting, key=lambda free: free.procs)
        return [(best, best.share(procs, gpus, mem))]
    if procs == gpus == 0:
        # ponytail: mem alone never spills over nodes; an attempt that needs it
        # should declare the procs that come with the memory.
        return None
    weight = "procs" if procs else "gpus"
    plan: list[tuple[_Free, int, int]] = []
    left_procs, left_gpus = procs, gpus
    for free in sorted(candidates, key=lambda free: -getattr(free, weight)):
        if left_procs <= 0 and left_gpus <= 0:
            break
        taken_procs, taken_gpus = min(free.procs, left_procs), min(free.gpus, left_gpus)
        if taken_procs or taken_gpus:
            plan.append((free, taken_procs, taken_gpus))
            left_procs -= taken_procs
            left_gpus -= taken_gpus
    if left_procs > 0 or left_gpus > 0:
        return None
    mems: list[int | None] = [None] * len(plan)
    if mem is not None:
        total = procs or gpus
        split = [mem * (taken_procs if procs else taken_gpus) // total for _, taken_procs, taken_gpus in plan]
        split[0] += mem - sum(split)
        if any(share > (free.mem or 0) for (free, _, _), share in zip(plan, split, strict=True)):
            return None
        mems = list(split)
    return [
        (free, free.share(taken_procs, taken_gpus, share))
        for (free, taken_procs, taken_gpus), share in zip(plan, mems, strict=True)
    ]


def assign(inventory: Inventory, requirement: Mapping[str, int]) -> Placement | None:
    """Reserve one attempt's place in the inventory.

    ``nodes=N`` takes N idle nodes whole, the smallest that together hold the
    ``procs``, ``gpus`` and ``mem``. Otherwise the attempt goes on the one node
    with the fewest free ``procs`` that holds it all, else on the fewest nodes,
    filled from the node with the most free ``procs`` (``gpus`` when it needs no
    ``procs``), with ``mem`` split in proportion. An attempt needing none of
    them gets no ``procs`` on the node with the most free ones. ``mem`` counts
    only when the inventory places it.

    :param inventory: The inventory, changed only when the attempt is placed.
    :param requirement: The attempt's effective requirement; other labels are ignored.
    :return: The placement, or ``None`` when the attempt does not fit now.
    """

    shares = _plan(inventory, requirement)
    if shares is None:
        return None
    for free, share in shares:
        free.reserve(share)
    variable = next((free.node.gpu_variable for free, share in shares if share.gpus and free.node.gpu_variable), None)
    return Placement(tuple(share for _, share in shares), variable)


def _restore(
    base: tuple[str, ...] | None, free: list[str] | None, returned: tuple[str, ...] | None
) -> list[str] | None:
    """Merge returned slots or ids back into the free ones, in the node's own order."""

    if base is None:
        return None
    pool = Counter(free or ()) + Counter(returned or ())
    restored: list[str] = []
    for item in base:
        if pool[item] > 0:
            restored.append(item)
            pool[item] -= 1
    return restored


def release(inventory: Inventory, placement: Placement) -> None:
    """Return everything :func:`assign` reserved for one placement.

    :param inventory: The inventory the placement was assigned from.
    :param placement: The placement to return.
    """

    by_host = {free.node.host: free for free in inventory.nodes}
    for share in placement.nodes:
        free = by_host[share.host]
        free.holders -= 1
        if share.whole:
            free.whole = False
        free.procs += share.procs
        free.gpus += share.gpus
        if free.mem is not None and share.mem is not None:
            free.mem += share.mem
        free.cpu_slots = _restore(free.node.cpu_slots, free.cpu_slots, share.cpu_slots)
        free.gpu_ids = _restore(free.node.gpu_ids, free.gpu_ids, share.gpu_ids)


def can_assign(inventory: Inventory, requirement: Mapping[str, int]) -> bool:
    """Return whether :func:`assign` would place *requirement* now, without reserving it."""

    return _plan(inventory, requirement) is not None


def fits(inventory: Inventory, requirement: Mapping[str, int]) -> bool:
    """Return whether *requirement* fits the inventory once nothing else is running."""

    return _plan(inventory.empty(), requirement) is not None


def nodefile_lines(placement: Placement) -> list[str]:
    """Return the nodefile of a placement: one host line per processor slot, one for a share without any.

    :param placement: The placement.
    :return: The host lines, without line ends.
    """

    return [share.host for share in placement.nodes for _ in range(max(share.procs, 1))]


def describe(placement: Placement) -> list[dict[str, Any]]:
    """Return the context ``binding.nodes`` of a placement, omitting unknown members."""

    nodes: list[dict[str, Any]] = []
    for share in placement.nodes:
        item: dict[str, Any] = {"host": share.host, "procs": share.procs, "gpus": share.gpus}
        if share.mem is not None:
            item["mem"] = share.mem
        if share.gpu_ids is not None:
            item["gpu_ids"] = list(share.gpu_ids)
        if share.cpu_slots is not None:
            item["cpus"] = list(share.cpu_slots)
        nodes.append(item)
    return nodes


def _cpulist_size(cpulist: str) -> int:
    size = 0
    for item in cpulist.split(","):
        first, _, last = item.partition("-")
        size += int(last or first) - int(first) + 1
    return size


def render_launch(
    placement: Placement,
    *,
    kind: str,
    template: str | None,
    nodefile: str,
    gpus_present: bool = False,
    cpus_per_task: int | None = None,
    mem: int | None = None,
) -> list[str] | None:
    """Return the launch prefix a runner puts before its parallel command.

    A *template* (the ``manager.launch_template`` setting) is split like a shell
    word list and ``{procs}``, ``{nodes}``, ``{hosts}``, ``{nodefile}``,
    ``{gpus}``, ``{mem}`` (MB on the first node, or empty) and
    ``{cpus_per_proc}`` are substituted in each word; other braces stay as
    written. Without one, a Slurm allocation gets an ``srun`` step that lays its
    tasks out as the nodefile does (``--distribution=arbitrary``, with
    ``SLURM_HOSTFILE`` set to the nodefile for that ``srun`` only).

    :param placement: The attempt's placement.
    :param kind: The allocation kind.
    :param template: The launch template, or ``None``.
    :param nodefile: The path of the attempt's nodefile.
    :param gpus_present: Whether the allocation has GPUs, so a Slurm step
        without any must say so.
    :param cpus_per_task: The manager's Slurm ``--cpus-per-task``, which a step
        under ``--exact`` does not inherit, or ``None``.
    :param mem: The attempt's ``mem`` requirement, the step memory when the
        shares carry none because the inventory does not place memory.
    :return: The prefix, or ``None`` without a template outside Slurm.
    :raises ValueError: If the template names an unknown placeholder or cannot be split.
    """

    shares = placement.nodes
    hosts = ",".join(share.host for share in shares)
    tasks = len(nodefile_lines(placement))
    gpus = sum(share.gpus for share in shares)
    if template is not None:
        slots = shares[0].cpu_slots
        values = {
            "procs": str(sum(share.procs for share in shares)),
            "nodes": str(len(shares)),
            "hosts": hosts,
            "nodefile": nodefile,
            "gpus": str(gpus),
            "mem": "" if shares[0].mem is None else str(shares[0].mem),
            "cpus_per_proc": str(_cpulist_size(slots[0])) if slots else "1",
        }

        def substitute(match: re.Match[str]) -> str:
            if match.group(1) not in values:
                raise ValueError(f"unknown launch template placeholder {match.group(0)}")
            return values[match.group(1)]

        return [_PLACEHOLDER.sub(substitute, token) for token in shlex.split(template)]
    if kind != "slurm":
        return None
    argv = [
        "env",
        f"SLURM_HOSTFILE={nodefile}",
        "srun",
        f"--nodes={len(shares)}",
        f"--ntasks={tasks}",
        f"--nodelist={hosts}",
        "--distribution=arbitrary",
        "--exact",
    ]
    if cpus_per_task is not None:
        argv.append(f"--cpus-per-task={cpus_per_task}")
    mems = [share.mem for share in shares]
    total = mem if None in mems else sum(share or 0 for share in mems)
    # Zero is no option: srun reads --mem=0 as all of the node's memory.
    if total and len(shares) == 1:
        argv.append(f"--mem={total}M")
    elif total:
        argv.append(f"--mem-per-cpu={-(-total // (tasks * (cpus_per_task or 1)))}M")
    if gpus:
        argv.append(f"--gpus={gpus}")
    elif gpus_present:
        argv.append("--gres=none")
    return argv
