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
from ._scheduler import scheduler_for

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


def _memory_shares(
    plan: Sequence[tuple[_Free, int, int]], mem: int | None, denominator: int, *, by_gpus: bool = False
) -> list[int | None] | None:
    """Return proportional memory shares that fit each selected node."""

    if mem is None:
        empty_shares: list[int | None] = [None] * len(plan)
        return empty_shares
    if denominator <= 0:
        return None
    shares = [mem * (taken_gpus if by_gpus else taken_procs) // denominator for _, taken_procs, taken_gpus in plan]
    if any(share > (free.mem or 0) for (free, _, _), share in zip(plan, shares, strict=True)):
        return None
    remainder = mem - sum(shares)
    for index, (free, _, _) in enumerate(plan):
        room = (free.mem or 0) - shares[index]
        if room and remainder:
            shares[index] += 1
            remainder -= 1
    result: list[int | None] = list(shares)
    return None if remainder else result


def _whole_fallback(idle: Sequence[_Free], count: int, procs: int, gpus: int, mem: int | None) -> list[_Free] | None:
    """Find an exact whole-node combination after the ordered-window fast path."""

    # ponytail: this exact fallback is exponential for adversarial node mixes;
    # the ordered-window fast path keeps ordinary homogeneous allocations linear.
    suffix: list[tuple[int, int, int]] = [(0, 0, 0)] * (len(idle) + 1)
    for index in reversed(range(len(idle))):
        node = idle[index]
        old = suffix[index + 1]
        suffix[index] = (
            old[0] + node.procs,
            old[1] + node.gpus,
            old[2] + (node.mem or 0),
        )
    memo: set[tuple[int, int, int, int, int]] = set()
    stack: list[tuple[int, int, int, int, int, tuple[int, ...]]] = [(0, count, procs, gpus, mem or 0, ())]
    selected: tuple[int, ...] | None = None
    while stack:
        index, left, need_procs, need_gpus, need_mem, chosen = stack.pop()
        key = (index, left, max(0, need_procs), max(0, need_gpus), max(0, need_mem))
        if key in memo:
            continue
        memo.add(key)
        if left == 0:
            if need_procs <= 0 and need_gpus <= 0 and need_mem <= 0:
                selected = chosen
                break
            continue
        if len(idle) - index < left:
            continue
        available = suffix[index]
        if available[0] < need_procs or available[1] < need_gpus or available[2] < need_mem:
            continue
        if any(
            sum(sorted((getattr(item.node, label) or 0 for item in idle[index:]), reverse=True)[:left]) < need
            for label, need in (("procs", need_procs), ("gpus", need_gpus), ("mem", need_mem))
            if need
        ):
            continue
        node = idle[index]
        # Push the skip branch first so the preferred include-first order is
        # retained when the stack pops the next state.
        stack.append((index + 1, left, need_procs, need_gpus, need_mem, chosen))
        stack.append(
            (
                index + 1,
                left - 1,
                need_procs - node.procs,
                need_gpus - node.gpus,
                need_mem - (node.mem or 0),
                (*chosen, index),
            )
        )
    return None if selected is None else [idle[index] for index in selected]


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
        fallback_chosen = _whole_fallback(idle, count, procs, gpus, mem)
        if fallback_chosen is None:
            return None
        return [(free, free.share(free.procs, free.gpus, free.mem, whole=True)) for free in fallback_chosen]
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
    need = procs or gpus
    limits = [free.procs if procs else free.gpus for free in candidates]
    caps = [
        limit if not mem else min(limit, (free.mem or 0) * need // mem)
        for free, limit in zip(candidates, limits, strict=True)
    ]
    eligible = [index for index, cap in enumerate(caps) if cap >= 1]
    taken_procs, taken_gpus = [0] * len(candidates), [0] * len(candidates)
    left = gpus if procs else 0
    if left:
        gpu_nodes = [index for index in eligible if candidates[index].gpus]
        for index in sorted(gpu_nodes, key=lambda index: (-candidates[index].gpus, -caps[index])):
            if not left:
                break
            taken_gpus[index] = min(candidates[index].gpus, left)
            taken_procs[index] = 1
            left -= taken_gpus[index]
    filled = taken_procs if procs else taken_gpus
    if left or sum(filled) > need:
        return None
    left = need - sum(filled)
    for index in sorted(eligible, key=lambda index: filled[index] - caps[index]):
        more = min(caps[index] - filled[index], left)
        filled[index] += more
        left -= more
    if left:
        return None
    plan = [
        (free, used_procs, used_gpus)
        for free, used_procs, used_gpus in zip(candidates, taken_procs, taken_gpus, strict=True)
        if used_procs or used_gpus
    ]
    shares = _memory_shares(plan, mem, need, by_gpus=not procs)
    if shares is None:  # cannot happen: every share fits rounded up
        return None
    return [
        (free, free.share(used_procs, used_gpus, share))
        for (free, used_procs, used_gpus), share in zip(plan, shares, strict=True)
    ]


def assign(inventory: Inventory, requirement: Mapping[str, int]) -> Placement | None:
    """Reserve one attempt's place in the inventory.

    ``nodes=N`` takes N idle nodes whole, the smallest that together hold the
    ``procs``, ``gpus`` and ``mem``. Otherwise the attempt goes on the one node
    with the fewest free ``procs`` that holds it all, else it spills: when it
    needs ``gpus`` (and ``procs``), the nodes with the most free ``gpus`` are
    taken first with one processor slot each, then the remaining ``procs`` are
    filled from the largest remaining per-node capacity, a node's capacity
    being its free ``procs`` limited so that its proportional ``mem`` share,
    rounded up, fits its free memory (``gpus`` stand in for ``procs`` when it
    needs none). ``mem`` is split in proportion to what each node takes. This
    prefers few nodes without guaranteeing the fewest, and finds a placement
    whenever one exists under that rule. An attempt needing none of them gets
    no ``procs`` on the node with the most free ones. ``mem`` counts only when
    the inventory places it.

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
    """Return one host line per processor slot in a placement.

    :param placement: The placement.
    :return: The host lines, without line ends.
    """

    return [share.host for share in placement.nodes for _ in range(share.procs)]


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
    cpus_per_proc: int = 1,
    mem: int | None = None,
    mpi: str | None = None,
) -> list[str] | None:
    """Return the launch prefix a runner puts before its parallel command.

    A *template* (the ``manager.launch_template`` setting) is split like a shell
    word list and ``{procs}``, ``{nodes}``, ``{hosts}``, ``{nodefile}``,
    ``{gpus}``, ``{mem}`` (MB on the first node, or empty) and
    ``{cpus_per_proc}`` are substituted in each word; other braces stay as
    written. Without one, the allocation scheduler supplies its default step
    arguments from the placement and normalized allocation metadata, with
    *mpi* as the step's MPI plugin; the template owns its argv, so *mpi* is
    ignored with one.

    :param placement: The attempt's placement.
    :param kind: The allocation kind.
    :param template: The launch template, or ``None``.
    :param nodefile: The path of the attempt's nodefile.
    :param gpus_present: Whether the allocation has GPUs.
    :param cpus_per_proc: The allocation's normalized CPUs per processor slot.
    :param mem: The attempt's ``mem`` requirement, the step memory when the
        shares carry none because the inventory does not place memory.
    :param mpi: The ``manager.launch_mpi`` plugin of the default step, or ``None``.
    :return: The prefix, or ``None`` when no default scheduler step applies.
    :raises ValueError: If the template is invalid or the scheduler cannot launch the placement.
    """

    shares = placement.nodes
    hosts = ",".join(share.host for share in shares)
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
            "cpus_per_proc": str(_cpulist_size(slots[0]) if slots else cpus_per_proc),
        }

        def substitute(match: re.Match[str]) -> str:
            if match.group(1) not in values:
                raise ValueError(f"unknown launch template placeholder {match.group(0)}")
            return values[match.group(1)]

        return [_PLACEHOLDER.sub(substitute, token) for token in shlex.split(template)]
    scheduler = scheduler_for(kind)
    if scheduler is None:
        return None
    return scheduler.step_argv(
        placement, nodefile=nodefile, gpus_present=gpus_present, cpus_per_proc=cpus_per_proc, mem=mem, mpi=mpi
    )
