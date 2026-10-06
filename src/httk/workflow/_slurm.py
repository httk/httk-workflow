"""The maintained Slurm realization of the private scheduler contract."""

import logging
import re
import socket
import subprocess
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING

from ._allocation import (
    Allocation,
    Node,
    Run,
    _gpu_ids,
    local_cpu_slots,
)

if TYPE_CHECKING:
    from ._manager_binding import Placement

_LOGGER = logging.getLogger(__name__)
_REPEATED = re.compile(r"([0-9]+)(?:\(x([0-9]+)\))?")


def _slurm_gpus(raw: str) -> int:
    """Parse Slurm typed GPU counts such as ``a100:2,v100:1``."""

    total = 0
    for item in raw.split(","):
        count = item.rsplit(":", 1)[-1].strip()
        if not count.isdigit():
            raise ValueError(raw)
        total += int(count)
    return total


def _integer(environ: Mapping[str, str], name: str, *, memory: bool = False) -> int | None:
    raw = environ.get(name)
    if raw is None:
        return None
    value = raw.strip()
    multiplier = 1
    divide_kibibytes = False
    if memory and value:
        suffix = value[-1].upper()
        if suffix in {"M", "G", "K"}:
            value = value[:-1]
            if suffix == "G":
                multiplier = 1024
            elif suffix == "K":
                divide_kibibytes = True
    try:
        parsed = _slurm_gpus(value) if name == "SLURM_GPUS" else int(value)
    except ValueError:
        _LOGGER.warning("ignoring unparsable Slurm resource variable %s=%r", name, raw)
        return None
    if parsed < 0:
        _LOGGER.warning("ignoring negative Slurm resource variable %s=%r", name, raw)
        return None
    return parsed // 1024 if divide_kibibytes else parsed * multiplier


def slurm_counts(environ: Mapping[str, str]) -> dict[str, int]:
    """Read aggregate capacities from an active Slurm allocation.

    :param environ: Environment mapping to inspect.
    :return: Aggregate capacities, or an empty mapping outside a job.
    """

    if "SLURM_JOB_ID" not in environ:
        return {}
    ntasks = _integer(environ, "SLURM_NTASKS")
    gpus = _integer(environ, "SLURM_GPUS")
    nodes = _integer(environ, "SLURM_JOB_NUM_NODES")
    resources: dict[str, int] = {}
    if ntasks is not None:
        resources["procs"] = ntasks
    if gpus is not None:
        resources["gpus"] = gpus
    if nodes is not None:
        resources["nodes"] = nodes
    if "SLURM_MEM_PER_CPU" in environ:
        mem_per_cpu = _integer(environ, "SLURM_MEM_PER_CPU", memory=True)
        cpus = _integer(environ, "SLURM_CPUS_PER_TASK") if "SLURM_CPUS_PER_TASK" in environ else 1
        if mem_per_cpu is not None and cpus is not None and ntasks is not None:
            resources["mem"] = mem_per_cpu * cpus * ntasks
    elif "SLURM_MEM_PER_NODE" in environ:
        mem_per_node = _integer(environ, "SLURM_MEM_PER_NODE", memory=True)
        if mem_per_node is not None and nodes is not None:
            resources["mem"] = mem_per_node * nodes
    return resources


def slurm_end_time(environ: Mapping[str, str]) -> float | None:
    """Return the enclosing Slurm job end time, when known.

    :param environ: Environment containing Slurm job metadata.
    :return: Positive epoch seconds, or ``None`` when unavailable.
    """

    raw = environ.get("SLURM_JOB_END_TIME")
    if "SLURM_JOB_ID" not in environ or raw is None:
        return None
    text = raw.strip()
    if not text.isdigit() or int(text) <= 0:
        _LOGGER.warning("ignoring SLURM_JOB_END_TIME=%r: not a positive epoch second", raw)
        return None
    return float(text)


def _expand_per_node(text: str) -> list[int] | None:
    counts: list[int] = []
    for item in text.split(","):
        match = _REPEATED.fullmatch(item.strip())
        if match is None:
            return None
        counts += [int(match.group(1))] * int(match.group(2) or 1)
    return counts


def _slurm_hosts(environ: Mapping[str, str], run: Run) -> list[str] | None:
    nodelist = environ.get("SLURM_JOB_NODELIST")
    if nodelist:
        try:
            completed = run(
                ["scontrol", "show", "hostnames", nodelist], capture_output=True, text=True, timeout=30, check=False
            )
        except (OSError, subprocess.SubprocessError) as exc:
            _LOGGER.debug("scontrol show hostnames failed: %s", exc)
        else:
            hosts = completed.stdout.split() if completed.returncode == 0 else []
            if hosts:
                return hosts
    if environ.get("SLURM_JOB_NUM_NODES", "").strip() == "1":
        return [environ.get("SLURMD_NODENAME") or socket.gethostname()]
    return None


def _gpu_counts(environ: Mapping[str, str], total: int, node_count: int) -> list[int] | None:
    if total == 0:
        return [0] * node_count
    if node_count == 1:
        return [total]
    raw = environ.get("SLURM_GPUS_PER_NODE", "")
    try:
        count = _slurm_gpus(raw)
    except ValueError:
        return None
    if count * node_count != total:
        return None
    return [count] * node_count


def slurm_cpus_per_proc(environ: Mapping[str, str]) -> int:
    """Return normalized Slurm CPUs per task metadata.

    :param environ: Environment containing ``SLURM_CPUS_PER_TASK``.
    :return: A positive process CPU count, defaulting to one.
    """

    value = _integer(environ, "SLURM_CPUS_PER_TASK")
    return value if value is not None and value > 0 else 1


def slurm_allocation(
    environ: Mapping[str, str], *, run: Run = subprocess.run, cpu_slots: bool = False
) -> Allocation | None:
    """Probe the enclosing Slurm allocation, preserving uncertain aggregates.

    :param environ: Environment containing Slurm allocation metadata.
    :param run: Command runner used to expand the Slurm node list.
    :param cpu_slots: Whether to include local CPU affinity slots.
    :return: A node allocation, aggregate allocation, or ``None`` outside Slurm.
    """

    if "SLURM_JOB_ID" not in environ:
        return None
    end_time = slurm_end_time(environ)
    counts = slurm_counts(environ)
    cpus_per_proc = slurm_cpus_per_proc(environ)
    aggregate = Allocation("slurm", end_time, (), counts, cpus_per_proc)
    hosts = _slurm_hosts(environ, run)
    if hosts is None:
        _LOGGER.warning("cannot list the Slurm job's nodes; using its aggregate counts only")
        return aggregate
    procs = [0] * len(hosts)
    if "procs" in counts:
        per_node = _expand_per_node(environ.get("SLURM_TASKS_PER_NODE", "")) if len(hosts) > 1 else [counts["procs"]]
        if per_node is None or len(per_node) != len(hosts) or sum(per_node) != counts["procs"]:
            _LOGGER.warning("Slurm task distribution does not match its aggregate counts; using aggregate counts only")
            return aggregate
        procs = per_node
    gpus = counts.get("gpus", 0)
    per_node_gpus = _gpu_counts(environ, gpus, len(hosts))
    if per_node_gpus is None:
        _LOGGER.warning("Slurm GPU distribution is unknown or nonuniform; using aggregate counts only")
        return aggregate
    mem: list[int | None] = [None] * len(hosts)
    if "mem" in counts:
        if "SLURM_MEM_PER_CPU" in environ:
            mem = [counts["mem"] * count // counts["procs"] if counts.get("procs") else 0 for count in procs]
        else:
            mem = [counts["mem"] // len(hosts)] * len(hosts)
    batch = environ.get("SLURMD_NODENAME")
    batch_index = hosts.index(batch) if batch in hosts else -1
    announced = _gpu_ids(environ)
    if announced is not None and batch_index >= 0:
        expected = per_node_gpus[batch_index]
        if len(announced[1]) != expected:
            _LOGGER.warning(
                "Slurm visible GPU ids contradict the reported node allocation; using aggregate counts only"
            )
            return aggregate
    nodes = [Node(host, procs[index], mem[index], per_node_gpus[index]) for index, host in enumerate(hosts)]
    if batch_index >= 0:
        batch_node = replace(nodes[batch_index], local=True)
        if announced is not None:
            batch_node = replace(batch_node, gpu_ids=announced[1], gpu_variable=announced[0])
        if cpu_slots:
            batch_node = replace(batch_node, cpu_slots=local_cpu_slots(batch_node.procs))
        nodes[batch_index] = batch_node
    allocation = Allocation("slurm", end_time, tuple(nodes), {}, cpus_per_proc)
    if allocation.capacity() != counts:
        _LOGGER.warning("the Slurm nodes do not add up to their aggregate counts; using aggregate counts only")
        return aggregate
    return allocation


class SlurmScheduler:
    """Implement the private scheduler hooks for Slurm."""

    name = "slurm"

    def detects(self, environ: Mapping[str, str]) -> bool:
        """Return whether Slurm marks an active job in *environ*."""

        return "SLURM_JOB_ID" in environ

    def counts(self, environ: Mapping[str, str]) -> dict[str, int]:
        """Return Slurm's aggregate resource counts."""

        return slurm_counts(environ)

    def end_time(self, environ: Mapping[str, str]) -> float | None:
        """Return Slurm's job end time, when exported."""

        return slurm_end_time(environ)

    def probe_allocation(self, environ: Mapping[str, str], *, run: Run, cpu_slots: bool = False) -> Allocation | None:
        """Probe Slurm's node allocation and optional local metadata."""

        return slurm_allocation(environ, run=run, cpu_slots=cpu_slots)

    def step_argv(
        self,
        placement: "Placement",
        *,
        nodefile: str,
        gpus_present: bool = False,
        cpus_per_proc: int = 1,
        mem: int | None = None,
    ) -> list[str] | None:
        """Render the default Slurm step from placement metadata only.

        :param placement: The assigned node shares.
        :param nodefile: The scoped nodefile path exported to ``srun``.
        :param gpus_present: Whether the allocation has GPUs despite this share.
        :param cpus_per_proc: Normalized CPUs assigned to each process.
        :param mem: Aggregate memory override in MB, when supplied.
        :return: The step argv, or ``None`` when no process slot can launch.
        :raises ValueError: If a GPU share has no process slot.
        """

        tasks = sum(share.procs for share in placement.nodes)
        if tasks <= 0:
            return None
        if any(share.procs <= 0 and share.gpus > 0 for share in placement.nodes):
            raise ValueError("cannot launch a GPU share without processor slots")
        shares = tuple(share for share in placement.nodes if share.procs > 0)
        gpus = sum(share.gpus for share in shares)
        # The per-task SLURM_HOSTFILE alone names the hosts and the layout: srun rejects --nodes with the
        # arbitrary distribution, and a --nodelist would replace the hostfile with one entry per node.
        argv = [
            "env",
            f"SLURM_HOSTFILE={nodefile}",
            "srun",
            f"--ntasks={tasks}",
            "--distribution=arbitrary",
            "--exact",
            f"--cpus-per-task={cpus_per_proc}",
        ]
        mems = [share.mem for share in shares]
        total = mem if None in mems else sum(share or 0 for share in mems)
        if total and len(shares) == 1:
            argv.append(f"--mem={total}M")
        elif total:
            argv.append(f"--mem-per-cpu={-(-total // (tasks * cpus_per_proc))}M")
        if gpus:
            argv.append(f"--gpus={gpus}")
        elif gpus_present:
            argv.append("--gres=none")
        return argv


SLURM = SlurmScheduler()
