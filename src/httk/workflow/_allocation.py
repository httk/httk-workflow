"""What a manager learns about the batch allocation it runs inside.

An allocation probe reports the nodes of an allocation (host, processor slots,
memory, GPUs and, when known, their device ids and CPU lists) and when it ends.
``--allocation SPEC`` selects the probe: ``auto``, ``none``, ``slurm``, ``host``
or ``exec:PATH``, the last running a site executable that prints one
``httk-workflow-allocation`` JSON envelope.
"""

import json
import logging
import os
import re
import socket
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import FormatError
from .models import validate_capacity, validate_label

_LOGGER = logging.getLogger(__name__)

ALLOCATION_FORMAT = "httk-workflow-allocation"
ALLOCATION_SPECS = ("auto", "none", "slurm", "host")
#: The capacity labels an allocation derives from its nodes.
NODE_LABELS = frozenset({"procs", "mem", "gpus", "nodes"})
#: The variables a GPU allocation is announced in, in the order they are read.
GPU_VARIABLES = ("CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES", "ZE_AFFINITY_MASK")
_CPULIST = re.compile(r"[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*")
_VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_REPEATED = re.compile(r"([0-9]+)(?:\(x([0-9]+)\))?")

type Run = Callable[..., subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class Node:
    """One node of an allocation.

    :param host: The node's host name.
    :param procs: The processor slots this manager may use on the node.
    :param mem: The node's memory in MB, or ``None`` when unknown.
    :param gpus: The node's GPUs.
    :param cpu_slots: One Linux cpulist per processor slot, when known.
    :param gpu_ids: One opaque device id per GPU, when known.
    :param gpu_variable: The environment variable the GPU ids belong in, such as
        ``CUDA_VISIBLE_DEVICES``.
    """

    host: str
    procs: int
    mem: int | None = None
    gpus: int = 0
    cpu_slots: tuple[str, ...] | None = None
    gpu_ids: tuple[str, ...] | None = None
    gpu_variable: str | None = None


@dataclass(frozen=True)
class Allocation:
    """The nodes, extra resources and end of one manager's allocation.

    :param kind: The probe that produced it: ``slurm``, ``host``, or an envelope's kind.
    :param end_time: The epoch second the allocation ends, or ``None`` when unknown.
    :param nodes: The allocation's nodes, empty when only aggregate counts are known.
    :param resources: Aggregate counts when *nodes* is empty; otherwise only
        extras such as licenses, never ``procs``, ``mem``, ``gpus`` or ``nodes``.
    """

    kind: str
    end_time: float | None
    nodes: tuple[Node, ...] = ()
    resources: Mapping[str, int] = field(default_factory=dict)

    def capacity(self) -> dict[str, int]:
        """Return the manager capacity this allocation advertises.

        With nodes, ``procs`` (when any), ``gpus`` (when any), ``mem`` (when
        every node knows it) and ``nodes`` are node sums and the extras are
        merged in; without nodes it is *resources* as given.

        :return: The validated capacity.
        :raises ValueError: If a count is invalid or an extra redefines a node label.
        """

        if not self.nodes:
            return validate_capacity(dict(self.resources), "allocation.resources")
        clash = sorted(NODE_LABELS & self.resources.keys())
        if clash:
            raise ValueError(f"allocation.resources.{clash[0]} is derived from the allocation's nodes")
        capacity: dict[str, int] = {}
        procs = sum(node.procs for node in self.nodes)
        if procs:
            capacity["procs"] = procs
        gpus = sum(node.gpus for node in self.nodes)
        if gpus:
            capacity["gpus"] = gpus
        mems = [node.mem for node in self.nodes]
        if all(mem is not None for mem in mems):
            capacity["mem"] = sum(mem for mem in mems if mem is not None)
        capacity["nodes"] = len(self.nodes)
        return validate_capacity({**capacity, **self.resources}, "allocation")


def slurm_end_time(environ: Mapping[str, str]) -> float | None:
    """Return the epoch second the enclosing Slurm job ends, if it is known.

    :param environ: The environment to read ``SLURM_JOB_ID`` and ``SLURM_JOB_END_TIME`` from.
    :return: The end time, or ``None`` outside a Slurm job or when it is not a positive integer.
    """

    raw = environ.get("SLURM_JOB_END_TIME")
    if "SLURM_JOB_ID" not in environ or raw is None:
        return None
    text = raw.strip()
    if not text.isdigit() or int(text) <= 0:
        _LOGGER.warning("ignoring SLURM_JOB_END_TIME=%r: not a positive epoch second", raw)
        return None
    return float(text)


def _slurm_gpus(raw: str) -> int:
    """Parse ``SLURM_GPUS``: ``4``, ``a100:4``, or ``a100:2,v100:2``."""

    total = 0
    for item in raw.split(","):
        count = item.rsplit(":", 1)[-1].strip()
        if not count.isdigit():
            raise ValueError(raw)
        total += int(count)
    return total


def slurm_counts(environ: Mapping[str, str]) -> dict[str, int]:
    """Read manager resource capacities from an active Slurm allocation.

    :param environ: Environment mapping to inspect.
    :return: The aggregate capacities Slurm advertises, or an empty mapping outside a job.
    """

    if "SLURM_JOB_ID" not in environ:
        return {}

    def integer(name: str, *, memory: bool = False) -> int | None:
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
            _LOGGER.warning("ignoring unparsable SLURM resource variable %s=%r", name, raw)
            return None
        if parsed < 0:
            _LOGGER.warning("ignoring negative SLURM resource variable %s=%r", name, raw)
            return None
        if divide_kibibytes:
            return parsed // 1024
        return parsed * multiplier

    resources: dict[str, int] = {}
    ntasks = integer("SLURM_NTASKS")
    if ntasks is not None:
        resources["procs"] = ntasks
    gpus = integer("SLURM_GPUS")
    if gpus is not None:
        resources["gpus"] = gpus
    nodes = integer("SLURM_JOB_NUM_NODES")
    if nodes is not None:
        resources["nodes"] = nodes

    if "SLURM_MEM_PER_CPU" in environ:
        mem_per_cpu = integer("SLURM_MEM_PER_CPU", memory=True)
        cpus = integer("SLURM_CPUS_PER_TASK") if "SLURM_CPUS_PER_TASK" in environ else 1
        if mem_per_cpu is not None and cpus is not None and ntasks is not None:
            resources["mem"] = mem_per_cpu * cpus * ntasks
    elif "SLURM_MEM_PER_NODE" in environ:
        mem_per_node = integer("SLURM_MEM_PER_NODE", memory=True)
        if mem_per_node is not None and nodes is not None:
            resources["mem"] = mem_per_node * nodes
    return resources


def _expand_per_node(text: str) -> list[int] | None:
    """Expand Slurm's per-node count lists such as ``16(x2),8``, or ``None`` when malformed."""

    counts: list[int] = []
    for item in text.split(","):
        match = _REPEATED.fullmatch(item.strip())
        if match is None:
            return None
        counts += [int(match.group(1))] * int(match.group(2) or 1)
    return counts


def _gpu_ids(environ: Mapping[str, str]) -> tuple[str, tuple[str, ...]] | None:
    """Return the first announced GPU variable and its ids, or ``None``."""

    for variable in GPU_VARIABLES:
        if variable in environ:
            return variable, tuple(item.strip() for item in environ[variable].split(",") if item.strip())
    return None


def _slurm_hosts(environ: Mapping[str, str], run: Run) -> list[str] | None:
    """List the job's hosts with ``scontrol``, or the batch host of a one-node job."""

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


def slurm_allocation(environ: Mapping[str, str], *, run: Run = subprocess.run) -> Allocation | None:
    """Probe the enclosing Slurm job's nodes.

    The nodes come from ``scontrol show hostnames``, tasks per node from
    ``SLURM_TASKS_PER_NODE``, memory from the job's memory request, and GPU ids
    of the batch host from its ``CUDA_VISIBLE_DEVICES`` (or ROCm/oneAPI
    equivalent). Whenever the nodes cannot be described consistently with the
    aggregate counts of :func:`slurm_counts`, the allocation carries those
    counts only, so its capacity is always exactly theirs.

    :param environ: The job's environment.
    :param run: The :func:`subprocess.run` used to call ``scontrol``.
    :return: The allocation, or ``None`` outside a Slurm job.
    """

    if "SLURM_JOB_ID" not in environ:
        return None
    end_time = slurm_end_time(environ)
    counts = slurm_counts(environ)
    aggregate = Allocation("slurm", end_time, (), counts)
    hosts = _slurm_hosts(environ, run)
    if hosts is None:
        _LOGGER.warning("cannot list the Slurm job's nodes; using its aggregate counts only")
        return aggregate
    procs = [0] * len(hosts)
    if "procs" in counts:
        per_node = _expand_per_node(environ.get("SLURM_TASKS_PER_NODE", "")) if len(hosts) > 1 else [counts["procs"]]
        if per_node is None or len(per_node) != len(hosts) or sum(per_node) != counts["procs"]:
            _LOGGER.warning(
                "SLURM_TASKS_PER_NODE=%r does not match the job's tasks and nodes; using its aggregate counts only",
                environ.get("SLURM_TASKS_PER_NODE"),
            )
            return aggregate
        procs = per_node
    gpus = counts.get("gpus", 0)
    if gpus % len(hosts):
        _LOGGER.warning("%d GPUs do not split evenly over %d nodes; using aggregate counts only", gpus, len(hosts))
        return aggregate
    # Memory follows the tasks, exactly as slurm_counts sums it.
    mem: list[int | None] = [None] * len(hosts)
    if "mem" in counts:
        if "SLURM_MEM_PER_CPU" in environ:
            mem = [counts["mem"] * count // counts["procs"] if counts["procs"] else 0 for count in procs]
        else:
            mem = [counts["mem"] // len(hosts)] * len(hosts)
    per_node_gpus = gpus // len(hosts)
    batch = environ.get("SLURMD_NODENAME")
    # The ids are the batch host's; an unknown batch host, or one outside the
    # list (an salloc shell on a login node), owns none of them.
    batch_index = hosts.index(batch) if batch in hosts else -1
    announced = _gpu_ids(environ)
    if announced is not None and (not announced[1] or len(announced[1]) != per_node_gpus):
        announced = None
    nodes = [
        Node(host, procs[index], mem[index], per_node_gpus)
        if index != batch_index or announced is None
        else Node(host, procs[index], mem[index], per_node_gpus, gpu_ids=announced[1], gpu_variable=announced[0])
        for index, host in enumerate(hosts)
    ]
    allocation = Allocation("slurm", end_time, tuple(nodes), {})
    if allocation.capacity() != counts:
        _LOGGER.warning("the Slurm job's nodes do not add up to its counts %s; using the counts only", counts)
        return aggregate
    return allocation


def half_physical_memory_mb() -> int | None:
    """Return half of this host's physical memory in MB, or ``None`` when unknown."""

    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // 2**20 // 2
    except (ValueError, OSError, AttributeError):
        return None


def host_allocation(environ: Mapping[str, str]) -> Allocation:
    """Describe this host as a one-node allocation.

    :param environ: The environment to read announced GPU ids from.
    :return: This host with its usable processors, half its memory and its announced GPUs.
    """

    try:
        procs = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        procs = os.cpu_count() or 1
    announced = _gpu_ids(environ)
    host = socket.gethostname()
    memory = half_physical_memory_mb()
    node = (
        Node(host, procs, memory, len(announced[1]), gpu_ids=announced[1], gpu_variable=announced[0])
        if announced is not None and announced[1]
        else Node(host, procs, memory)
    )
    return Allocation("host", None, (node,), {})


def _count(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FormatError(f"{name} must be a non-negative integer")
    return value


def _strings(value: object, name: str, length: int) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise FormatError(f"{name} must be a list of non-empty strings")
    if len(value) != length:
        raise FormatError(f"{name} must have {length} entries")
    return tuple(value)


_ENVELOPE_KEYS = frozenset({"format", "format_version", "kind", "end_time", "nodes", "resources"})
_NODE_KEYS = frozenset({"host", "procs", "mem", "gpus", "cpus", "gpu_ids", "gpu_variable"})


def _envelope_node(value: object, name: str) -> Node:
    if not isinstance(value, dict):
        raise FormatError(f"{name} must be an object")
    item: dict[str, Any] = value
    unknown = sorted(item.keys() - _NODE_KEYS)
    if unknown:
        raise FormatError(f"{name}.{unknown[0]} is not an allocation node member")
    host = item.get("host")
    if not isinstance(host, str) or not host:
        raise FormatError(f"{name}.host must be a non-empty string")
    procs = _count(item.get("procs"), f"{name}.procs")
    mem = None if item.get("mem") is None else _count(item["mem"], f"{name}.mem")
    gpus = _count(item.get("gpus", 0), f"{name}.gpus")
    cpus = None
    if "cpus" in item:
        cpus = _strings(item["cpus"], f"{name}.cpus", procs)
        if not all(_CPULIST.fullmatch(text) for text in cpus):
            raise FormatError(f"{name}.cpus entries must be Linux cpulists such as 0-7,16")
    gpu_ids = None if "gpu_ids" not in item else _strings(item["gpu_ids"], f"{name}.gpu_ids", gpus)
    variable = item.get("gpu_variable")
    if variable is not None and (not isinstance(variable, str) or not _VARIABLE.fullmatch(variable)):
        raise FormatError(f"{name}.gpu_variable must be an environment variable name")
    if gpu_ids is not None and variable is None:
        raise FormatError(f"{name}.gpu_variable is required with gpu_ids")
    return Node(host, procs, mem, gpus, cpu_slots=cpus, gpu_ids=gpu_ids, gpu_variable=variable)


def allocation_from_envelope(value: object) -> Allocation:
    """Validate one ``httk-workflow-allocation`` envelope strictly.

    :param value: The decoded JSON envelope.
    :return: The allocation it describes.
    :raises httk.workflow.errors.FormatError: If the envelope is malformed or has unknown members.
    """

    if not isinstance(value, dict):
        raise FormatError("allocation envelope must be an object")
    envelope: dict[str, Any] = value
    unknown = sorted(envelope.keys() - _ENVELOPE_KEYS)
    if unknown:
        raise FormatError(f"allocation.{unknown[0]} is not an allocation envelope member")
    if envelope.get("format") != ALLOCATION_FORMAT or envelope.get("format_version") != 1:
        raise FormatError(f"allocation envelope must be {ALLOCATION_FORMAT} format_version 1")
    kind = validate_label(envelope.get("kind"), "allocation.kind")
    end_time = envelope.get("end_time")
    if end_time is not None and (
        isinstance(end_time, bool) or not isinstance(end_time, (int, float)) or not 0 < end_time < float("inf")
    ):
        raise FormatError("allocation.end_time must be a positive epoch second or null")
    nodes = envelope.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise FormatError("allocation.nodes must be a non-empty list")
    parsed = tuple(_envelope_node(node, f"allocation.nodes[{index}]") for index, node in enumerate(nodes))
    if len({node.host for node in parsed}) != len(parsed):
        raise FormatError("allocation.nodes hosts must be unique")
    resources = validate_capacity(envelope.get("resources", {}), "allocation.resources")
    clash = sorted(NODE_LABELS & resources.keys())
    if clash:
        raise FormatError(f"allocation.resources.{clash[0]} is derived from the allocation's nodes")
    return Allocation(kind, None if end_time is None else float(end_time), parsed, resources)


def exec_allocation(path: str, environ: Mapping[str, str], *, timeout: float = 60.0) -> Allocation:
    """Run a site allocation probe and parse the envelope it prints.

    :param path: The probe executable, run without a shell and without arguments.
    :param environ: The probe's environment.
    :param timeout: Seconds the probe may take.
    :return: The allocation the probe reports.
    :raises ValueError: If the probe cannot run, fails, times out or prints an invalid envelope.
    """

    try:
        completed = subprocess.run(
            [path], env=dict(environ), capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired as exc:
        raise ValueError(f"allocation probe {path} timed out after {timeout:g} s") from exc
    except OSError as exc:
        raise ValueError(f"allocation probe {path} cannot run: {exc}") from exc
    stderr = completed.stderr.strip()[-500:]
    if completed.returncode != 0:
        raise ValueError(f"allocation probe {path} exited with status {completed.returncode}: {stderr}")
    try:
        return allocation_from_envelope(json.loads(completed.stdout))
    except ValueError as exc:
        raise ValueError(
            f"allocation probe {path} printed an invalid envelope: {exc}; "
            f"stdout: {completed.stdout[:200]!r}; stderr: {stderr}"
        ) from exc


def argv_allocation(argv: Sequence[str]) -> str | None:
    """Return the ``--allocation`` spec a manager argv already carries.

    :param argv: The manager argument vector.
    :return: The last spec given as ``--allocation SPEC`` or ``--allocation=SPEC``, or ``None``.
    """

    spec = None
    for index, item in enumerate(argv):
        if item.startswith("--allocation="):
            spec = item.partition("=")[2]
        elif item == "--allocation":
            spec = argv[index + 1] if index + 1 < len(argv) else None
    return spec


def split_allocation(spec: str | None, count: int) -> list[str]:
    """Return the ``--allocation`` arguments for each of *count* split managers.

    Managers that split one allocation only count capacity, so they never probe:
    an explicit node-listing spec is overridden (argparse keeps the last) with a warning.

    :param spec: The spec the managers' argv already carries, or ``None``.
    :param count: The number of managers, more than one.
    :return: ``["--allocation", "none"]``.
    """

    if spec not in (None, "none", "auto"):
        _LOGGER.warning(
            "binding needs one manager per allocation; children of --count %d schedule by counts only", count
        )
    return ["--allocation", "none"]


def parse_allocation_spec(spec: str) -> str:
    """Validate an ``--allocation`` spec without probing.

    :param spec: ``auto``, ``none``, ``slurm``, ``host`` or ``exec:PATH``.
    :return: The spec.
    :raises ValueError: If the spec is unknown or ``exec:`` names no path.
    """

    if spec in ALLOCATION_SPECS or (spec.startswith("exec:") and spec[5:]):
        return spec
    raise ValueError(f"--allocation {spec!r}: expected auto, none, slurm, host, or exec:PATH")


def probe_allocation(spec: str, environ: Mapping[str, str], *, run: Run = subprocess.run) -> Allocation | None:
    """Probe the allocation an ``--allocation`` spec selects.

    :param spec: ``auto`` (``slurm`` inside a Slurm job, else ``none``),
        ``none``, ``slurm``, ``host`` or ``exec:PATH``.
    :param environ: The environment the probe reads.
    :param run: The :func:`subprocess.run` used to call ``scontrol``.
    :return: The allocation, or ``None`` when there is none.
    :raises ValueError: If the spec is invalid or a site probe fails.
    """

    spec = parse_allocation_spec(spec)
    if spec == "auto":
        spec = "slurm" if "SLURM_JOB_ID" in environ else "none"
    if spec == "none":
        return None
    if spec == "slurm":
        return slurm_allocation(environ, run=run)
    if spec == "host":
        return host_allocation(environ)
    return exec_allocation(spec[5:], environ)
