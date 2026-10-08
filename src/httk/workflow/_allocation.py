"""What a manager learns about the batch allocation it runs inside.

An allocation probe reports the nodes of an allocation (host, processor slots,
memory, GPUs and, when known, their device ids and CPU lists), when it ends and
its scheduler identity. ``--allocation SPEC`` selects the probe: ``auto``,
``none``, ``slurm``, ``host`` or ``exec:PATH``, the last running a site
executable that prints one ``httk-workflow-allocation`` JSON envelope.

A launch record names the allocation its ranks ran in
(:class:`RecordedAllocation`), so that another manager can later ask whether
that allocation has ended (:func:`allocation_ended`): through the maintained
scheduler, or by running the site probe as ``PATH ended``.
"""

import json
import logging
import math
import os
import re
import socket
import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Self

from .errors import FormatError
from .models import validate_capacity, validate_label

_LOGGER = logging.getLogger(__name__)

ALLOCATION_FORMAT = "httk-workflow-allocation"
#: The query an ``exec:PATH`` probe reads on stdin when it runs as ``PATH ended``.
ALLOCATION_QUERY_FORMAT = "httk-workflow-allocation-query"
#: The answer an ``exec:PATH`` probe prints to that query.
ALLOCATION_STATUS_FORMAT = "httk-workflow-allocation-status"
#: The most entries an allocation identity has.
MAXIMUM_IDENTITY_ENTRIES = 16
#: The longest allocation identity value, in UTF-8 bytes.
MAXIMUM_IDENTITY_VALUE_BYTES = 256
#: The longest recorded ``--allocation`` specification, in UTF-8 bytes.
MAXIMUM_PROBE_BYTES = 4096
#: The capacity labels an allocation derives from its nodes.
NODE_LABELS = frozenset({"procs", "mem", "gpus", "nodes"})
#: The variables a GPU allocation is announced in, in the order they are read.
GPU_VARIABLES = ("CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES", "ZE_AFFINITY_MASK")
#: The GPU variables whose empty value hides every device; an empty
#: ``ZE_AFFINITY_MASK`` does not hide Level Zero devices.
GPU_HIDING_VARIABLES = frozenset({"CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES"})
_CPULIST = re.compile(r"[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*")
_VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TRUE = frozenset({"true", "1", "yes"})

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
    :param local: Whether the probe knows this node is the manager's own host.
    """

    host: str
    procs: int
    mem: int | None = None
    gpus: int = 0
    cpu_slots: tuple[str, ...] | None = None
    gpu_ids: tuple[str, ...] | None = None
    gpu_variable: str | None = None
    local: bool = False


@dataclass(frozen=True)
class Allocation:
    """The nodes, extra resources and end of one manager's allocation.

    :param kind: The probe that produced it: ``slurm``, ``host``, or an envelope's kind.
    :param end_time: The epoch second the allocation ends, or ``None`` when unknown.
    :param nodes: The allocation's nodes, empty when only aggregate counts are known.
    :param resources: Aggregate counts when *nodes* is empty; otherwise only
        extras such as licenses, never ``procs``, ``mem``, ``gpus`` or ``nodes``.
    :param cpus_per_proc: CPUs assigned to each scheduler process.
    :param identity: The scheduler's opaque identity of the allocation, such as
        ``{"job_id": "123"}`` for Slurm, or ``None`` when it has none.
    :param probe: The ``--allocation`` specification that probed it, such as
        ``slurm`` or ``exec:PATH``, or ``None`` when it was not probed.
    """

    kind: str
    end_time: float | None
    nodes: tuple[Node, ...] = ()
    resources: Mapping[str, int] = field(default_factory=dict)
    cpus_per_proc: int = 1
    identity: Mapping[str, str] | None = None
    probe: str | None = None

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


def bind_cpus_setting(settings: Mapping[str, object]) -> bool:
    """Return whether the workspace setting ``manager.bind_cpus`` asks for CPU pinning.

    :param settings: The workspace settings.
    :return: ``True`` for ``True``, the integer ``1``, or ``true``, ``1`` or ``yes``
        in any case; otherwise ``False``.
    """

    value = settings.get("manager.bind_cpus")
    # ``True == 1``, so the integer test covers both.
    return (isinstance(value, int) and value == 1) or (isinstance(value, str) and value.lower() in _TRUE)


def format_cpulist(cpus: Iterable[int]) -> str:
    """Render CPU numbers as a compact Linux cpulist such as ``0-3,8``.

    :param cpus: The CPU numbers.
    :return: The cpulist.
    """

    ranges: list[list[int]] = []
    for cpu in sorted(set(cpus)):
        if ranges and ranges[-1][1] == cpu - 1:
            ranges[-1][1] = cpu
        else:
            ranges.append([cpu, cpu])
    return ",".join(str(first) if first == last else f"{first}-{last}" for first, last in ranges)


def parse_cpulist(cpulist: str) -> set[int]:
    """Return the CPU numbers of a Linux cpulist such as ``0-3,8``.

    :param cpulist: The cpulist.
    :return: The CPU numbers.
    :raises ValueError: If it is not a cpulist or has a descending range.
    """

    if not _CPULIST.fullmatch(cpulist):
        raise ValueError(f"{cpulist!r} is not a Linux cpulist such as 0-7,16")
    cpus: set[int] = set()
    for item in cpulist.split(","):
        first, _, last = item.partition("-")
        if int(last or first) < int(first):
            raise ValueError(f"{cpulist!r} has the descending range {item}")
        cpus.update(range(int(first), int(last or first) + 1))
    return cpus


def local_cpu_slots(procs: int) -> tuple[str, ...] | None:
    """Split this process's CPU affinity into *procs* equal slots.

    :param procs: The processor slots.
    :return: One cpulist per slot (leftover CPUs stay unused), or ``None`` when
        the affinity is unknown or has fewer CPUs than slots.
    """

    try:
        cpus = sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return None
    chunk = len(cpus) // procs if procs > 0 else 0
    if chunk == 0:
        return None
    return tuple(format_cpulist(cpus[index * chunk : (index + 1) * chunk]) for index in range(procs))


def _gpu_ids(environ: Mapping[str, str]) -> tuple[str, tuple[str, ...]] | None:
    """Return the first announced GPU variable and its ids, or ``None``."""

    for variable in GPU_VARIABLES:
        if variable in environ:
            return variable, tuple(item.strip() for item in environ[variable].split(",") if item.strip())
    return None


def half_physical_memory_mb() -> int | None:
    """Return half of this host's physical memory in MB, or ``None`` when unknown."""

    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // 2**20 // 2
    except (ValueError, OSError, AttributeError):
        return None


def host_allocation(environ: Mapping[str, str], *, cpu_slots: bool = False) -> Allocation:
    """Describe this host as a one-node allocation.

    :param environ: The environment to read announced GPU ids from.
    :param cpu_slots: Whether to split this process's CPU affinity into the
        node's slots (see :func:`local_cpu_slots`).
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
        Node(host, procs, memory, len(announced[1]), gpu_ids=announced[1], gpu_variable=announced[0], local=True)
        if announced is not None and announced[1]
        else Node(host, procs, memory, local=True)
    )
    if cpu_slots:
        node = replace(node, cpu_slots=local_cpu_slots(procs))
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


def validate_identity(value: object, name: str) -> dict[str, str]:
    """Validate an allocation identity: an object of label keys and short string values.

    :param value: The decoded identity.
    :param name: The member name used in errors.
    :return: The identity.
    :raises httk.workflow.errors.FormatError: If it is not an object, has more than
        :data:`MAXIMUM_IDENTITY_ENTRIES` entries, a key that is not a label, or a value
        that is not a string of at most :data:`MAXIMUM_IDENTITY_VALUE_BYTES` UTF-8 bytes.
    """

    if not isinstance(value, dict):
        raise FormatError(f"{name} must be an object")
    if len(value) > MAXIMUM_IDENTITY_ENTRIES:
        raise FormatError(f"{name} has more than {MAXIMUM_IDENTITY_ENTRIES} entries")
    identity: dict[str, str] = {}
    for key, item in value.items():
        validate_label(key, f"{name} key")
        if not isinstance(item, str) or len(item.encode("utf-8", "surrogatepass")) > MAXIMUM_IDENTITY_VALUE_BYTES:
            raise FormatError(f"{name}.{key} must be a string of at most {MAXIMUM_IDENTITY_VALUE_BYTES} bytes")
        identity[key] = item
    return identity


def _end_time(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < math.inf:
        raise FormatError(f"{name} must be a positive epoch second or null")
    return float(value)


_ENVELOPE_KEYS = frozenset(
    {"format", "format_version", "kind", "end_time", "nodes", "resources", "cpus_per_proc", "identity"}
)
_NODE_KEYS = frozenset({"host", "procs", "mem", "gpus", "cpus", "gpu_ids", "gpu_variable", "local"})


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
        seen_cpus: set[int] = set()
        for slot in cpus:
            try:
                parsed = parse_cpulist(slot)
            except ValueError as exc:
                raise FormatError(f"{name}.cpus entries must be ascending Linux cpulists such as 0-7,16") from exc
            if seen_cpus & parsed:
                raise FormatError(f"{name}.cpus slots overlap")
            seen_cpus.update(parsed)
    gpu_ids = None if "gpu_ids" not in item else _strings(item["gpu_ids"], f"{name}.gpu_ids", gpus)
    if gpu_ids is not None and len(set(gpu_ids)) != len(gpu_ids):
        raise FormatError(f"{name}.gpu_ids must not contain duplicate device ids")
    variable = item.get("gpu_variable")
    if variable is not None and (not isinstance(variable, str) or not _VARIABLE.fullmatch(variable)):
        raise FormatError(f"{name}.gpu_variable must be an environment variable name")
    if gpu_ids is not None and variable is None:
        raise FormatError(f"{name}.gpu_variable is required with gpu_ids")
    local = item.get("local", False)
    if not isinstance(local, bool):
        raise FormatError(f"{name}.local must be a boolean")
    return Node(host, procs, mem, gpus, cpus, gpu_ids, variable, local)


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
    end_time = _end_time(envelope.get("end_time"), "allocation.end_time")
    identity = (
        None if envelope.get("identity") is None else validate_identity(envelope["identity"], "allocation.identity")
    )
    cpus_per_proc = envelope.get("cpus_per_proc", 1)
    if isinstance(cpus_per_proc, bool) or not isinstance(cpus_per_proc, int) or cpus_per_proc < 1:
        raise FormatError("allocation.cpus_per_proc must be a positive integer")
    nodes = envelope.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise FormatError("allocation.nodes must be a non-empty list")
    parsed = tuple(_envelope_node(node, f"allocation.nodes[{index}]") for index, node in enumerate(nodes))
    if len({node.host for node in parsed}) != len(parsed):
        raise FormatError("allocation.nodes hosts must be unique")
    if sum(node.local for node in parsed) > 1:
        raise FormatError("allocation.nodes may mark at most one node local")
    resources = validate_capacity(envelope.get("resources", {}), "allocation.resources")
    clash = sorted(NODE_LABELS & resources.keys())
    if clash:
        raise FormatError(f"allocation.resources.{clash[0]} is derived from the allocation's nodes")
    return Allocation(kind, end_time, parsed, resources, cpus_per_proc, identity)


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


@dataclass(frozen=True)
class RecordedAllocation:
    """The allocation a launch record names: what another manager may ask about later.

    :param probe: The recording manager's ``--allocation`` specification, such as ``slurm`` or ``exec:PATH``.
    :param kind: The allocation kind.
    :param identity: The scheduler's identity of the allocation, or ``None``.
    :param end_time: The epoch second the allocation ends, or ``None`` when unknown.
    """

    probe: str
    kind: str
    identity: Mapping[str, str] | None
    end_time: float | None

    @classmethod
    def from_allocation(cls, allocation: Allocation) -> Self:
        """Describe a manager's allocation for its launch records.

        :param allocation: The manager's allocation.
        :return: The record, naming the allocation's kind as its probe when it was not probed.
        """

        identity = None if allocation.identity is None else dict(allocation.identity)
        return cls(allocation.probe or allocation.kind, allocation.kind, identity, allocation.end_time)

    @classmethod
    def from_record(cls, value: object) -> Self | None:
        """Read the ``allocation`` member of a launch record leniently.

        Unknown members are ignored, and an ``identity`` or ``end_time`` that is
        not valid is dropped, which only removes evidence; a member without a
        usable ``probe`` and ``kind`` names no allocation.

        :param value: The decoded member.
        :return: The recorded allocation, or ``None`` when it is ``null`` or unusable.
        """

        if not isinstance(value, dict):
            return None
        probe, kind = value.get("probe"), value.get("kind")
        if not isinstance(probe, str) or not probe or len(probe.encode("utf-8", "surrogatepass")) > MAXIMUM_PROBE_BYTES:
            return None
        try:
            kind = validate_label(kind, "allocation.kind")
        except FormatError:
            return None
        try:
            identity = None if value.get("identity") is None else validate_identity(value["identity"], "identity")
        except FormatError:
            identity = None
        try:
            end_time = _end_time(value.get("end_time"), "allocation.end_time")
        except FormatError:
            end_time = None
        return cls(probe, kind, identity, end_time)

    @property
    def queryable(self) -> bool:
        """Return whether :func:`allocation_ended` can ask anyone about this allocation.

        :return: Whether it has an identity and names an ``exec:PATH`` probe or a maintained scheduler.
        """

        if self.identity is None:
            return False
        if self.probe.startswith("exec:") and self.probe[5:]:
            return True
        from ._scheduler import scheduler_for

        return scheduler_for(self.kind if self.probe == "auto" else self.probe) is not None

    def as_json(self) -> dict[str, object]:
        """Return the record's ``allocation`` member.

        :return: ``probe``, ``kind``, ``identity`` (an object or ``None``) and ``end_time``.
        """

        identity = None if self.identity is None else dict(self.identity)
        return {"probe": self.probe, "kind": self.kind, "identity": identity, "end_time": self.end_time}


def exec_allocation_ended(path: str, recorded: RecordedAllocation, *, run: Run, timeout: float) -> bool | None:
    """Ask a site allocation probe whether a recorded allocation has ended.

    The probe runs as ``PATH ended``, without a shell, with one
    ``httk-workflow-allocation-query`` document on stdin, and answers with one
    ``httk-workflow-allocation-status`` document on stdout.

    :param path: The probe executable.
    :param recorded: The allocation a launch record names.
    :param run: The command runner.
    :param timeout: Seconds the probe may take.
    :return: The probe's ``ended`` answer, or ``None`` when it fails, times out or answers
        anything else, such as an older probe printing its allocation envelope.
    """

    query = {
        "format": ALLOCATION_QUERY_FORMAT,
        "format_version": 1,
        "kind": recorded.kind,
        "identity": None if recorded.identity is None else dict(recorded.identity),
        "end_time": recorded.end_time,
    }
    try:
        completed = run(
            [path, "ended"], input=json.dumps(query), capture_output=True, text=True, timeout=timeout, check=False
        )
        answer = json.loads(completed.stdout) if completed.returncode == 0 else None
    except (OSError, ValueError, RecursionError, subprocess.SubprocessError) as exc:
        _LOGGER.debug("allocation probe %s cannot answer whether its allocation ended: %s", path, exc)
        return None
    if (
        not isinstance(answer, dict)
        or answer.keys() != {"format", "format_version", "ended"}
        or answer["format"] != ALLOCATION_STATUS_FORMAT
        or type(answer["format_version"]) is not int
        or answer["format_version"] != 1
        or type(answer["ended"]) is not bool
    ):
        return None
    return answer["ended"]


def can_ask(recorded: RecordedAllocation) -> bool:
    """Return whether the client that answers about a queryable allocation is installed on this host.

    :param recorded: The allocation a launch record names.
    :return: For an ``exec:PATH`` probe, whether PATH exists here; for a maintained scheduler, whether its
        query client is installed; ``False`` for an allocation that is not queryable.
    """

    if not recorded.queryable:
        return False
    if recorded.probe.startswith("exec:"):
        return os.path.isfile(recorded.probe[5:])
    from ._scheduler import scheduler_for

    scheduler = scheduler_for(recorded.kind if recorded.probe == "auto" else recorded.probe)
    return scheduler is not None and scheduler.can_query()


def allocation_ended(recorded: RecordedAllocation, *, run: Run = subprocess.run, timeout: float) -> bool | None:
    """Ask whoever can tell whether a recorded allocation has ended.

    Only an allocation with an identity is asked: an ``exec:PATH`` probe with
    :func:`exec_allocation_ended`, a maintained scheduler named by the probe (or,
    for ``auto``, by the kind) with its
    :meth:`~httk.workflow._scheduler.Scheduler.allocation_ended`; ``host``,
    ``none`` and anything else cannot tell.

    :param recorded: The allocation a launch record names.
    :param run: The command runner.
    :param timeout: Seconds the query may take.
    :return: ``True`` when the end is confirmed, ``False`` while the allocation is active, ``None`` when unknown.
    """

    if not recorded.queryable or recorded.identity is None:
        return None
    if recorded.probe.startswith("exec:"):
        return exec_allocation_ended(recorded.probe[5:], recorded, run=run, timeout=timeout)
    from ._scheduler import scheduler_for

    scheduler = scheduler_for(recorded.kind if recorded.probe == "auto" else recorded.probe)
    return None if scheduler is None else scheduler.allocation_ended(recorded.identity, run=run, timeout=timeout)


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
    """Validate an allocation specification without probing it.

    :param spec: ``auto``, ``none``, ``host``, a maintained scheduler, or ``exec:PATH``.
    :return: The unchanged specification.
    :raises ValueError: If the specification is not supported.
    """

    from ._scheduler import scheduler_for

    if spec in {"auto", "none", "host"} or scheduler_for(spec) is not None or (spec.startswith("exec:") and spec[5:]):
        return spec
    raise ValueError(f"--allocation {spec!r}: expected auto, none, host, a maintained scheduler, or exec:PATH")


def probe_allocation(
    spec: str, environ: Mapping[str, str], *, run: Run = subprocess.run, cpu_slots: bool = False
) -> Allocation | None:
    """Probe the allocation selected by *spec* through the scheduler lookup.

    :param spec: The validated allocation specification.
    :param environ: Environment used for scheduler detection and probing.
    :param run: Command runner used for scheduler helper commands.
    :param cpu_slots: Whether to include local CPU affinity slots.
    :return: The allocation, or ``None`` when allocation is disabled/unavailable.
    :raises ValueError: If the specification is invalid or a site probe fails.
    """

    spec = parse_allocation_spec(spec)
    if spec == "none":
        return None
    if spec == "host":
        return replace(host_allocation(environ, cpu_slots=cpu_slots), probe=spec)
    if spec.startswith("exec:"):
        # Recorded absolute, so another manager with another working directory runs the same probe.
        return replace(exec_allocation(spec[5:], environ), probe=f"exec:{os.path.realpath(spec[5:])}")
    from ._scheduler import detect_scheduler, scheduler_for

    scheduler = detect_scheduler(environ) if spec == "auto" else scheduler_for(spec)
    allocation = None if scheduler is None else scheduler.probe_allocation(environ, run=run, cpu_slots=cpu_slots)
    return None if allocation is None or scheduler is None else replace(allocation, probe=scheduler.name)
